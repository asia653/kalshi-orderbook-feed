//! Tests for the connection side of [`Kalshi`]: message parsing, and the
//! whole loop driven by a mock Kalshi on localhost.

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use super::book::ONE;
use super::*;

fn text(json: &str) -> Message {
    Message::Text(json.into())
}

#[test]
fn parses_lifecycle_and_ignores_other_messages() {
    let frame = parse_message(text(
        r#"{"type":"market_lifecycle_v2","sid":13,"seq":3,
            "msg":{"market_ticker":"A","event_type":"created","open_ts":1,"close_ts":99}}"#,
    ))
    .unwrap()
    .unwrap();
    assert_eq!((frame.sid, frame.seq), (Some(13), Some(3)));
    let Body::Lifecycle(event) = frame.body else { panic!("expected lifecycle") };
    assert_eq!((event.market_ticker.as_str(), event.close_ts), ("A", Some(99)));

    let other = parse_message(text(r#"{"type":"ticker","msg":{}}"#)).unwrap().unwrap();
    assert_eq!(other.body.kind(), "ticker", "unknown types are passed on, to be logged once");
    assert!(parse_message(Message::Ping(Vec::new().into())).unwrap().is_none());
    // As Kalshi sent it: the reply to a command is numbered like any message.
    let ok = r#"{"type":"ok","id":3,"sid":2,"seq":1,"msg":{"market_tickers":["KXBTCD-26NOV1017-T63999.99"]}}"#;
    let ok = parse_message(text(ok)).unwrap().unwrap();
    assert!(matches!(ok.body, Body::Ok));
    assert_eq!((ok.sid, ok.seq), (Some(2), Some(1)));
    // Also as Kalshi sent it, on the market lifecycle subscription.
    let event = r#"{"type":"event_lifecycle","sid":1,"seq":39,"msg":{"event_ticker":"KXQUICKSETTLE-10NOV26H1810",
        "series_ticker":"KXQUICKSETTLE"},"sending_ts_ms":1794348059267}"#;
    let event = parse_message(text(event)).unwrap().unwrap();
    assert!(matches!(event.body, Body::EventLifecycle));
    assert_eq!((event.sid, event.seq), (Some(1), Some(39)));
    let error = r#"{"id":1,"type":"error","msg":{"code":8,"msg":"Unknown channel"}}"#;
    assert!(parse_message(text(error)).is_err());
}

#[test]
fn parses_the_documented_orderbook_examples() {
    let frame = parse_message(text(
        r#"{"type":"orderbook_snapshot","sid":2,"seq":2,"msg":{"market_ticker":"FED-23DEC-T3.00",
            "clock":4182057,"yes_dollars_fp":[["0.0800","300.00"]],"no_dollars_fp":[["0.5400","20.00"]]}}"#,
    ))
    .unwrap()
    .unwrap();
    let Body::BookSnapshot { symbol, snapshot } = frame.body else { panic!("expected snapshot") };
    assert_eq!(symbol, "FED-23DEC-T3.00");
    assert_eq!(snapshot.clock, 4182057);
    assert_eq!(snapshot.yes, [(80_000, 300 * ONE)]);
    assert_eq!(snapshot.no, [(540_000, 20 * ONE)]);

    let frame = parse_message(text(
        r#"{"type":"orderbook_delta","sending_ts_ms":1669149841123,"sid":2,"seq":3,
            "msg":{"market_ticker":"FED-23DEC-T3.00","clock":4182061,"price_dollars":"0.9600",
            "delta_fp":"-54.00","side":"yes","ts_ms":1669149841000}}"#,
    ))
    .unwrap()
    .unwrap();
    assert_eq!(frame.timestamp_ms, Some(1669149841000), "ts_ms wins over sending_ts_ms");
    let Body::BookDelta { delta, .. } = frame.body else { panic!("expected delta") };
    assert_eq!((delta.side, delta.price, delta.delta), (BookSide::Yes, 960_000, -54 * ONE));

    // An empty side is omitted entirely.
    let empty = r#"{"type":"orderbook_snapshot","sid":2,"seq":4,"msg":{"market_ticker":"X","clock":0}}"#;
    let Body::BookSnapshot { snapshot, .. } = parse_message(text(empty)).unwrap().unwrap().body else {
        panic!("expected snapshot")
    };
    assert!(snapshot.yes.is_empty() && snapshot.no.is_empty());
}

#[test]
fn sequence_gaps_are_detected_per_subscription() {
    let (mut kalshi, _events) = Kalshi::new(KalshiConfig::demo()).unwrap();
    assert!(!kalshi.is_gap(1, 7), "the first message sets the baseline");
    assert!(!kalshi.is_gap(1, 8));
    assert!(!kalshi.is_gap(2, 1), "another subscription has its own count");
    assert!(kalshi.is_gap(1, 10));
    assert!(!kalshi.is_gap(1, 11), "counting resumes from the new baseline");
}

#[test]
fn gaps_close_together_share_one_refetch() {
    let (mut kalshi, _events) = Kalshi::new(KalshiConfig::demo()).unwrap();
    let load = |kalshi: &mut Kalshi, symbol: &str| {
        let snapshot = BookSnapshot { clock: 1, exchange_ms: None, yes: vec![], no: vec![] };
        let book = kalshi.books.entry(symbol.to_owned()).or_insert_with(|| Synced::new(Book::default()));
        book.on_snapshot(snapshot, &mut Vec::new()).unwrap();
    };
    load(&mut kalshi, "B");
    load(&mut kalshi, "A");

    // The first gap is answered at once, for every book.
    kalshi.on_book_gap();
    assert!(kalshi.refetch_at.unwrap() <= tokio::time::Instant::now());
    assert_eq!(kalshi.take_stale_books(), ["A", "B"]);
    assert!(kalshi.refetch_at.is_none());
    let sent = kalshi.last_refetch.unwrap();

    // A's new snapshot arrives, then two more gaps before B's does.
    load(&mut kalshi, "A");
    kalshi.on_book_gap();
    kalshi.on_book_gap();
    assert_eq!(kalshi.refetch_at, Some(sent + REFETCH_COOLDOWN), "one re-fetch, after the cooldown");
    assert_eq!(kalshi.take_stale_books(), ["A", "B"], "a gap taints every book again");

    // A book that loaded a snapshot after the last gap is not asked for again.
    kalshi.on_book_gap();
    load(&mut kalshi, "A");
    assert_eq!(kalshi.take_stale_books(), ["B"]);
}

#[test]
fn every_book_in_sync_is_published_each_interval() {
    let (mut kalshi, events) = Kalshi::new(KalshiConfig::demo()).unwrap();
    let book = |exchange_ms, yes| {
        let mut book = Synced::new(Book::default());
        let snapshot = BookSnapshot { clock: 1, exchange_ms, yes, no: vec![(580_000, ONE)] };
        book.on_snapshot(snapshot, &mut Vec::new()).unwrap();
        book
    };
    kalshi.books.insert("B".to_owned(), book(Some(1_700_000_000_000), vec![(400_000, ONE)]));
    kalshi.books.insert("A".to_owned(), book(None, vec![]));
    kalshi.books.insert("WAITING".to_owned(), Synced::new(Book::default()));
    let due = kalshi.next_snapshot;

    kalshi.publish_snapshots();

    let published: Vec<Event> = events.try_iter().collect();
    let [
        Event::Snapshot { symbol: first, bids: first_bids, .. },
        Event::Snapshot { symbol: second, bids, asks, received_at, exchange_timestamp },
    ] = published.as_slice()
    else {
        panic!("expected two snapshots (none for the book still waiting), got {published:?}");
    };
    assert_eq!((first.as_str(), second.as_str()), ("A", "B"), "in symbol order");
    assert!(first_bids.is_empty(), "an empty side is still published");
    assert_eq!((bids.as_slice(), asks.as_slice()), ([(400_000, ONE)].as_slice(), [(420_000, ONE)].as_slice()));
    assert_eq!(exchange_timestamp.timestamp_millis(), 1_700_000_000_000);
    assert!(received_at > exchange_timestamp);
    assert_eq!(kalshi.next_snapshot, due + Duration::from_secs(60));
}

/// Serves one page of open markets, "A" and "B", to every request.
async fn mock_rest(listener: TcpListener) {
    loop {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut request = Vec::new();
        while !request.windows(4).any(|w| w == b"\r\n\r\n") {
            let mut chunk = [0u8; 1024];
            let n = stream.read(&mut chunk).await.unwrap();
            request.extend_from_slice(&chunk[..n]);
        }
        let request = String::from_utf8_lossy(&request);
        assert!(request.contains("mve_filter=exclude"), "MVEs must be excluded: {request}");
        let body = r#"{"markets":[{"ticker":"A","close_time":"2099-01-01T00:00:00Z"},
            {"ticker":"B","close_time":"2099-01-01T00:00:00Z"}],"cursor":""}"#;
        let response = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
            body.len()
        );
        stream.write_all(response.as_bytes()).await.unwrap();
    }
}

type ServerWs = WebSocketStream<TcpStream>;

async fn next_command(ws: &mut ServerWs) -> Value {
    loop {
        if let Message::Text(text) = ws.next().await.unwrap().unwrap() {
            return serde_json::from_str(text.as_str()).unwrap();
        }
    }
}

async fn send(ws: &mut ServerWs, json: Value) {
    ws.send(Message::Text(json.to_string().into())).await.unwrap();
}

fn snapshot(seq: u64, ticker: &str, clock: i64, yes_quantity: &str) -> Value {
    json!({"type": "orderbook_snapshot", "sid": 2, "seq": seq, "sending_ts_ms": 1_700_000_000_000_i64,
        "msg": {"market_ticker": ticker, "clock": clock,
            "yes_dollars_fp": [["0.4000", yes_quantity]], "no_dollars_fp": [["0.5800", "10.00"]]}})
}

fn delta(seq: u64, ticker: &str, clock: i64, delta: &str) -> Value {
    json!({"type": "orderbook_delta", "sid": 2, "seq": seq,
        "msg": {"market_ticker": ticker, "clock": clock, "price_dollars": "0.4000",
            "delta_fp": delta, "side": "yes", "ts_ms": 1_700_000_000_500_i64}})
}

fn lifecycle(seq: u64, ticker: &str, event_type: &str) -> Value {
    json!({"type": "market_lifecycle_v2", "sid": 1, "seq": seq,
        "msg": {"market_ticker": ticker, "event_type": event_type}})
}

/// Plays Kalshi's side of one connection and checks every command we send.
/// Returns the connection so the test can watch it being closed.
async fn mock_websocket(listener: TcpListener) -> ServerWs {
    let (stream, _) = listener.accept().await.unwrap();
    let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();

    let command = next_command(&mut ws).await;
    assert_eq!(command["cmd"], "subscribe");
    assert_eq!(command["params"]["channels"], json!(["market_lifecycle_v2"]));

    // Once REST has answered, both symbols are subscribed in one command.
    let command = next_command(&mut ws).await;
    assert_eq!(command["cmd"], "subscribe");
    assert_eq!(command["params"]["channels"], json!(["orderbook_delta"]));
    assert_eq!(command["params"]["market_tickers"], json!(["A", "B"]));

    send(&mut ws, snapshot(1, "A", 100, "20.00")).await;
    send(&mut ws, snapshot(2, "B", 100, "30.00")).await;
    send(&mut ws, delta(3, "A", 101, "-5.00")).await;

    // seq 4 never arrives. Every book must be re-fetched.
    send(&mut ws, delta(5, "A", 103, "-1.00")).await;
    let command = next_command(&mut ws).await;
    assert_eq!(command["cmd"], "update_subscription");
    assert_eq!(command["params"]["action"], "get_snapshot");
    assert_eq!(command["params"]["sids"], json!([2]));
    assert_eq!(command["params"]["market_tickers"], json!(["A", "B"]));
    // The fresh snapshots already include the delta sent at clock 103.
    send(&mut ws, snapshot(6, "A", 103, "12.00")).await;
    send(&mut ws, snapshot(7, "B", 103, "30.00")).await;
    send(&mut ws, delta(8, "A", 104, "2.00")).await;

    // B settles: unsubscribe it.
    send(&mut ws, lifecycle(1, "B", "settled")).await;
    let command = next_command(&mut ws).await;
    assert_eq!(command["params"]["action"], "delete_markets");
    assert_eq!(command["params"]["market_tickers"], json!(["B"]));
    // Kalshi's reply takes the next sequence number. Counting it is what
    // keeps the deltas after it from looking like a gap.
    send(&mut ws, json!({"type": "ok", "id": 4, "sid": 2, "seq": 9, "msg": {"market_tickers": ["A"]}})).await;
    // A late delta for B must be ignored, not crash anything.
    send(&mut ws, delta(10, "B", 105, "1.00")).await;
    send(&mut ws, delta(11, "A", 105, "1.00")).await;

    // C opens: subscribe it.
    send(&mut ws, lifecycle(2, "C", "activated")).await;
    let command = next_command(&mut ws).await;
    assert_eq!(command["cmd"], "subscribe");
    assert_eq!(command["params"]["market_tickers"], json!(["C"]));
    ws
}

/// Events with the timestamps left out, for comparing.
fn summarize(event: Event) -> String {
    match event {
        Event::Symbol(SymbolChange::Added(symbol)) => format!("+{symbol}"),
        Event::Symbol(SymbolChange::Removed(symbol)) => format!("-{symbol}"),
        Event::Snapshot { symbol, bids, asks, .. } => {
            format!("snapshot {symbol} bids={bids:?} asks={asks:?}")
        }
        Event::Level { symbol, side, price_level, quantity, exchange_timestamp, .. } => {
            assert_eq!(exchange_timestamp.timestamp_millis(), 1_700_000_000_500);
            format!("level {symbol} {side} {price_level} {quantity}")
        }
    }
}

#[tokio::test]
async fn stays_in_sync_with_a_mock_kalshi() {
    let rest = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let websocket = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = KalshiConfig::demo();
    config.rest_url = format!("http://{}/trade-api/v2", rest.local_addr().unwrap());
    config.ws_url = format!("ws://{}/trade-api/ws/v2", websocket.local_addr().unwrap());
    let (mut kalshi, events) = Kalshi::new(config).unwrap();

    tokio::spawn(mock_rest(rest));
    let script = tokio::spawn(mock_websocket(websocket));
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let client = tokio::spawn(async move {
        kalshi.run(async { let _ = stopped.await; }).await;
    });

    let mut server = tokio::time::timeout(Duration::from_secs(10), script)
        .await
        .expect("the mock script should finish")
        .expect("every command should match the script");

    // Shutdown: Kalshi's side must see a proper close, and `run` must return.
    stop.send(()).unwrap();
    let mut closed = false;
    while let Some(Ok(message)) = tokio::time::timeout(Duration::from_secs(5), server.next())
        .await
        .expect("the websocket should be closed on shutdown")
    {
        closed |= message.is_close();
    }
    assert!(closed, "a close frame should be sent");
    tokio::time::timeout(Duration::from_secs(5), client)
        .await
        .expect("run should return after shutdown")
        .unwrap();

    let mut seen: Vec<String> = events.try_iter().map(summarize).collect();
    seen[..2].sort(); // A and B are added in no particular order
    let asks = "[(420000, 10000000)]";
    assert_eq!(
        seen,
        [
            "+A".to_owned(),
            "+B".to_owned(),
            format!("snapshot A bids=[(400000, 20000000)] asks={asks}"),
            format!("snapshot B bids=[(400000, 30000000)] asks={asks}"),
            "level A Buy 400000 15000000".to_owned(),
            // The gap: the delta at seq 5 is held, then dropped as stale.
            format!("snapshot A bids=[(400000, 12000000)] asks={asks}"),
            format!("snapshot B bids=[(400000, 30000000)] asks={asks}"),
            "level A Buy 400000 14000000".to_owned(),
            "-B".to_owned(),
            // Applied, so the `ok` before it was not taken for a gap.
            "level A Buy 400000 15000000".to_owned(),
            "+C".to_owned(),
        ]
    );
}

/// Live check against the demo environment; needs network, no API key.
/// Run with `cargo test live -- --ignored --nocapture`.
#[tokio::test]
#[ignore]
async fn live_demo_rest() {
    let config = KalshiConfig::demo();
    let started = Instant::now();
    let url = format!("{}/markets", config.rest_url);
    let open = fetch_open_markets(reqwest::Client::new(), None, url, None).await.unwrap();
    let past = open.markets.values().filter(|c| c.is_some_and(|c| c <= open.fetched_at)).count();
    let combos = open.markets.keys().filter(|t| t.starts_with("KXMVE")).count();
    println!(
        "REST: {} open regular markets in {:?}; {past} already past close time; {combos} look like MVEs",
        open.markets.len(),
        started.elapsed(),
    );
    assert!(!open.markets.is_empty());
}
