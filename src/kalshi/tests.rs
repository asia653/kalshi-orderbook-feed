//! Tests for [`Kalshi`] as a whole: message parsing, the REST fetch, and
//! the whole loop driven by a mock Kalshi on localhost.

use std::time::Instant;

use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message;

use super::book::{Book, BookSide, BookSnapshot, ONE};
use super::wire::{Body, parse_message};
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
fn every_book_in_sync_is_published_each_interval() {
    let (mut kalshi, events) = Kalshi::new(KalshiConfig::demo()).unwrap();
    let book = |exchange_ms, yes| {
        let mut book = Synced::new(Book::default());
        let snapshot = BookSnapshot { clock: 1, exchange_ms, yes, no: vec![(580_000, ONE)] };
        book.on_snapshot(snapshot, &mut Vec::new()).unwrap();
        book
    };
    let books = &mut kalshi.connections[0].books;
    books.insert("B".to_owned(), book(Some(1_700_000_000_000), vec![(400_000, ONE)]));
    books.insert("A".to_owned(), book(None, vec![]));
    books.insert("WAITING".to_owned(), Synced::new(Book::default()));
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
    serve_markets(listener, vec!["A".to_owned(), "B".to_owned()]).await
}

/// Serves `tickers` as one page of open markets to every request.
async fn serve_markets(listener: TcpListener, tickers: Vec<String>) {
    let markets: Vec<Value> =
        tickers.iter().map(|ticker| json!({"ticker": ticker, "close_time": "2099-01-01T00:00:00Z"})).collect();
    let body = json!({"markets": markets, "cursor": ""}).to_string();
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
        let head = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
            body.len()
        );
        stream.write_all(head.as_bytes()).await.unwrap();
        stream.write_all(body.as_bytes()).await.unwrap();
    }
}

#[tokio::test]
async fn a_rate_limited_page_is_asked_for_again() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = reqwest::Url::parse(&format!("http://{}/trade-api/v2/markets", listener.local_addr().unwrap())).unwrap();
    let served = tokio::spawn(async move {
        let body = r#"{"markets":[{"ticker":"A","close_time":"2099-01-01T00:00:00Z"}],"cursor":""}"#;
        let ok = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
            body.len()
        );
        let limited = "HTTP/1.1 429 Too Many Requests\r\ncontent-length: 0\r\nconnection: close\r\n\r\n".to_owned();
        for response in [limited, ok] {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            while !request.windows(4).any(|w| w == b"\r\n\r\n") {
                let mut chunk = [0u8; 1024];
                let n = stream.read(&mut chunk).await.unwrap();
                request.extend_from_slice(&chunk[..n]);
            }
            stream.write_all(response.as_bytes()).await.unwrap();
        }
    });

    let open = page_open_markets(&reqwest::Client::new(), &None, &url, None).await.unwrap();
    assert_eq!(open.markets.keys().collect::<Vec<_>>(), ["A"]);
    served.await.unwrap();
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
    let (mut kalshi, events, rest, websocket) = against_mocks().await;
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

/// A `Kalshi` pointed at mocks on localhost, and the two listeners.
async fn against_mocks() -> (Kalshi, Receiver<Event>, TcpListener, TcpListener) {
    let rest = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let websocket = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = KalshiConfig::demo();
    config.rest_url = format!("http://{}/trade-api/v2", rest.local_addr().unwrap());
    config.ws_url = format!("ws://{}/trade-api/ws/v2", websocket.local_addr().unwrap());
    let (kalshi, events) = Kalshi::new(config).unwrap();
    (kalshi, events, rest, websocket)
}

async fn accept(listener: &TcpListener) -> ServerWs {
    let (stream, _) = listener.accept().await.unwrap();
    tokio_tungstenite::accept_async(stream).await.unwrap()
}

#[tokio::test]
async fn a_lost_connection_is_reopened_and_resubscribed() {
    let (mut kalshi, events, rest, websocket) = against_mocks().await;
    tokio::spawn(mock_rest(rest));
    let script = tokio::spawn(async move {
        let mut ws = accept(&websocket).await;
        assert_eq!(next_command(&mut ws).await["params"]["channels"], json!(["market_lifecycle_v2"]));
        assert_eq!(next_command(&mut ws).await["params"]["market_tickers"], json!(["A", "B"]));
        send(&mut ws, snapshot(1, "A", 100, "20.00")).await;
        // The connection dies without a word.
        drop(ws);

        // The new socket is told about every symbol at once, without
        // waiting for REST, and Kalshi starts its numbering again.
        let mut ws = accept(&websocket).await;
        assert_eq!(next_command(&mut ws).await["params"]["channels"], json!(["market_lifecycle_v2"]));
        let command = next_command(&mut ws).await;
        assert_eq!(command["params"]["channels"], json!(["orderbook_delta"]));
        assert_eq!(command["params"]["market_tickers"], json!(["A", "B"]));
        send(&mut ws, snapshot(1, "A", 107, "12.00")).await;
        send(&mut ws, snapshot(2, "B", 107, "30.00")).await;
        send(&mut ws, delta(3, "A", 108, "2.00")).await;
        // The reply to this proves everything above was handled.
        send(&mut ws, lifecycle(1, "B", "settled")).await;
        assert_eq!(next_command(&mut ws).await["params"]["action"], "delete_markets");
        ws
    });
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let client = tokio::spawn(async move {
        kalshi.run(async { let _ = stopped.await; }).await;
    });
    let _server = tokio::time::timeout(Duration::from_secs(10), script)
        .await
        .expect("the client should reconnect")
        .expect("every command should match the script");
    stop.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(5), client).await.expect("run should return").unwrap();

    let mut seen: Vec<String> = events.try_iter().map(summarize).collect();
    seen[..2].sort();
    let asks = "[(420000, 10000000)]";
    assert_eq!(
        seen,
        [
            "+A".to_owned(),
            "+B".to_owned(),
            format!("snapshot A bids=[(400000, 20000000)] asks={asks}"),
            // After the reconnect every book starts again from a snapshot.
            format!("snapshot A bids=[(400000, 12000000)] asks={asks}"),
            format!("snapshot B bids=[(400000, 30000000)] asks={asks}"),
            "level A Buy 400000 14000000".to_owned(),
            "-B".to_owned(),
        ]
    );
}

#[tokio::test]
async fn many_symbols_are_subscribed_in_batches_in_order() {
    let (mut kalshi, _events, rest, websocket) = against_mocks().await;
    let tickers: Vec<String> = (0..1200).map(|n| format!("M{n:04}")).collect();
    tokio::spawn(serve_markets(rest, tickers.clone()));
    let script = tokio::spawn(async move {
        let mut ws = accept(&websocket).await;
        assert_eq!(next_command(&mut ws).await["params"]["channels"], json!(["market_lifecycle_v2"]));
        let mut asked = Vec::new();
        for expected in [500, 500, 200] {
            let command = next_command(&mut ws).await;
            let batch = command["params"]["market_tickers"].as_array().unwrap().clone();
            assert_eq!(batch.len(), expected);
            asked.extend(batch.into_iter().map(|ticker| ticker.as_str().unwrap().to_owned()));
        }
        assert_eq!(asked, tickers);
    });
    tokio::select! {
        _ = kalshi.run(std::future::pending()) => unreachable!("run only returns on shutdown"),
        done = tokio::time::timeout(Duration::from_secs(10), script) => done.expect("in time").unwrap(),
    }
}

/// Kalshi stops reading while megabytes of subscribe commands are still
/// waiting to be sent. Messages it sends must still be read and acted on.
#[tokio::test]
async fn unsent_commands_do_not_stop_messages_being_read() {
    let (mut kalshi, events, rest, websocket) = against_mocks().await;
    // Long tickers: about 18 MB of subscribe commands, more than the socket
    // buffers between the two ends can hold.
    let tickers: Vec<String> = (0..60_000).map(|n| format!("M{n:05}-{}", "X".repeat(290))).collect();
    tokio::spawn(serve_markets(rest, tickers));
    let (go, gone) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(async move {
        let mut ws = accept(&websocket).await;
        assert_eq!(next_command(&mut ws).await["params"]["channels"], json!(["market_lifecycle_v2"]));
        // From here on nothing more is read from the socket.
        gone.await.unwrap();
        send(&mut ws, lifecycle(1, "LATE", "activated")).await;
        std::future::pending::<()>().await;
    });
    let client = tokio::spawn(async move { kalshi.run(std::future::pending()).await });

    // Wait until the symbols are loaded, which queues the commands...
    let wait_for = |wanted: String| {
        let events = events.clone();
        async move {
            loop {
                if events.try_iter().any(|event| event == Event::Symbol(SymbolChange::Added(wanted.clone()))) {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
    };
    let last = format!("M59999-{}", "X".repeat(290));
    tokio::time::timeout(Duration::from_secs(30), wait_for(last))
        .await
        .expect("the symbols should load");
    // ...then have Kalshi speak, and expect to hear it.
    go.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(10), wait_for("LATE".to_owned()))
        .await
        .expect("a message must be read even though commands are still unsent");
    client.abort();
    server.abort();
}

/// Live check against the demo environment; needs network, no API key.
/// Run with `cargo test live -- --ignored --nocapture`.
#[tokio::test]
#[ignore]
async fn live_demo_rest() {
    let config = KalshiConfig::demo();
    let started = Instant::now();
    let url = reqwest::Url::parse(&format!("{}/markets", config.rest_url)).unwrap();
    let open = page_open_markets(&reqwest::Client::new(), &None, &url, None).await.unwrap();
    let past = open.markets.values().filter(|c| c.is_some_and(|c| c <= open.fetched_at)).count();
    let combos = open.markets.keys().filter(|t| t.starts_with("KXMVE")).count();
    println!(
        "REST: {} open regular markets in {:?}; {past} already past close time; {combos} look like MVEs",
        open.markets.len(),
        started.elapsed(),
    );
    assert!(!open.markets.is_empty());
}
