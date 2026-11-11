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
    // As Kalshi sent it, shortly before closing the socket by itself.
    let ended = r#"{"type":"unsubscribed","sid":2,"seq":77545,"sending_ts_ms":1794352588797}"#;
    assert!(matches!(parse_message(text(ended)).unwrap().unwrap().body, Body::Unsubscribed));
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
    let reading = async {
        loop {
            if let Message::Text(text) = ws.next().await.unwrap().unwrap() {
                return serde_json::from_str(text.as_str()).unwrap();
            }
        }
    };
    tokio::time::timeout(Duration::from_secs(5), reading).await.expect("a command should arrive")
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
    let (mut kalshi, events, rest, websocket) = against_mocks(1).await;
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
async fn against_mocks(connections: usize) -> (Kalshi, Receiver<Event>, TcpListener, TcpListener) {
    against_mocks_with(|config| config.connections = connections).await
}

async fn against_mocks_with(
    adjust: impl FnOnce(&mut KalshiConfig),
) -> (Kalshi, Receiver<Event>, TcpListener, TcpListener) {
    let rest = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let websocket = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = KalshiConfig::demo();
    config.rest_url = format!("http://{}/trade-api/v2", rest.local_addr().unwrap());
    config.ws_url = format!("ws://{}/trade-api/ws/v2", websocket.local_addr().unwrap());
    config.connections = 1;
    adjust(&mut config);
    let (kalshi, events) = Kalshi::new(config).unwrap();
    (kalshi, events, rest, websocket)
}

async fn accept(listener: &TcpListener) -> ServerWs {
    let (stream, _) = listener.accept().await.unwrap();
    tokio_tungstenite::accept_async(stream).await.unwrap()
}

#[tokio::test]
async fn a_lost_connection_is_reopened_and_resubscribed() {
    let (mut kalshi, events, rest, websocket) = against_mocks(1).await;
    tokio::spawn(mock_rest(rest));
    let script = tokio::spawn(async move {
        let mut ws = accept(&websocket).await;
        assert_eq!(next_command(&mut ws).await["params"]["channels"], json!(["market_lifecycle_v2"]));
        assert_eq!(next_command(&mut ws).await["params"]["market_tickers"], json!(["A", "B"]));
        send(&mut ws, snapshot(1, "A", 100, "20.00")).await;
        // Kalshi ends the subscription but leaves the socket open. The
        // service must not sit on it: it hangs up and connects again.
        send(&mut ws, json!({"type": "unsubscribed", "sid": 2, "seq": 2})).await;
        while let Some(Ok(_)) = ws.next().await {}

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
    let (mut kalshi, _events, rest, websocket) = against_mocks(1).await;
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
    let (mut kalshi, events, rest, websocket) = against_mocks(1).await;
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

/// Two mock sockets that have both been subscribed to A and B and have
/// each sent their snapshots, plus what the service has published so far.
/// The sockets send different quantities (20 and 30, or 21 and 31), so what
/// is published shows which socket the service is using as its source.
struct Pair {
    source: ServerWs,
    standby: ServerWs,
    /// The quantities the standby sent for A and B, in contracts.
    standby_sent: (i64, i64),
    events: Receiver<Event>,
    seen: Vec<String>,
}

impl Pair {
    async fn start(events: Receiver<Event>, websocket: &TcpListener) -> Self {
        let [mut first, mut second] = [accept(websocket).await, accept(websocket).await];
        for ws in [&mut first, &mut second] {
            assert_eq!(next_command(ws).await["params"]["channels"], json!(["market_lifecycle_v2"]));
            assert_eq!(next_command(ws).await["params"]["market_tickers"], json!(["A", "B"]));
        }
        // Whichever connection has all of its books first is the source.
        // Loading one socket before the other settles which that is.
        send(&mut first, snapshot(1, "A", 100, "20.00")).await;
        send(&mut first, snapshot(2, "B", 100, "30.00")).await;
        let mut pair = Pair { source: first, standby: second, standby_sent: (21, 31), events, seen: Vec::new() };
        pair.wait_for(4).await;
        pair.seen[..2].sort();
        assert_eq!(pair.seen, ["+A".to_owned(), "+B".to_owned(), snapshot_event("A", 20), snapshot_event("B", 30)]);
        send(&mut pair.standby, snapshot(1, "A", 100, "21.00")).await;
        send(&mut pair.standby, snapshot(2, "B", 100, "31.00")).await;
        pair
    }

    /// Waits until `count` events have been published in total.
    async fn wait_for(&mut self, count: usize) {
        let collecting = async {
            while self.seen.len() < count {
                self.seen.extend(self.events.try_iter().map(summarize));
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        };
        if tokio::time::timeout(Duration::from_secs(5), collecting).await.is_err() {
            panic!("expected {count} events, got {:?}", self.seen);
        }
    }
}

fn snapshot_event(symbol: &str, contracts: i64) -> String {
    format!("snapshot {symbol} bids=[(400000, {})] asks=[(420000, 10000000)]", contracts * ONE)
}

#[tokio::test]
async fn the_standby_takes_over_when_the_source_is_cut() {
    let (mut kalshi, events, rest, websocket) = against_mocks(2).await;
    tokio::spawn(mock_rest(rest));
    let (cut, cuts) = tokio::sync::mpsc::unbounded_channel::<()>();
    let cuts = Box::pin(futures_util::stream::unfold(cuts, |mut cuts| async move {
        cuts.recv().await.map(|()| ((), cuts))
    }));
    let client = tokio::spawn(async move { kalshi.run_with_faults(std::future::pending(), cuts).await });
    let mut pair = Pair::start(events, &websocket).await;
    let (a, b) = pair.standby_sent;

    cut.send(()).unwrap();
    // The source's socket ends without a closing handshake.
    let ending = async { while let Some(Ok(_)) = pair.source.next().await {} };
    tokio::time::timeout(Duration::from_secs(5), ending).await.expect("the source should be cut");

    // Every book the standby holds is published, then its changes follow.
    pair.wait_for(6).await;
    send(&mut pair.standby, delta(3, "A", 101, "-5.00")).await;
    pair.wait_for(7).await;
    // Lifecycle messages are now taken from the standby too.
    send(&mut pair.standby, lifecycle(1, "B", "settled")).await;
    assert_eq!(next_command(&mut pair.standby).await["params"]["action"], "delete_markets");
    pair.wait_for(8).await;
    assert_eq!(
        pair.seen[4..],
        [
            snapshot_event("A", a),
            snapshot_event("B", b),
            format!("level A Buy 400000 {}", (a - 5) * ONE),
            "-B".to_owned(),
        ]
    );

    // The cut connection comes back, catches up quietly, and is ready to
    // take over in its turn.
    let mut returned = accept(&websocket).await;
    assert_eq!(next_command(&mut returned).await["params"]["channels"], json!(["market_lifecycle_v2"]));
    assert_eq!(next_command(&mut returned).await["params"]["market_tickers"], json!(["A"]));
    send(&mut returned, snapshot(1, "A", 105, "40.00")).await;
    cut.send(()).unwrap();
    // Its book is published once it has loaded and the other socket is gone.
    // (REST is asked again too, and the mock still lists B, so "+B" may
    // come before or after it.)
    while !pair.seen[8..].contains(&snapshot_event("A", 40)) {
        pair.wait_for(pair.seen.len() + 1).await;
    }
    client.abort();
}

#[tokio::test]
async fn the_standby_takes_over_when_the_source_misses_a_message() {
    let (mut kalshi, events, rest, websocket) = against_mocks(2).await;
    tokio::spawn(mock_rest(rest));
    let client = tokio::spawn(async move { kalshi.run(std::future::pending()).await });
    let mut pair = Pair::start(events, &websocket).await;
    let (a, b) = pair.standby_sent;

    // seq 3 never arrives on the source.
    send(&mut pair.source, delta(4, "A", 102, "-1.00")).await;
    pair.wait_for(6).await;

    // The old source re-fetches its books, but none of that is published.
    let command = next_command(&mut pair.source).await;
    assert_eq!(command["params"]["action"], "get_snapshot");
    assert_eq!(command["params"]["market_tickers"], json!(["A", "B"]));
    send(&mut pair.source, snapshot(5, "A", 102, "50.00")).await;
    send(&mut pair.source, snapshot(6, "B", 102, "60.00")).await;
    send(&mut pair.source, delta(7, "A", 103, "1.00")).await;

    send(&mut pair.standby, delta(3, "A", 101, "-5.00")).await;
    pair.wait_for(7).await;
    // Kalshi sends lifecycle messages on both sockets; one of them counts.
    // Both sockets being told to unsubscribe shows both handled all of the above.
    send(&mut pair.source, lifecycle(1, "B", "settled")).await;
    send(&mut pair.standby, lifecycle(1, "B", "settled")).await;
    assert_eq!(next_command(&mut pair.source).await["params"]["action"], "delete_markets");
    assert_eq!(next_command(&mut pair.standby).await["params"]["action"], "delete_markets");
    pair.wait_for(8).await;
    assert_eq!(
        pair.seen[4..],
        [
            snapshot_event("A", a),
            snapshot_event("B", b),
            format!("level A Buy 400000 {}", (a - 5) * ONE),
            "-B".to_owned(),
        ]
    );
    client.abort();
}

/// Waits for a mock socket to be hung up on.
async fn expect_hang_up(ws: &mut ServerWs) {
    let ending = async { while let Some(Ok(_)) = ws.next().await {} };
    tokio::time::timeout(Duration::from_secs(5), ending).await.expect("the service should hang up");
}

fn now_ms() -> i64 {
    Utc::now().timestamp_millis()
}

#[tokio::test]
async fn a_quiet_source_is_cut_and_the_standby_takes_over() {
    let (mut kalshi, events, rest, websocket) = against_mocks_with(|config| {
        config.connections = 2;
        config.health.check_every = Duration::from_millis(20);
        config.health.quiet_after = Duration::from_millis(200);
    })
    .await;
    tokio::spawn(mock_rest(rest));
    let client = tokio::spawn(async move { kalshi.run(std::future::pending()).await });
    let mut pair = Pair::start(events, &websocket).await;
    let (a, b) = pair.standby_sent;

    // The source's socket stays open but says nothing more, while the
    // standby hears a change.
    tokio::time::sleep(Duration::from_millis(300)).await;
    send(&mut pair.standby, delta(3, "A", 101, "-5.00")).await;

    expect_hang_up(&mut pair.source).await;
    pair.wait_for(6).await;
    assert_eq!(pair.seen[4..], [snapshot_event("A", a - 5), snapshot_event("B", b)]);
    client.abort();
}

#[tokio::test]
async fn a_lagging_source_hands_over_and_reconnects() {
    let (mut kalshi, events, rest, websocket) = against_mocks_with(|config| {
        config.connections = 2;
        config.health.check_every = Duration::from_millis(50);
    })
    .await;
    tokio::spawn(mock_rest(rest));
    let client = tokio::spawn(async move { kalshi.run(std::future::pending()).await });
    let mut pair = Pair::start(events, &websocket).await;
    let (_, b) = pair.standby_sent;

    // Both sockets carry the same changes, but the source's were queued by
    // Kalshi two seconds before they arrive.
    let timed = |seq: u64, sent_ms: i64| {
        let mut message = delta(seq, "A", 98 + seq as i64, "-0.01");
        message["sending_ts_ms"] = json!(sent_ms);
        Message::Text(message.to_string().into())
    };
    let mut hung_up = false;
    for seq in 3..40 {
        // Sending to the slow socket fails once the service has cut it.
        hung_up |= pair.source.send(timed(seq, now_ms() - 2_000)).await.is_err();
        pair.standby.send(timed(seq, now_ms())).await.unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    // B never changed, so its snapshot tells the two sockets apart: the
    // fast connection's books were published when it became the source.
    while !pair.seen[4..].contains(&snapshot_event("B", b)) {
        pair.wait_for(pair.seen.len() + 1).await;
    }
    // The slow socket is not kept: it is cut, and a new one is opened.
    if !hung_up {
        expect_hang_up(&mut pair.source).await;
    }
    let mut again = tokio::time::timeout(Duration::from_secs(5), accept(&websocket))
        .await
        .expect("the lagging connection should connect again");
    assert_eq!(next_command(&mut again).await["params"]["channels"], json!(["market_lifecycle_v2"]));
    // The fast connection was left alone throughout.
    let untouched = tokio::time::timeout(Duration::from_millis(200), pair.standby.next()).await;
    assert!(untouched.is_err(), "the fast connection should stay open, got {untouched:?}");
    client.abort();
}

#[tokio::test]
async fn connections_still_loading_their_books_are_not_judged() {
    let (mut kalshi, _events, rest, websocket) = against_mocks_with(|config| {
        config.connections = 2;
        config.health.check_every = Duration::from_millis(20);
    })
    .await;
    tokio::spawn(mock_rest(rest));
    let client = tokio::spawn(async move { kalshi.run(std::future::pending()).await });

    // Both sockets open before the symbol list is known, and then load A
    // but not yet B: neither has every book, as during the burst of
    // snapshots at startup.
    let mut sockets = [accept(&websocket).await, accept(&websocket).await];
    for ws in &mut sockets {
        assert_eq!(next_command(ws).await["params"]["channels"], json!(["market_lifecycle_v2"]));
        assert_eq!(next_command(ws).await["params"]["market_tickers"], json!(["A", "B"]));
        send(ws, snapshot(1, "A", 100, "20.00")).await;
    }
    // One of them is two seconds behind the other, for many checks.
    for seq in 2..30 {
        for (ws, behind) in sockets.iter_mut().zip([2_000, 0]) {
            let mut message = delta(seq, "A", 98 + seq as i64, "-0.01");
            message["sending_ts_ms"] = json!(now_ms() - behind);
            send(ws, message).await;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    for ws in &mut sockets {
        let left_alone = tokio::time::timeout(Duration::from_millis(100), ws.next()).await;
        assert!(left_alone.is_err(), "a connection that is still loading is not cut, got {left_alone:?}");
    }
    client.abort();
}

#[tokio::test]
async fn a_socket_with_no_pings_is_dead() {
    let (mut kalshi, _events, rest, websocket) =
        against_mocks_with(|config| config.health.dead_after = Duration::from_millis(300)).await;
    tokio::spawn(mock_rest(rest));
    let client = tokio::spawn(async move { kalshi.run(std::future::pending()).await });

    // This mock never sends pings, as a dead line would not.
    let mut ws = accept(&websocket).await;
    assert_eq!(next_command(&mut ws).await["params"]["channels"], json!(["market_lifecycle_v2"]));
    expect_hang_up(&mut ws).await;
    let reconnected = tokio::time::timeout(Duration::from_secs(5), accept(&websocket)).await;
    assert!(reconnected.is_ok(), "the service should connect again");
    client.abort();
}

#[tokio::test]
async fn a_lone_connection_that_falls_silent_reconnects() {
    let (mut kalshi, _events, rest, websocket) = against_mocks_with(|config| {
        config.health.check_every = Duration::from_millis(20);
        config.health.silence_floor = Duration::from_millis(300);
        config.health.silence_factor = 10;
    })
    .await;
    tokio::spawn(mock_rest(rest));
    let client = tokio::spawn(async move { kalshi.run(std::future::pending()).await });

    let mut ws = accept(&websocket).await;
    assert_eq!(next_command(&mut ws).await["params"]["channels"], json!(["market_lifecycle_v2"]));
    assert_eq!(next_command(&mut ws).await["params"]["market_tickers"], json!(["A", "B"]));
    send(&mut ws, snapshot(1, "A", 100, "90.00")).await;
    send(&mut ws, snapshot(2, "B", 100, "30.00")).await;
    // Enough changes, close together, to learn that this market is busy...
    for seq in 3..30 {
        send(&mut ws, delta(seq, "A", 98 + seq as i64, "-1.00")).await;
    }
    // ...then nothing. There is no second connection to compare with, so
    // the long silence itself is the signal.
    expect_hang_up(&mut ws).await;
    client.abort();
}

#[tokio::test]
async fn symbols_are_divided_between_groups_by_event() {
    let (mut kalshi, events, rest, websocket) = against_mocks_with(|config| config.groups = 2).await;
    let tickers: Vec<String> =
        (0..20).flat_map(|event| (0..3).map(move |market| format!("S-E{event:02}-M{market}"))).collect();
    tokio::spawn(serve_markets(rest, tickers.clone()));
    let client = tokio::spawn(async move { kalshi.run(std::future::pending()).await });

    let mut asked: Vec<Vec<String>> = Vec::new();
    let mut sockets = [accept(&websocket).await, accept(&websocket).await];
    for ws in &mut sockets {
        assert_eq!(next_command(ws).await["params"]["channels"], json!(["market_lifecycle_v2"]));
        let command = next_command(ws).await;
        let batch = command["params"]["market_tickers"].as_array().unwrap();
        asked.push(batch.iter().map(|ticker| ticker.as_str().unwrap().to_owned()).collect());
    }
    let mut all: Vec<String> = asked.concat();
    all.sort();
    assert_eq!(all, tickers, "every symbol is followed, by exactly one group");
    assert!(asked.iter().all(|group| !group.is_empty()));
    let event = |ticker: &String| ticker.rsplit_once('-').unwrap().0.to_owned();
    let events_of = |group: &Vec<String>| group.iter().map(event).collect::<HashSet<String>>();
    assert!(events_of(&asked[0]).is_disjoint(&events_of(&asked[1])), "an event's markets stay together");

    // Each group has its own source, and both are published.
    for (ws, group) in sockets.iter_mut().zip(&asked) {
        send(ws, snapshot(1, &group[0], 100, "20.00")).await;
    }
    let wanted = [snapshot_event(&asked[0][0], 20), snapshot_event(&asked[1][0], 20)];
    let published = async {
        let mut seen: Vec<String> = Vec::new();
        while !wanted.iter().all(|event| seen.contains(event)) {
            seen.extend(events.try_iter().map(summarize));
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    };
    tokio::time::timeout(Duration::from_secs(5), published).await.expect("both groups should publish");
    client.abort();
}

#[test]
fn connections_are_named_by_letter_and_group() {
    let mut config = KalshiConfig::demo();
    (config.connections, config.groups) = (2, 1);
    let names = |config: &KalshiConfig| -> Vec<String> {
        (0..config.connections * config.groups).map(|index| connection_name(index, config)).collect()
    };
    assert_eq!(names(&config), ["A", "B"]);
    config.groups = 2;
    assert_eq!(names(&config), ["A1", "B1", "A2", "B2"]);
    (config.connections, config.groups) = (28, 1);
    assert_eq!(names(&config)[25..], ["Z", "AA", "AB"]);
}

#[test]
fn groups_are_stable_and_roughly_even() {
    assert_eq!(group_for("KXBTCD-26NOV1017-T1", 8), group_for("KXBTCD-26NOV1017-T99999.99", 8));
    assert_eq!(group_for("ANYTHING", 1), 0);
    let mut sizes = [0usize; 8];
    for event in 0..8_000 {
        sizes[group_for(&format!("KXSERIES-26NOV{event:04}-T1"), 8)] += 1;
    }
    assert!(sizes.iter().all(|size| (800..1_200).contains(size)), "{sizes:?}");
}

#[test]
fn the_status_line_can_be_printed_before_anything_is_connected() {
    let mut config = KalshiConfig::demo();
    config.groups = 3;
    let (mut kalshi, _events) = Kalshi::new(config).unwrap();
    kalshi.print_status();
    kalshi.check_health();
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
