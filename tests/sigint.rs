//! Runs the real binary against a mock Kalshi on localhost, sends it SIGINT,
//! and reads back what it wrote to stdout.

use std::process::{Child, Command, Stdio};
use std::time::Duration;

use base64::Engine as _;
use engineering_challenge::Side;
use engineering_challenge::util::dtos::challenge_orderbook::{
    AnyMessage, PriceLevelUpdateDomain, SnapshotAsksEntryDomain, SnapshotBidsEntryDomain,
    SnapshotDomain,
};
use engineering_challenge::util::{AlignedSbeReader, OrderbookSchema};
use futures_util::{SinkExt, StreamExt};
use rsa::RsaPrivateKey;
use rsa::pkcs8::{EncodePrivateKey, LineEnding};
use rsa::pss::{Signature, VerifyingKey};
use rsa::signature::Verifier;
use serde_json::{Value, json};
use sha2::Sha256;
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const CHUNK: u64 = 64 << 20;
const TIMEOUT: Duration = Duration::from_secs(20);

/// Kills the service if the test fails before it exits by itself.
struct Service(Child);

impl Drop for Service {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Serves one open market, "A", to every request.
async fn mock_rest(rest: &MockServer) {
    let markets = json!({"markets": [{"ticker": "A", "close_time": "2099-01-01T00:00:00Z"}], "cursor": ""});
    Mock::given(method("GET"))
        .and(path("/trade-api/v2/markets"))
        .respond_with(ResponseTemplate::new(200).set_body_json(markets))
        .mount(rest)
        .await;
}

async fn next_command(ws: &mut WebSocketStream<TcpStream>) -> Value {
    loop {
        if let Message::Text(text) = ws.next().await.unwrap().unwrap() {
            return serde_json::from_str(text.as_str()).unwrap();
        }
    }
}

async fn send(ws: &mut WebSocketStream<TcpStream>, json: Value) {
    ws.send(Message::Text(json.to_string().into())).await.unwrap();
}

#[tokio::test]
async fn sigint_closes_the_websocket_and_the_output() {
    tokio::time::timeout(TIMEOUT, scenario()).await.expect("the scenario should not hang");
}

async fn scenario() {
    let rest = MockServer::start().await;
    mock_rest(&rest).await;
    let websocket = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let rest_url = format!("{}/trade-api/v2", rest.uri());
    let ws_url = format!("ws://{}/trade-api/ws/v2", websocket.local_addr().unwrap());

    // A throwaway key, so the service signs its requests as it would for real.
    let key = RsaPrivateKey::new(&mut rand::thread_rng(), 1024).unwrap();
    let key_file = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(key_file.path(), key.to_pkcs8_pem(LineEnding::LF).unwrap().as_bytes()).unwrap();

    let stdout = tempfile::NamedTempFile::new().unwrap();
    let mut service = Service(
        Command::new(env!("CARGO_BIN_EXE_engineering-challenge"))
            .env_remove("KALSHI_ENV")
            .env_remove("KALSHI_SERIES_TICKER")
            .env("KALSHI_API_KEY_ID", "test-key-id")
            .env("KALSHI_PRIVATE_KEY_PATH", key_file.path())
            .env("KALSHI_REST_URL", rest_url)
            .env("KALSHI_WS_URL", ws_url)
            // The mock plays one connection's script.
            .env("KALSHI_CONNECTIONS", "1")
            .stdout(Stdio::from(stdout.reopen().unwrap()))
            .spawn()
            .unwrap(),
    );

    // The WebSocket handshake must carry a valid signature.
    let (stream, _) = websocket.accept().await.unwrap();
    let mut handshake = None;
    let mut ws = tokio_tungstenite::accept_hdr_async(stream, |request: &Request, response: Response| {
        handshake = Some((request.uri().path().to_owned(), request.headers().clone()));
        Ok(response)
    })
    .await
    .unwrap();
    let (path, headers) = handshake.unwrap();
    let header = |name: &str| headers.get(name).unwrap().to_str().unwrap().to_owned();
    assert_eq!(header("KALSHI-ACCESS-KEY"), "test-key-id");
    let signed = format!("{}GET{path}", header("KALSHI-ACCESS-TIMESTAMP"));
    let signature = base64::engine::general_purpose::STANDARD
        .decode(header("KALSHI-ACCESS-SIGNATURE"))
        .unwrap();
    VerifyingKey::<Sha256>::new(key.to_public_key())
        .verify(signed.as_bytes(), &Signature::try_from(signature.as_slice()).unwrap())
        .expect("the signature should cover timestamp + method + path");

    assert_eq!(next_command(&mut ws).await["params"]["channels"], json!(["market_lifecycle_v2"]));
    let command = next_command(&mut ws).await;
    assert_eq!(command["params"]["channels"], json!(["orderbook_delta"]));
    assert_eq!(command["params"]["market_tickers"], json!(["A"]));
    // The symbol list came from REST, and that request was signed as well.
    let fetched = rest.received_requests().await.unwrap();
    assert_eq!(fetched[0].headers["KALSHI-ACCESS-KEY"], "test-key-id");
    assert!(fetched[0].headers.contains_key("KALSHI-ACCESS-SIGNATURE"));

    send(&mut ws, json!({"type": "orderbook_snapshot", "sid": 2, "seq": 1, "sending_ts_ms": 1_700_000_000_000_i64,
        "msg": {"market_ticker": "A", "clock": 100,
            "yes_dollars_fp": [["0.4000", "20.00"]], "no_dollars_fp": [["0.5800", "10.00"]]}}))
    .await;
    send(&mut ws, json!({"type": "orderbook_delta", "sid": 2, "seq": 2,
        "msg": {"market_ticker": "A", "clock": 101, "price_dollars": "0.4000",
            "delta_fp": "-5.00", "side": "yes", "ts_ms": 1_700_000_000_500_i64}}))
    .await;
    // The unsubscribe that answers this proves the delta above was handled.
    send(&mut ws, json!({"type": "market_lifecycle_v2", "sid": 1, "seq": 1,
        "msg": {"market_ticker": "A", "event_type": "settled"}}))
    .await;
    assert_eq!(next_command(&mut ws).await["params"]["action"], "delete_markets");

    // Nothing is on stdout yet: the writer holds a chunk until it is full
    // or closed.
    assert_eq!(std::fs::metadata(stdout.path()).unwrap().len(), 0);

    let interrupted = Command::new("kill")
        .args(["-INT", &service.0.id().to_string()])
        .status()
        .unwrap();
    assert!(interrupted.success());

    let mut closed = false;
    while let Some(Ok(message)) = ws.next().await {
        closed |= message.is_close();
    }
    assert!(closed, "the websocket should be closed with a close frame");

    let status = loop {
        match service.0.try_wait().unwrap() {
            Some(status) => break status,
            None => tokio::time::sleep(Duration::from_millis(20)).await,
        }
    };
    assert!(status.success(), "the service should exit cleanly, got {status}");

    // One whole chunk, readable with the challenge's reader.
    assert_eq!(std::fs::metadata(stdout.path()).unwrap().len(), CHUNK);
    let mut reader = AlignedSbeReader::<OrderbookSchema>::open(stdout.path()).unwrap();
    let Some(AnyMessage::Snapshot(decoded)) = reader.read_msg().unwrap() else {
        panic!("expected a snapshot");
    };
    let snapshot = SnapshotDomain::try_from_decoder(decoded).unwrap();
    assert_eq!(snapshot.symbol, "A");
    assert_eq!(snapshot.exchange_timestamp.timestamp_millis(), 1_700_000_000_000);
    assert_eq!(snapshot.bids, [SnapshotBidsEntryDomain { price_level: 400_000, quantity: 20_000_000 }]);
    // A NO bid at 0.58 is a YES ask at 0.42.
    assert_eq!(snapshot.asks, [SnapshotAsksEntryDomain { price_level: 420_000, quantity: 10_000_000 }]);

    let Some(AnyMessage::PriceLevelUpdate(decoded)) = reader.read_msg().unwrap() else {
        panic!("expected a price level update");
    };
    let update = PriceLevelUpdateDomain::try_from_decoder(decoded).unwrap();
    assert_eq!(
        (update.symbol.as_str(), update.side, update.price_level, update.quantity),
        ("A", Side::Buy, 400_000, 15_000_000)
    );
    assert_eq!(update.exchange_timestamp.timestamp_millis(), 1_700_000_000_500);
    assert!(update.backtest_timestamp >= snapshot.received_at);
    assert!(reader.read_msg().unwrap().is_none());
}
