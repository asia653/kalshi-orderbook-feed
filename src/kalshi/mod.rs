//! [`Kalshi`] connects to Kalshi, keeps the list of trading symbols in sync,
//! and maintains an order book for each of them.
//!
//! * The symbol list is built from REST (`GET /markets?status=open`) and kept
//!   current by the `market_lifecycle_v2` WebSocket channel.
//! * Each symbol's book is built from the `orderbook_delta` channel, which
//!   sends a snapshot and then deltas.
//!
//! Both follow the same snapshot-plus-deltas rule, implemented once in
//! [`synced`]. Everything that happens is published as an [`Event`], and
//! every book is published again in full once per snapshot interval.

mod book;
mod symbols;
mod synced;

use std::collections::{BTreeSet, HashMap, HashSet};
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, bail};
use base64::Engine as _;
use chrono::{DateTime, Utc};
use crossbeam::channel::{Receiver, Sender, unbounded};
use futures_util::future::BoxFuture;
use futures_util::{SinkExt, StreamExt};
use rsa::RsaPrivateKey;
use rsa::pkcs1::DecodeRsaPrivateKey;
use rsa::pkcs8::DecodePrivateKey;
use rsa::pss::BlindedSigningKey;
use rsa::signature::{RandomizedSigner, SignatureEncoding};
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::Sha256;
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

pub use book::Level;
pub use symbols::SymbolChange;

use crate::Side;
use book::{Book, BookChange, BookDelta, BookSide, BookSnapshot, parse_fp, parse_price};
use symbols::{LifecycleEvent, OpenSet, SymbolList};
use synced::Synced;

const PROD_REST: &str = "https://external-api.kalshi.com/trade-api/v2";
const PROD_WS: &str = "wss://external-api-ws.kalshi.com/trade-api/ws/v2";
const DEMO_REST: &str = "https://external-api.demo.kalshi.co/trade-api/v2";
const DEMO_WS: &str = "wss://external-api-ws.demo.kalshi.co/trade-api/ws/v2";

const LIFECYCLE_CHANNEL: &str = "market_lifecycle_v2";
const BOOK_CHANNEL: &str = "orderbook_delta";

/// Symbols per subscribe / update command.
const COMMAND_BATCH: usize = 500;
const MAX_BACKOFF: Duration = Duration::from_secs(30);
/// The shortest time between two re-fetches of every book. Several sequence
/// gaps close together are answered with one re-fetch, not one each.
const REFETCH_COOLDOWN: Duration = Duration::from_secs(5);
/// How long a closing handshake may take before the socket is just dropped.
const CLOSE_TIMEOUT: Duration = Duration::from_secs(2);

type Ws = WebSocketStream<MaybeTlsStream<TcpStream>>;
type Fetch = BoxFuture<'static, anyhow::Result<OpenSet>>;

/// Something that happened to the symbol list or to a symbol's book.
#[derive(Debug, Clone, PartialEq)]
pub enum Event {
    /// A symbol started or stopped trading.
    Symbol(SymbolChange),
    /// A symbol's whole book, best price first on each side. Sent when a
    /// book is first received, whenever it had to be re-fetched, and for
    /// every book once per [`KalshiConfig::snapshot_interval`].
    Snapshot {
        symbol: String,
        bids: Vec<Level>,
        asks: Vec<Level>,
        received_at: DateTime<Utc>,
        exchange_timestamp: DateTime<Utc>,
    },
    /// One price level now holds `quantity` (zero means it is gone).
    Level {
        symbol: String,
        side: Side,
        price_level: i64,
        quantity: i64,
        received_at: DateTime<Utc>,
        exchange_timestamp: DateTime<Utc>,
    },
}

pub struct KalshiConfig {
    pub rest_url: String,
    pub ws_url: String,
    /// API key ID and RSA private key (PEM). The WebSocket requires them.
    pub credentials: Option<(String, String)>,
    /// Only follow markets in this series. `None` follows every open market.
    pub series_ticker: Option<String>,
    /// How often the symbol list is re-checked against REST.
    pub reconcile_interval: Duration,
    /// How often every book is published in full.
    pub snapshot_interval: Duration,
}

impl KalshiConfig {
    pub fn demo() -> Self {
        Self::with_urls(DEMO_REST, DEMO_WS)
    }

    pub fn prod() -> Self {
        Self::with_urls(PROD_REST, PROD_WS)
    }

    fn with_urls(rest_url: &str, ws_url: &str) -> Self {
        Self {
            rest_url: rest_url.to_owned(),
            ws_url: ws_url.to_owned(),
            credentials: None,
            series_ticker: None,
            reconcile_interval: Duration::from_secs(300),
            snapshot_interval: Duration::from_secs(60),
        }
    }

    /// Reads `KALSHI_ENV` (`demo` by default, or `prod`), `KALSHI_API_KEY_ID`,
    /// `KALSHI_PRIVATE_KEY_PATH` and optionally `KALSHI_SERIES_TICKER`.
    /// `KALSHI_REST_URL` and `KALSHI_WS_URL` override the environment's URLs.
    pub fn from_env() -> anyhow::Result<Self> {
        let mut config = match std::env::var("KALSHI_ENV").as_deref() {
            Ok("prod") => Self::prod(),
            Ok("demo") | Err(_) => Self::demo(),
            Ok(other) => bail!("KALSHI_ENV must be `demo` or `prod`, got `{other}`"),
        };
        let key_id = std::env::var("KALSHI_API_KEY_ID").context("KALSHI_API_KEY_ID not set")?;
        let key_path =
            std::env::var("KALSHI_PRIVATE_KEY_PATH").context("KALSHI_PRIVATE_KEY_PATH not set")?;
        let pem = std::fs::read_to_string(&key_path)
            .with_context(|| format!("reading private key at {key_path}"))?;
        config.credentials = Some((key_id, pem));
        config.series_ticker = std::env::var("KALSHI_SERIES_TICKER").ok();
        if let Ok(url) = std::env::var("KALSHI_REST_URL") {
            config.rest_url = url;
        }
        if let Ok(url) = std::env::var("KALSHI_WS_URL") {
            config.ws_url = url;
        }
        Ok(config)
    }
}

/// Connects to Kalshi and owns the symbol list and every symbol's book.
pub struct Kalshi {
    config: KalshiConfig,
    auth: Option<Arc<Auth>>,
    http: reqwest::Client,
    events: Sender<Event>,

    symbols: Synced<SymbolList>,
    books: HashMap<String, Synced<Book>>,
    /// When every book is next published in full.
    next_snapshot: tokio::time::Instant,

    // State of the current WebSocket connection; reset on every reconnect.
    /// Symbols Kalshi has been asked to send books for.
    subscribed: HashSet<String>,
    /// Subscription id of the orderbook channel, once Kalshi has told us.
    book_sid: Option<u64>,
    /// Book commands that need `book_sid` and are waiting for it.
    deferred: Vec<(&'static str, Vec<String>)>,
    /// Last sequence number seen on each subscription.
    last_seq: HashMap<u64, u64>,
    /// When the books a sequence gap invalidated will be asked for again.
    refetch_at: Option<tokio::time::Instant>,
    /// When that was last done.
    last_refetch: Option<tokio::time::Instant>,
    /// Message types we do not handle and have already logged once.
    unhandled: HashSet<String>,
    /// A lifecycle message was missed, so the list must be re-fetched.
    symbols_stale: bool,
    next_command_id: u64,
}

impl Kalshi {
    /// Returns the object and the receiving end of its event channel.
    pub fn new(config: KalshiConfig) -> anyhow::Result<(Self, Receiver<Event>)> {
        let auth = match &config.credentials {
            Some((key_id, pem)) => {
                let key = RsaPrivateKey::from_pkcs8_pem(pem)
                    .or_else(|_| RsaPrivateKey::from_pkcs1_pem(pem))
                    .context("private key is not a valid RSA PEM")?;
                Some(Arc::new(Auth {
                    key_id: key_id.clone(),
                    key: BlindedSigningKey::<Sha256>::new(key),
                }))
            }
            None => None,
        };
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()?;
        let (events, events_rx) = unbounded();
        let symbols = Synced::new(SymbolList::new(config.series_ticker.clone()));
        let next_snapshot = tokio::time::Instant::now() + config.snapshot_interval;
        let kalshi = Self {
            config,
            auth,
            http,
            events,
            symbols,
            books: HashMap::new(),
            next_snapshot,
            subscribed: HashSet::new(),
            book_sid: None,
            deferred: Vec::new(),
            last_seq: HashMap::new(),
            refetch_at: None,
            last_refetch: None,
            unhandled: HashSet::new(),
            symbols_stale: false,
            next_command_id: 0,
        };
        Ok((kalshi, events_rx))
    }

    /// The current symbol list, sorted.
    pub fn symbols(&self) -> Vec<String> {
        self.symbols.state().symbols()
    }

    /// Connects and stays in sync until `shutdown` completes, then closes the
    /// WebSocket and returns. Reconnects with backoff on any error.
    pub async fn run(&mut self, shutdown: impl Future<Output = ()>) {
        tokio::pin!(shutdown);
        let mut backoff = Duration::from_secs(1);
        loop {
            let started = Instant::now();
            let connected = tokio::select! {
                _ = &mut shutdown => return,
                connected = self.connect() => connected,
            };
            let error = match connected {
                Ok(mut ws) => tokio::select! {
                    _ = &mut shutdown => return close(ws).await,
                    Err(error) = self.session(&mut ws) => error,
                },
                Err(error) => error,
            };
            eprintln!("kalshi: session ended: {error:#}");
            if started.elapsed() > Duration::from_secs(60) {
                backoff = Duration::from_secs(1);
            }
            let jitter = Duration::from_millis(rand::random::<u64>() % 1000);
            tokio::select! {
                _ = &mut shutdown => return,
                _ = tokio::time::sleep(backoff + jitter) => {}
            }
            backoff = (backoff * 2).min(MAX_BACKOFF);
        }
    }

    // ---- connection -------------------------------------------------------

    /// One WebSocket connection's lifetime. Only ever returns an error.
    async fn session(&mut self, ws: &mut Ws) -> Result<std::convert::Infallible, anyhow::Error> {
        // A new connection has no subscriptions and nothing is in sync.
        self.subscribed.clear();
        self.book_sid = None;
        self.deferred.clear();
        self.last_seq.clear();
        self.refetch_at = None;
        for book in self.books.values_mut() {
            *book = Synced::new(Book::default());
        }

        // Listen before fetching: lifecycle events that arrive while REST is
        // paging are held by `self.symbols` and replayed after the load.
        self.command(ws, "subscribe", json!({ "channels": [LIFECYCLE_CHANNEL] }))
            .await?;
        let existing = self.books.keys().cloned().collect();
        self.subscribe_books(ws, existing).await?;
        let mut fetch = Some(self.start_fetch());

        let mut reconcile = tokio::time::interval(self.config.reconcile_interval);
        reconcile.tick().await; // the first tick fires immediately
        loop {
            let now = Utc::now().timestamp();
            let until_close = self.symbols.state().until_next_close(now);
            let refetch_at = self.refetch_at;
            tokio::select! {
                message = ws.next() => {
                    let message = message.context("websocket closed")??;
                    self.on_message(ws, message).await?;
                }
                open = async { fetch.as_mut().expect("guarded").await }, if fetch.is_some() => {
                    fetch = None;
                    let mut changes = Vec::new();
                    self.symbols.on_snapshot(open?, &mut changes)?;
                    self.on_symbol_changes(ws, changes).await?;
                }
                _ = tokio::time::sleep(until_close) => {
                    let mut changes = Vec::new();
                    self.symbols.state_mut().expire(Utc::now().timestamp(), &mut changes);
                    self.on_symbol_changes(ws, changes).await?;
                }
                _ = reconcile.tick() => self.symbols_stale = true,
                _ = tokio::time::sleep_until(self.next_snapshot) => self.publish_snapshots(),
                _ = async { tokio::time::sleep_until(refetch_at.expect("guarded")).await },
                    if refetch_at.is_some() =>
                {
                    let stale = self.take_stale_books();
                    self.update_books(ws, "get_snapshot", stale).await?;
                }
            }
            if self.symbols_stale && fetch.is_none() {
                fetch = Some(self.start_fetch());
            }
        }
    }

    async fn connect(&self) -> anyhow::Result<Ws> {
        let mut request = self.config.ws_url.as_str().into_client_request()?;
        let path = request.uri().path().to_owned();
        for (name, value) in auth_headers(&self.auth, "GET", &path)? {
            request.headers_mut().insert(name, value.parse()?);
        }
        let (ws, _) = tokio_tungstenite::connect_async(request)
            .await
            .context("websocket connect")?;
        Ok(ws)
    }

    /// Starts fetching the open set from REST. Until it is loaded the symbol
    /// list holds incoming lifecycle events instead of applying them.
    fn start_fetch(&mut self) -> Fetch {
        self.symbols.invalidate();
        self.symbols_stale = false;
        Box::pin(fetch_open_markets(
            self.http.clone(),
            self.auth.clone(),
            format!("{}/markets", self.config.rest_url),
            self.config.series_ticker.clone(),
        ))
    }

    async fn command(&mut self, ws: &mut Ws, cmd: &str, params: Value) -> anyhow::Result<()> {
        self.next_command_id += 1;
        let command = json!({ "id": self.next_command_id, "cmd": cmd, "params": params });
        ws.send(Message::Text(command.to_string().into())).await?;
        Ok(())
    }

    /// Asks Kalshi for the books of `symbols`. A repeated subscribe adds
    /// markets to the connection's one orderbook subscription, and each new
    /// market gets a snapshot.
    async fn subscribe_books(&mut self, ws: &mut Ws, symbols: BTreeSet<String>) -> anyhow::Result<()> {
        let symbols: Vec<String> = symbols.into_iter().collect();
        for batch in symbols.chunks(COMMAND_BATCH) {
            let params = json!({
                "channels": [BOOK_CHANNEL],
                "market_tickers": batch,
                // Keep NO levels in NO prices; `Book` converts them to asks.
                "use_yes_price": false,
            });
            self.command(ws, "subscribe", params).await?;
        }
        self.subscribed.extend(symbols);
        Ok(())
    }

    /// Sends `delete_markets` or `get_snapshot` for `symbols`, or defers it
    /// until the orderbook subscription id is known.
    async fn update_books(
        &mut self,
        ws: &mut Ws,
        action: &'static str,
        symbols: Vec<String>,
    ) -> anyhow::Result<()> {
        if symbols.is_empty() {
            return Ok(());
        }
        let Some(sid) = self.book_sid else {
            self.deferred.push((action, symbols));
            return Ok(());
        };
        for batch in symbols.chunks(COMMAND_BATCH) {
            let params = json!({ "sids": [sid], "market_tickers": batch, "action": action });
            self.command(ws, "update_subscription", params).await?;
        }
        Ok(())
    }

    // ---- reacting ---------------------------------------------------------

    async fn on_message(&mut self, ws: &mut Ws, message: Message) -> anyhow::Result<()> {
        let received_at = Utc::now();
        let Some(frame) = parse_message(message)? else {
            return Ok(());
        };
        if let (true, Some(sid)) = (frame.body.is_book(), frame.sid) {
            self.learn_book_sid(ws, sid).await?;
        }
        // Every message on a subscription is numbered, whatever its type, so
        // every one is checked here before its type is looked at.
        if let (Some(sid), Some(seq)) = (frame.sid, frame.seq) {
            let last = self.last_seq.get(&sid).copied();
            if let (true, Some(last)) = (self.is_gap(sid, seq), last) {
                eprintln!(
                    "kalshi: sequence gap on subscription {sid}: expected {}, got {seq} ({})",
                    last + 1,
                    frame.body.kind(),
                );
                if self.book_sid == Some(sid) {
                    self.on_book_gap();
                } else {
                    self.symbols_stale = true;
                }
            }
        }
        let exchange_timestamp = frame
            .timestamp_ms
            .and_then(DateTime::from_timestamp_millis)
            .unwrap_or(received_at);

        match frame.body {
            Body::Lifecycle(event) => {
                let mut changes = Vec::new();
                self.symbols.on_delta(event, &mut changes)?;
                self.on_symbol_changes(ws, changes).await?;
            }
            Body::Subscribed { channel, sid } => {
                eprintln!("kalshi: subscribed to {channel} as subscription {sid}");
                if channel == BOOK_CHANNEL {
                    self.learn_book_sid(ws, sid).await?;
                }
            }
            // Their sequence numbers were counted above; nothing else to do.
            Body::Ok | Body::EventLifecycle => {}
            Body::Other { kind, raw } => {
                if self.unhandled.insert(kind.clone()) {
                    eprintln!("kalshi: not handling `{kind}` messages; the first one: {raw}");
                }
            }
            Body::BookSnapshot { symbol, snapshot } => {
                if let Some(book) = self.books.get_mut(&symbol) {
                    let mut changes = Vec::new();
                    book.on_snapshot(snapshot, &mut changes)?;
                    self.publish_book(&symbol, changes, received_at, exchange_timestamp);
                }
            }
            Body::BookDelta { symbol, delta } => {
                // A late delta for a symbol we have dropped is not an error.
                if let Some(book) = self.books.get_mut(&symbol) {
                    let mut changes = Vec::new();
                    let applied = book.on_delta(delta, &mut changes);
                    self.publish_book(&symbol, changes, received_at, exchange_timestamp);
                    if let Err(error) = applied {
                        eprintln!("kalshi: {symbol}: {error:#}; requesting a snapshot");
                        self.update_books(ws, "get_snapshot", vec![symbol]).await?;
                    }
                }
            }
        }
        Ok(())
    }

    /// True if a message was missed on subscription `sid`.
    fn is_gap(&mut self, sid: u64, seq: u64) -> bool {
        match self.last_seq.insert(sid, seq) {
            Some(last) => seq != last + 1,
            None => false,
        }
    }

    /// A message was missed on the orderbook subscription. Its sequence is
    /// shared by all symbols, so no book can be trusted: every one goes back
    /// to waiting for a snapshot, and a re-fetch is scheduled. The re-fetch
    /// is immediate unless one was sent within [`REFETCH_COOLDOWN`].
    fn on_book_gap(&mut self) {
        for book in self.books.values_mut() {
            book.invalidate();
        }
        let now = tokio::time::Instant::now();
        let earliest = self.last_refetch.map_or(now, |last| last + REFETCH_COOLDOWN);
        self.refetch_at.get_or_insert(earliest.max(now));
    }

    /// The symbols whose books need a new snapshot, sorted. A book that
    /// loaded a snapshot after the last gap is already correct and is left
    /// alone.
    fn take_stale_books(&mut self) -> Vec<String> {
        self.refetch_at = None;
        self.last_refetch = Some(tokio::time::Instant::now());
        let mut stale: Vec<String> = self
            .books
            .iter()
            .filter(|(_, book)| !book.is_live())
            .map(|(symbol, _)| symbol.clone())
            .collect();
        stale.sort();
        eprintln!("kalshi: re-fetching {} books after a sequence gap", stale.len());
        stale
    }

    async fn learn_book_sid(&mut self, ws: &mut Ws, sid: u64) -> anyhow::Result<()> {
        if self.book_sid.is_none() {
            self.book_sid = Some(sid);
            for (action, symbols) in std::mem::take(&mut self.deferred) {
                self.update_books(ws, action, symbols).await?;
            }
        }
        Ok(())
    }

    /// Creates and drops books to match the symbol list, tells Kalshi, and
    /// publishes the changes.
    async fn on_symbol_changes(
        &mut self,
        ws: &mut Ws,
        changes: Vec<SymbolChange>,
    ) -> anyhow::Result<()> {
        let mut subscribe = BTreeSet::new();
        let mut unsubscribe = BTreeSet::new();
        let mut refresh = BTreeSet::new();
        for change in changes {
            match &change {
                SymbolChange::Added(symbol) => {
                    self.books.insert(symbol.clone(), Synced::new(Book::default()));
                    if self.subscribed.contains(symbol) {
                        // Removed and re-added before we unsubscribed: Kalshi
                        // will not resend a snapshot unless asked.
                        unsubscribe.remove(symbol);
                        refresh.insert(symbol.clone());
                    } else {
                        subscribe.insert(symbol.clone());
                    }
                }
                SymbolChange::Removed(symbol) => {
                    self.books.remove(symbol);
                    refresh.remove(symbol);
                    if !subscribe.remove(symbol) && self.subscribed.contains(symbol) {
                        unsubscribe.insert(symbol.clone());
                    }
                }
            }
            self.publish(Event::Symbol(change));
        }
        self.subscribe_books(ws, subscribe).await?;
        for symbol in &unsubscribe {
            self.subscribed.remove(symbol);
        }
        self.update_books(ws, "delete_markets", unsubscribe.into_iter().collect())
            .await?;
        self.update_books(ws, "get_snapshot", refresh.into_iter().collect())
            .await
    }

    fn publish_book(
        &self,
        symbol: &str,
        changes: Vec<BookChange>,
        received_at: DateTime<Utc>,
        exchange_timestamp: DateTime<Utc>,
    ) {
        let Some(book) = self.books.get(symbol) else {
            return;
        };
        for change in changes {
            self.publish(match change {
                BookChange::Replaced => Event::Snapshot {
                    symbol: symbol.to_owned(),
                    bids: book.state().bids(),
                    asks: book.state().asks(),
                    received_at,
                    exchange_timestamp,
                },
                BookChange::Level { side, price_level, quantity } => Event::Level {
                    symbol: symbol.to_owned(),
                    side,
                    price_level,
                    quantity,
                    received_at,
                    exchange_timestamp,
                },
            });
        }
    }

    /// Publishes every book that is in sync, in full, in symbol order. A
    /// book still waiting for its snapshot is left out: there is nothing
    /// trustworthy to say about it yet.
    fn publish_snapshots(&mut self) {
        let received_at = Utc::now();
        let mut live: Vec<(&String, &Synced<Book>)> =
            self.books.iter().filter(|(_, book)| book.is_live()).collect();
        live.sort_by_key(|(symbol, _)| *symbol);
        for (symbol, book) in live {
            let book = book.state();
            self.publish(Event::Snapshot {
                symbol: symbol.clone(),
                bids: book.bids(),
                asks: book.asks(),
                received_at,
                // The time of the last exchange message the book reflects.
                exchange_timestamp: book
                    .exchange_ms()
                    .and_then(DateTime::from_timestamp_millis)
                    .unwrap_or(received_at),
            });
        }
        // Keep a steady cadence, unless we have fallen a whole interval behind.
        let now = tokio::time::Instant::now();
        let next = self.next_snapshot + self.config.snapshot_interval;
        self.next_snapshot = if next > now { next } else { now + self.config.snapshot_interval };
    }

    fn publish(&self, event: Event) {
        // A dropped receiver just means nobody is listening.
        let _ = self.events.send(event);
    }
}

/// Closes the WebSocket politely: sends a close frame and waits, briefly,
/// for Kalshi to acknowledge it.
async fn close(mut ws: Ws) {
    let handshake = async {
        if ws.close(None).await.is_ok() {
            // Anything still in flight is read and discarded until Kalshi's
            // own close frame (or the end of the stream) arrives.
            while let Some(Ok(message)) = ws.next().await {
                if message.is_close() {
                    break;
                }
            }
        }
    };
    if tokio::time::timeout(CLOSE_TIMEOUT, handshake).await.is_err() {
        eprintln!("kalshi: no close acknowledgement; dropping the connection");
    }
}

// ---- wire formats ---------------------------------------------------------

/// One parsed WebSocket message.
struct Frame {
    sid: Option<u64>,
    seq: Option<u64>,
    /// Kalshi's time for the message, Unix milliseconds.
    timestamp_ms: Option<i64>,
    body: Body,
}

enum Body {
    Lifecycle(LifecycleEvent),
    Subscribed { channel: String, sid: u64 },
    BookSnapshot { symbol: String, snapshot: BookSnapshot },
    BookDelta { symbol: String, delta: BookDelta },
    /// The acknowledgement of a command on an existing subscription.
    Ok,
    /// A new event (a group of markets) was announced on the lifecycle
    /// subscription. Its markets get lifecycle messages of their own, so
    /// this needs no action.
    EventLifecycle,
    /// A message type we do not act on; `raw` is its (shortened) text.
    Other { kind: String, raw: String },
}

impl Body {
    fn kind(&self) -> &str {
        match self {
            Body::Lifecycle(_) => LIFECYCLE_CHANNEL,
            Body::Subscribed { .. } => "subscribed",
            Body::BookSnapshot { .. } => "orderbook_snapshot",
            Body::BookDelta { .. } => "orderbook_delta",
            Body::Ok => "ok",
            Body::EventLifecycle => "event_lifecycle",
            Body::Other { kind, .. } => kind,
        }
    }

    fn is_book(&self) -> bool {
        matches!(self, Body::BookSnapshot { .. } | Body::BookDelta { .. })
    }
}

#[derive(Deserialize)]
struct Envelope {
    #[serde(rename = "type")]
    kind: String,
    sid: Option<u64>,
    seq: Option<u64>,
    sending_ts_ms: Option<i64>,
    msg: Option<Value>,
}

#[derive(Deserialize)]
struct SubscribedMsg {
    channel: String,
    sid: u64,
}

#[derive(Deserialize)]
struct SnapshotMsg {
    market_ticker: String,
    clock: i64,
    #[serde(default)]
    yes_dollars_fp: Vec<(String, String)>,
    #[serde(default)]
    no_dollars_fp: Vec<(String, String)>,
}

#[derive(Deserialize)]
struct DeltaMsg {
    market_ticker: String,
    clock: i64,
    price_dollars: String,
    delta_fp: String,
    side: String,
    ts_ms: Option<i64>,
}

/// Returns `None` for WebSocket frames that carry no message (ping, pong).
fn parse_message(message: Message) -> anyhow::Result<Option<Frame>> {
    let text = match message {
        Message::Text(text) => text,
        Message::Close(frame) => bail!("server closed the websocket: {frame:?}"),
        _ => return Ok(None), // ping/pong are answered by the library
    };
    let envelope: Envelope = serde_json::from_str(text.as_str())?;
    let mut timestamp_ms = envelope.sending_ts_ms;
    let body = match (envelope.kind.as_str(), envelope.msg) {
        (LIFECYCLE_CHANNEL, Some(msg)) => Body::Lifecycle(serde_json::from_value(msg)?),
        ("subscribed", Some(msg)) => match serde_json::from_value::<SubscribedMsg>(msg) {
            Ok(msg) => Body::Subscribed { channel: msg.channel, sid: msg.sid },
            Err(_) => other(&envelope.kind, text.as_str()),
        },
        ("orderbook_snapshot", Some(msg)) => {
            let msg: SnapshotMsg = serde_json::from_value(msg)?;
            let levels = |side: Vec<(String, String)>| -> anyhow::Result<Vec<Level>> {
                side.iter()
                    .map(|(price, quantity)| Ok((parse_price(price)?, parse_fp(quantity)?)))
                    .collect()
            };
            Body::BookSnapshot {
                snapshot: BookSnapshot {
                    clock: msg.clock,
                    exchange_ms: timestamp_ms,
                    yes: levels(msg.yes_dollars_fp)?,
                    no: levels(msg.no_dollars_fp)?,
                },
                symbol: msg.market_ticker,
            }
        }
        ("orderbook_delta", Some(msg)) => {
            let msg: DeltaMsg = serde_json::from_value(msg)?;
            timestamp_ms = msg.ts_ms.or(timestamp_ms);
            let side = match msg.side.as_str() {
                "yes" => BookSide::Yes,
                "no" => BookSide::No,
                other => bail!("unknown orderbook side `{other}`"),
            };
            Body::BookDelta {
                delta: BookDelta {
                    clock: msg.clock,
                    exchange_ms: timestamp_ms,
                    side,
                    price: parse_price(&msg.price_dollars)?,
                    delta: parse_fp(&msg.delta_fp)?,
                },
                symbol: msg.market_ticker,
            }
        }
        ("ok", _) => Body::Ok,
        ("event_lifecycle", _) => Body::EventLifecycle,
        ("error", msg) => bail!("kalshi websocket error: {msg:?}"),
        _ => other(&envelope.kind, text.as_str()),
    };
    Ok(Some(Frame {
        sid: envelope.sid,
        seq: envelope.seq,
        timestamp_ms,
        body,
    }))
}

fn other(kind: &str, text: &str) -> Body {
    Body::Other {
        kind: kind.to_owned(),
        raw: text.chars().take(300).collect(),
    }
}

#[derive(Deserialize)]
struct MarketsPage {
    #[serde(default)]
    markets: Vec<RestMarket>,
    cursor: Option<String>,
}

#[derive(Deserialize)]
struct RestMarket {
    ticker: String,
    close_time: Option<DateTime<Utc>>,
}

/// Pages `GET /markets?status=open`, leaving out multivariate (MVE) markets.
async fn fetch_open_markets(
    http: reqwest::Client,
    auth: Option<Arc<Auth>>,
    url: String,
    series: Option<String>,
) -> anyhow::Result<OpenSet> {
    let url = reqwest::Url::parse(&url)?;
    let fetched_at = Utc::now().timestamp();
    let mut markets = HashMap::new();
    let mut cursor = String::new();
    loop {
        let mut request = http.get(url.clone()).query(&[
            ("status", "open"),
            ("mve_filter", "exclude"),
            ("limit", "1000"),
        ]);
        if let Some(series) = &series {
            request = request.query(&[("series_ticker", series)]);
        }
        if !cursor.is_empty() {
            request = request.query(&[("cursor", &cursor)]);
        }
        for (name, value) in auth_headers(&auth, "GET", url.path())? {
            request = request.header(name, value);
        }
        let page: MarketsPage = request
            .send()
            .await?
            .error_for_status()?
            .json()
            .await
            .context("decoding /markets response")?;
        for market in page.markets {
            markets.insert(market.ticker, market.close_time.map(|t| t.timestamp()));
        }
        match page.cursor {
            Some(next) if !next.is_empty() => cursor = next,
            _ => return Ok(OpenSet { fetched_at, markets }),
        }
    }
}

struct Auth {
    key_id: String,
    key: BlindedSigningKey<Sha256>,
}

/// Kalshi signs `timestamp_ms + method + path` (no query string) with
/// RSA-PSS/SHA-256. Returns no headers when there are no credentials.
fn auth_headers(
    auth: &Option<Arc<Auth>>,
    method: &str,
    path: &str,
) -> anyhow::Result<Vec<(&'static str, String)>> {
    let Some(auth) = auth else {
        return Ok(Vec::new());
    };
    let timestamp = Utc::now().timestamp_millis().to_string();
    let message = format!("{timestamp}{method}{path}");
    let signature = auth
        .key
        .try_sign_with_rng(&mut rand::thread_rng(), message.as_bytes())
        .context("signing request")?;
    let signature = base64::engine::general_purpose::STANDARD.encode(signature.to_bytes());
    Ok(vec![
        ("KALSHI-ACCESS-KEY", auth.key_id.clone()),
        ("KALSHI-ACCESS-SIGNATURE", signature),
        ("KALSHI-ACCESS-TIMESTAMP", timestamp),
    ])
}

#[cfg(test)]
mod tests;
