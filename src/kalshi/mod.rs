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
//!
//! [`Kalshi`] owns the symbol list and the REST fetch. Each WebSocket, with
//! the books built from it, is a [`connection::Connection`].

mod book;
mod connection;
mod symbols;
mod synced;
mod wire;

use std::collections::HashSet;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, bail};
use base64::Engine as _;
use chrono::{DateTime, Utc};
use crossbeam::channel::{Receiver, Sender, unbounded};
use futures_util::future::BoxFuture;
use reqwest::StatusCode;
use rsa::RsaPrivateKey;
use rsa::pkcs1::DecodeRsaPrivateKey;
use rsa::pkcs8::DecodePrivateKey;
use rsa::pss::BlindedSigningKey;
use rsa::signature::{RandomizedSigner, SignatureEncoding};
use serde::Deserialize;
use sha2::Sha256;

pub use book::Level;
pub use symbols::SymbolChange;

use crate::Side;
use connection::{Connection, Heard};
use symbols::{OpenSet, SymbolList};
use synced::Synced;

const PROD_REST: &str = "https://external-api.kalshi.com/trade-api/v2";
const PROD_WS: &str = "wss://external-api-ws.kalshi.com/trade-api/ws/v2";
const DEMO_REST: &str = "https://external-api.demo.kalshi.co/trade-api/v2";
const DEMO_WS: &str = "wss://external-api-ws.demo.kalshi.co/trade-api/ws/v2";

/// The longest pause between two attempts at a failing REST fetch.
const MAX_FETCH_PAUSE: Duration = Duration::from_secs(30);
/// The longest pause before asking again for a rate-limited REST page.
const MAX_RATE_LIMIT_PAUSE: Duration = Duration::from_secs(5);

type Fetch = BoxFuture<'static, OpenSet>;

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
    /// `GET` this for the open markets.
    markets_url: reqwest::Url,
    events: Sender<Event>,

    symbols: Synced<SymbolList>,
    /// The symbol list may have missed a change and must be re-fetched.
    symbols_stale: bool,
    /// The REST fetch in progress, if any. Only one runs at a time.
    fetch: Option<Fetch>,
    /// When every book is next published in full.
    next_snapshot: tokio::time::Instant,

    /// For now exactly one, which supplies both the lifecycle messages and
    /// the books that are published.
    connections: Vec<Connection>,
    /// Message types we do not handle and have already logged once.
    unhandled: HashSet<String>,
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
        let markets_url = reqwest::Url::parse(&format!("{}/markets", config.rest_url))
            .context("the REST URL is not a valid URL")?;
        let (events, events_rx) = unbounded();
        let symbols = Synced::new(SymbolList::new(config.series_ticker.clone()));
        let next_snapshot = tokio::time::Instant::now() + config.snapshot_interval;
        let connections = vec![Connection::new(config.ws_url.clone(), auth.clone())];
        let kalshi = Self {
            config,
            auth,
            http,
            markets_url,
            events,
            symbols,
            symbols_stale: false,
            fetch: None,
            next_snapshot,
            connections,
            unhandled: HashSet::new(),
        };
        Ok((kalshi, events_rx))
    }

    /// The current symbol list, sorted.
    pub fn symbols(&self) -> Vec<String> {
        self.symbols.state().symbols()
    }

    /// Connects and stays in sync until `shutdown` completes, then closes the
    /// WebSocket and returns. A failed connection is reopened after a pause.
    pub async fn run(&mut self, shutdown: impl Future<Output = ()>) {
        tokio::pin!(shutdown);
        let mut reconcile = tokio::time::interval(self.config.reconcile_interval);
        reconcile.tick().await; // the first tick fires immediately
        loop {
            let until_close = self.symbols.state().until_next_close(Utc::now().timestamp());
            let next_snapshot = self.next_snapshot;
            let fetching = self.fetch.is_some();
            let (connections, fetch) = (&mut self.connections, &mut self.fetch);
            tokio::select! {
                _ = &mut shutdown => break,
                (index, heard) = next_heard(connections) => self.on_heard(index, heard),
                open = async { fetch.as_mut().expect("guarded").await }, if fetching => {
                    self.fetch = None;
                    let mut changes = Vec::new();
                    match self.symbols.on_snapshot(open, &mut changes) {
                        Ok(()) => self.on_symbol_changes(changes),
                        Err(error) => self.distrust_symbols(error),
                    }
                }
                _ = tokio::time::sleep(until_close) => {
                    let mut changes = Vec::new();
                    self.symbols.state_mut().expire(Utc::now().timestamp(), &mut changes);
                    self.on_symbol_changes(changes);
                }
                _ = reconcile.tick() => self.symbols_stale = true,
                _ = tokio::time::sleep_until(next_snapshot) => self.publish_snapshots(),
            }
            // Listen before fetching: lifecycle messages that arrive while
            // REST is paging are held by `self.symbols` and replayed after
            // the load. So a fetch only starts while someone is listening.
            if self.symbols_stale && self.fetch.is_none() && self.connections[0].is_open() {
                self.fetch = Some(self.start_fetch());
            }
        }
        for connection in &mut self.connections {
            connection.close().await;
        }
    }

    // ---- reacting ---------------------------------------------------------

    fn on_heard(&mut self, index: usize, heard: Heard) {
        match heard {
            Heard::Opened => {
                // A new socket has no subscriptions. Follow every current
                // symbol, and re-fetch the list: changes made while nothing
                // was listening were missed.
                let following: Vec<SymbolChange> =
                    self.symbols().into_iter().map(SymbolChange::Added).collect();
                let connection = &mut self.connections[index];
                connection.subscribe_lifecycle();
                connection.on_symbol_changes(&following);
                self.symbols_stale = true;
            }
            Heard::Lost(error) => {
                eprintln!("kalshi: session ended: {error:#}");
                // Nothing is listening, so a fetch in progress could not be
                // trusted. A new one starts when the socket is open again.
                self.fetch = None;
                self.symbols_stale = true;
            }
            Heard::Missed => self.symbols_stale = true,
            Heard::Lifecycle(event) => {
                let mut changes = Vec::new();
                match self.symbols.on_delta(event, &mut changes) {
                    Ok(()) => self.on_symbol_changes(changes),
                    Err(error) => self.distrust_symbols(error),
                }
            }
            Heard::Book(event) => self.publish(event),
            Heard::Unhandled { kind, raw } => {
                if self.unhandled.insert(kind.clone()) {
                    eprintln!("kalshi: not handling `{kind}` messages; the first one: {raw}");
                }
            }
        }
    }

    /// Passes changes to the symbol list on to every connection, and
    /// publishes them.
    fn on_symbol_changes(&mut self, changes: Vec<SymbolChange>) {
        for connection in &mut self.connections {
            connection.on_symbol_changes(&changes);
        }
        for change in changes {
            self.publish(Event::Symbol(change));
        }
    }

    fn distrust_symbols(&mut self, error: anyhow::Error) {
        eprintln!("kalshi: the symbol list is out of sync: {error:#}; fetching it again");
        self.symbols_stale = true;
    }

    /// Starts fetching the open set from REST. Until it is loaded the symbol
    /// list holds incoming lifecycle events instead of applying them.
    fn start_fetch(&mut self) -> Fetch {
        self.symbols.invalidate();
        self.symbols_stale = false;
        Box::pin(fetch_open_markets(
            self.http.clone(),
            self.auth.clone(),
            self.markets_url.clone(),
            self.config.series_ticker.clone(),
        ))
    }

    /// Publishes every book that is in sync, in full, in symbol order. A
    /// book still waiting for its snapshot is left out: there is nothing
    /// trustworthy to say about it yet.
    fn publish_snapshots(&mut self) {
        let received_at = Utc::now();
        let mut live: Vec<(&String, &Synced<book::Book>)> = self.connections[0]
            .books
            .iter()
            .filter(|(_, book)| book.is_live())
            .collect();
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

/// Waits for whichever connection has something to report first.
async fn next_heard(connections: &mut [Connection]) -> (usize, Heard) {
    let waiting = connections.iter_mut().map(|connection| Box::pin(connection.next()));
    let (heard, index, _) = futures_util::future::select_all(waiting).await;
    (index, heard)
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

/// Fetches the open markets, starting over after a pause for as long as it
/// keeps failing.
async fn fetch_open_markets(
    http: reqwest::Client,
    auth: Option<Arc<Auth>>,
    url: reqwest::Url,
    series: Option<String>,
) -> OpenSet {
    let mut pause = Duration::from_secs(1);
    loop {
        match page_open_markets(&http, &auth, &url, series.as_deref()).await {
            Ok(open) => return open,
            Err(error) => {
                eprintln!("kalshi: fetching the open markets failed: {error:#}; trying again in {pause:?}");
                tokio::time::sleep(pause).await;
                pause = (pause * 2).min(MAX_FETCH_PAUSE);
            }
        }
    }
}

/// Pages `GET /markets?status=open`, leaving out multivariate (MVE) markets.
async fn page_open_markets(
    http: &reqwest::Client,
    auth: &Option<Arc<Auth>>,
    url: &reqwest::Url,
    series: Option<&str>,
) -> anyhow::Result<OpenSet> {
    let fetched_at = Utc::now().timestamp();
    let mut markets = std::collections::HashMap::new();
    let mut cursor = String::new();
    loop {
        // Kalshi answers 429 when requests come too fast. There is no
        // penalty: the same page is simply asked for again a little later.
        let mut pause = Duration::from_millis(250);
        let response = loop {
            let mut request = http.get(url.clone()).query(&[
                ("status", "open"),
                ("mve_filter", "exclude"),
                ("limit", "1000"),
            ]);
            if let Some(series) = series {
                request = request.query(&[("series_ticker", series)]);
            }
            if !cursor.is_empty() {
                request = request.query(&[("cursor", &cursor)]);
            }
            for (name, value) in auth_headers(auth, "GET", url.path())? {
                request = request.header(name, value);
            }
            let response = request.send().await?;
            if response.status() != StatusCode::TOO_MANY_REQUESTS {
                break response;
            }
            eprintln!("kalshi: REST rate limit reached; asking again in {pause:?}");
            tokio::time::sleep(pause).await;
            pause = (pause * 2).min(MAX_RATE_LIMIT_PAUSE);
        };
        let page: MarketsPage = response
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
