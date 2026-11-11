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
//!
//! Several connections follow the same symbols so that one can fail without
//! losing the feed. One of them is the *source*: only its books are
//! published. When the source is lost or misses a message and another
//! connection is in sync, that one becomes the source, and every one of its
//! books is published in full before its changes continue.

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
use futures_util::{Stream, StreamExt};
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
    /// How many WebSockets follow each symbol. One is the source; the rest
    /// are ready to take over from it.
    pub connections: usize,
    /// How many groups the symbols are divided into. Each group has its own
    /// `connections` WebSockets, so the total is `groups * connections`.
    /// More groups means less traffic on each socket.
    pub groups: usize,
    pub health: Health,
    /// How often a status line goes to stderr. `None` for never.
    pub status_interval: Option<Duration>,
}

/// When a connection counts as unhealthy.
#[derive(Debug, Clone)]
pub struct Health {
    /// How often the connections are compared.
    pub check_every: Duration,
    /// A socket on which nothing arrives for this long, not even one of
    /// Kalshi's 10-second pings, is dead.
    pub dead_after: Duration,
    /// How long a connection waits for the snapshots it asked for before it
    /// asks for more books anyway.
    pub stalled_after: Duration,
    /// A connection that has heard nothing for this long, while another
    /// connection following the same symbols heard a delta, has a quiet
    /// subscription.
    pub quiet_after: Duration,
    /// With nothing to compare against: a connection is silent when it has
    /// heard nothing for `silence_factor` times its usual gap between
    /// deltas, and for at least `silence_floor`.
    pub silence_floor: Duration,
    pub silence_factor: u32,
    /// A connection is lagging when its messages are this much more delayed
    /// than those of the fastest connection following the same symbols, for
    /// `lag_checks` checks in a row. It is cut and connects again.
    pub lag_margin: Duration,
    pub lag_checks: u32,
    /// After a connection is cut for lag, how long before another of its
    /// group can be.
    pub lag_dwell: Duration,
}

impl Default for Health {
    fn default() -> Self {
        Self {
            check_every: Duration::from_secs(1),
            dead_after: Duration::from_secs(25),
            stalled_after: Duration::from_secs(5),
            quiet_after: Duration::from_secs(3),
            silence_floor: Duration::from_secs(30),
            silence_factor: 100,
            lag_margin: Duration::from_millis(500),
            lag_checks: 3,
            lag_dwell: Duration::from_secs(10),
        }
    }
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
            connections: 2,
            groups: 1,
            health: Health::default(),
            status_interval: None,
        }
    }

    /// Reads `KALSHI_ENV` (`demo` by default, or `prod`), `KALSHI_API_KEY_ID`
    /// and `KALSHI_PRIVATE_KEY_PATH`. Optional:
    ///
    /// * `KALSHI_SERIES_TICKER`: only follow this series.
    /// * `KALSHI_CONNECTIONS` (2) and `KALSHI_GROUPS` (1): see the fields.
    /// * `KALSHI_STATUS_SECONDS` (5): how often to print a status line; 0
    ///   for never.
    /// * `KALSHI_REST_URL` and `KALSHI_WS_URL`: override the environment's.
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
        let number = |name: &str| -> anyhow::Result<Option<u64>> {
            match std::env::var(name) {
                Ok(text) => Ok(Some(text.parse().with_context(|| format!("{name} must be a number"))?)),
                Err(_) => Ok(None),
            }
        };
        if let Some(connections) = number("KALSHI_CONNECTIONS")? {
            config.connections = connections as usize;
        }
        if let Some(groups) = number("KALSHI_GROUPS")? {
            config.groups = groups as usize;
        }
        config.status_interval = match number("KALSHI_STATUS_SECONDS")? {
            Some(0) => None,
            seconds => Some(Duration::from_secs(seconds.unwrap_or(5))),
        };
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

    /// Every connection, group after group: group `g` has the connections
    /// from `g * config.connections` up to the next group's first.
    connections: Vec<Connection>,
    /// For each group, the connection whose books are published.
    sources: Vec<usize>,
    /// For each group, when one of its connections was last cut for lag.
    lag_cut_at: Vec<Option<tokio::time::Instant>>,
    /// The connection whose lifecycle messages are applied. It only needs
    /// to be open, and can be in any group: lifecycle messages are the same
    /// on every socket.
    lead: usize,
    /// Which connection is asked first for news, so none is starved.
    turn: usize,
    /// Events published since the last status line.
    published: u64,
    /// Message types we do not handle and have already logged once.
    unhandled: HashSet<String>,
}

/// Which group follows `symbol`. All markets of one event land in the same
/// group, and a market always lands in the same group, so symbols coming
/// and going never move the others around.
fn group_for(symbol: &str, groups: usize) -> usize {
    // A market ticker is its event's ticker plus a last part: the event of
    // `KXBTCD-26NOV1017-T75749.99` is `KXBTCD-26NOV1017`.
    let event = symbol.rsplit_once('-').map_or(symbol, |(event, _)| event);
    // FNV-1a: small, and the same on every run and every machine.
    let hash = event.bytes().fold(0xcbf2_9ce4_8422_2325_u64, |hash, byte| {
        (hash ^ u64::from(byte)).wrapping_mul(0x0000_0100_0000_01b3)
    });
    (hash % groups as u64) as usize
}

/// What connection `index` is called in the log: a letter for its place in
/// its group, then the group's number when there is more than one group.
/// One group of two is A and B; two groups are A1, B1, A2 and B2.
fn connection_name(index: usize, config: &KalshiConfig) -> String {
    let (group, mut place) = (index / config.connections, index % config.connections);
    // Past Z the letters carry on as a spreadsheet's columns do: AA, AB, ...
    let mut name = String::new();
    loop {
        name.insert(0, char::from(b'A' + (place % 26) as u8));
        if place < 26 {
            break;
        }
        place = place / 26 - 1;
    }
    if config.groups > 1 {
        name += &(group + 1).to_string();
    }
    name
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

        anyhow::ensure!(config.connections >= 1, "at least one connection is needed");
        anyhow::ensure!(config.groups >= 1, "at least one group is needed");
        let mut connections: Vec<Connection> = (0..config.groups * config.connections)
            .map(|index| Connection::new(connection_name(index, &config), config.ws_url.clone(), auth.clone(), config.health.dead_after, config.health.stalled_after))
            .collect();
        // Each group starts with its first connection as the source.
        let sources: Vec<usize> = (0..config.groups).map(|group| group * config.connections).collect();
        for &source in &sources {
            connections[source].set_reporting(true);
        }
        let lag_cut_at = vec![None; config.groups];
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
            sources,
            lag_cut_at,
            lead: 0,
            turn: 0,
            published: 0,
            unhandled: HashSet::new(),
        };
        Ok((kalshi, events_rx))
    }

    /// The current symbol list, sorted.
    pub fn symbols(&self) -> Vec<String> {
        self.symbols.state().symbols()
    }

    /// Connects and stays in sync until `shutdown` completes, then closes the
    /// WebSockets and returns. A failed connection is reopened after a pause.
    pub async fn run(&mut self, shutdown: impl Future<Output = ()>) {
        self.run_with_faults(shutdown, futures_util::stream::pending()).await
    }

    /// Like [`Kalshi::run`], and every item of `drop_source` cuts a source's
    /// socket as a network failure would. For seeing a switch of source
    /// happen on demand.
    pub async fn run_with_faults(
        &mut self,
        shutdown: impl Future<Output = ()>,
        drop_source: impl Stream<Item = ()> + Unpin,
    ) {
        // Fused, so a stream that ends is simply never heard from again.
        let mut drop_source = drop_source.fuse();
        tokio::pin!(shutdown);
        let mut reconcile = tokio::time::interval(self.config.reconcile_interval);
        reconcile.tick().await; // the first tick fires immediately
        let mut health = tokio::time::interval(self.config.health.check_every);
        health.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let showing = self.config.status_interval.is_some();
        let mut status = tokio::time::interval(self.config.status_interval.unwrap_or(Duration::MAX));
        status.tick().await;
        loop {
            let until_close = self.symbols.state().until_next_close(Utc::now().timestamp());
            let next_snapshot = self.next_snapshot;
            let fetching = self.fetch.is_some();
            let (connections, fetch) = (&mut self.connections, &mut self.fetch);
            self.turn = self.turn.wrapping_add(1);
            tokio::select! {
                _ = &mut shutdown => break,
                (index, heard) = next_heard(connections, self.turn) => self.on_heard(index, heard),
                Some(()) = drop_source.next() => {
                    // The first group's source: one cut is enough to watch.
                    let source = self.sources[0];
                    eprintln!("kalshi: dropping connection {} on request", self.connections[source].name());
                    self.connections[source].drop_socket("dropped on request");
                }
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
                _ = health.tick() => self.check_health(),
                _ = status.tick(), if showing => self.print_status(),
            }
            // Listen before fetching: lifecycle messages that arrive while
            // REST is paging are held by `self.symbols` and replayed after
            // the load. So a fetch only starts while someone is listening.
            if self.symbols_stale && self.fetch.is_none() && self.connections[self.lead].is_open() {
                self.fetch = Some(self.start_fetch());
            }
        }
        futures_util::future::join_all(self.connections.iter_mut().map(Connection::close)).await;
    }

    // ---- groups -----------------------------------------------------------

    fn group_of(&self, connection: usize) -> usize {
        connection / self.config.connections
    }

    /// The connections of `group`.
    fn members(&self, group: usize) -> std::ops::Range<usize> {
        group * self.config.connections..(group + 1) * self.config.connections
    }

    // ---- reacting ---------------------------------------------------------

    fn on_heard(&mut self, index: usize, heard: Heard) {
        let group = self.group_of(index);
        match heard {
            Heard::Opened => {
                // A new socket has no subscriptions: follow every current
                // symbol of its group.
                let groups = self.config.groups;
                let following: Vec<String> = self
                    .symbols()
                    .into_iter()
                    .filter(|symbol| group_for(symbol, groups) == group)
                    .collect();
                let connection = &mut self.connections[index];
                connection.subscribe_lifecycle();
                connection.follow_all(following);
                if index == self.lead || !self.connections[self.lead].is_open() {
                    self.follow_lead(index);
                }
            }
            Heard::Lost(error) => {
                eprintln!("kalshi[{}]: connection lost: {error:#}", self.connections[index].name());
                if index == self.lead {
                    let open = (0..self.connections.len()).find(|&i| self.connections[i].is_open());
                    self.follow_lead(open.unwrap_or(index));
                }
                self.choose_source(group);
            }
            Heard::InSync | Heard::OutOfSync => self.choose_source(group),
            // Only the lead's lifecycle messages count. Mixing two streams
            // would let a slow one undo what the other has already said.
            Heard::Missed if index == self.lead => self.symbols_stale = true,
            Heard::Lifecycle(event) if index == self.lead => {
                let mut changes = Vec::new();
                match self.symbols.on_delta(event, &mut changes) {
                    Ok(()) => self.on_symbol_changes(changes),
                    Err(error) => self.distrust_symbols(error),
                }
            }
            Heard::Missed | Heard::Lifecycle(_) => {}
            // A connection that has just stopped being the source may still
            // have changes queued; they are no longer wanted.
            Heard::Book(event) if index == self.sources[group] => self.publish(event),
            Heard::Book(_) => {}
            Heard::Unhandled { kind, raw } => {
                if self.unhandled.insert(kind.clone()) {
                    eprintln!("kalshi: not handling `{kind}` messages; the first one: {raw}");
                }
            }
        }
    }

    /// Takes lifecycle messages from `lead` from now on. Whatever was said
    /// while nobody was listening, or on the old lead after the new one
    /// stopped hearing it, is unknown, so the list is fetched again; a fetch
    /// already in progress could have missed the same things.
    fn follow_lead(&mut self, lead: usize) {
        self.lead = lead;
        self.fetch = None;
        self.symbols_stale = true;
    }

    /// Keeps `group`'s source if its books can be trusted. Otherwise moves
    /// to a connection of the group whose books can, if there is one.
    fn choose_source(&mut self, group: usize) {
        if self.connections[self.sources[group]].in_sync() {
            return;
        }
        // If nobody is better, the source catches up by itself, publishing
        // each book's fresh snapshot as it arrives.
        if let Some(next) = self.members(group).find(|&i| self.connections[i].in_sync()) {
            self.switch_source(group, next);
        }
    }

    /// Makes `next` the source of `group` and publishes all of its books:
    /// the two connections may have seen different things, and a full
    /// snapshot of each book replaces whatever the old source said.
    fn switch_source(&mut self, group: usize, next: usize) {
        let old = std::mem::replace(&mut self.sources[group], next);
        eprintln!(
            "kalshi: the source is now connection {} (was {})",
            self.connections[next].name(),
            self.connections[old].name()
        );
        self.connections[old].set_reporting(false);
        self.connections[next].set_reporting(true);
        let mut books = self.books_of(next, Utc::now());
        books.sort_by(|a, b| a.0.cmp(&b.0));
        for (_, event) in books {
            self.publish(event);
        }
    }

    /// Passes changes to the symbol list on to the connections of each
    /// symbol's group, and publishes them.
    fn on_symbol_changes(&mut self, changes: Vec<SymbolChange>) {
        let mut by_group: Vec<Vec<SymbolChange>> = vec![Vec::new(); self.config.groups];
        for change in &changes {
            let (SymbolChange::Added(symbol) | SymbolChange::Removed(symbol)) = change;
            by_group[group_for(symbol, self.config.groups)].push(change.clone());
        }
        for (index, connection) in self.connections.iter_mut().enumerate() {
            connection.on_symbol_changes(&by_group[index / self.config.connections]);
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

    // ---- health -----------------------------------------------------------

    /// Compares the connections of each group with each other. Run every
    /// [`Health::check_every`].
    fn check_health(&mut self) {
        let health = self.config.health.clone();
        let now = tokio::time::Instant::now();
        for group in 0..self.config.groups {
            let members = self.members(group);

            // A subscription that has stopped delivering. The connection is
            // cut and connects again; if it was the source, losing it makes
            // another connection the source.
            for index in members.clone() {
                let others = members.clone().filter(|&other| other != index);
                let heard_elsewhere =
                    others.clone().filter_map(|other| self.connections[other].vitals.last_delta_at).max();
                let others_ready = others.clone().all(|other| self.connections[other].in_sync());
                let connection = &mut self.connections[index];
                if !connection.in_sync() {
                    connection.strikes = 0;
                    continue;
                }
                let last = connection.vitals.last_book_at;
                let quiet = heard_elsewhere.is_some_and(|heard| heard > last + health.quiet_after);
                // With nothing to compare against, go by this connection's
                // own habits. Not while another connection is itself
                // recovering: they take turns, so the feed is never dropped
                // everywhere at once.
                let silent = others_ready
                    && connection.vitals.usual_gap().is_some_and(|gap| {
                        now - last > health.silence_floor.max(gap * health.silence_factor)
                    });
                if !quiet && !silent {
                    connection.strikes = 0;
                    continue;
                }
                // Twice in a row, so a message we were merely late to read
                // is not taken for one that never came.
                connection.strikes += 1;
                if connection.strikes >= 2 {
                    let silence = now - last;
                    connection.drop_socket(&if quiet {
                        format!("quiet subscription: nothing for {silence:.1?} while another connection heard deltas")
                    } else {
                        format!("silent subscription: nothing for {silence:.1?}, far longer than usual")
                    });
                }
            }

            // A connection that delivers late. Kalshi's side of that socket
            // is taken to be the slow part, so it is cut and connects again;
            // if it was the source, the fastest connection takes over first.
            // The fastest is never cut, and only one connection per
            // `lag_dwell`, so a group is not emptied by this.
            let lags: Vec<(usize, f64)> = members
                .clone()
                .filter_map(|index| {
                    let connection = &mut self.connections[index];
                    // Taken either way, so what a connection saw while
                    // catching up is not counted against it afterwards.
                    let lag = connection.vitals.take_recent_lag();
                    connection.in_sync().then_some((index, lag?))
                })
                .collect();
            // No verdict without numbers from two connections: against our
            // own clock alone, a delay could as well be the clock.
            if lags.len() < 2 {
                continue;
            }
            let (fastest, fastest_lag) =
                lags.iter().copied().min_by(|a, b| a.1.total_cmp(&b.1)).expect("there are two");
            let margin = health.lag_margin.as_secs_f64() * 1000.0;
            let mut slowest: Option<(usize, f64)> = None;
            for &(index, lag) in &lags {
                let connection = &mut self.connections[index];
                if lag - fastest_lag <= margin {
                    connection.slow_checks = 0;
                    continue;
                }
                connection.slow_checks += 1;
                if connection.slow_checks >= health.lag_checks && slowest.is_none_or(|(_, worst)| lag > worst) {
                    slowest = Some((index, lag));
                }
            }
            let settled = self.lag_cut_at[group].is_none_or(|at| now - at >= health.lag_dwell);
            if let (Some((slow, lag)), true) = (slowest, settled) {
                self.lag_cut_at[group] = Some(now);
                if self.sources[group] == slow {
                    self.switch_source(group, fastest);
                }
                let faster = self.connections[fastest].name().to_owned();
                self.connections[slow].drop_socket(&format!(
                    "lagging: {lag:.0} ms behind Kalshi against {fastest_lag:.0} ms on connection {faster}"
                ));
            }
        }
    }

    /// One line on stderr: what is followed, what is written, and how each
    /// connection is doing.
    fn print_status(&mut self) {
        let seconds = self.config.status_interval.map_or(1.0, |interval| interval.as_secs_f64());
        let mut line = format!(
            "kalshi: {} symbols, {:.0} events/s written",
            self.symbols.state().symbols().len(),
            std::mem::take(&mut self.published) as f64 / seconds,
        );
        let detailed = self.connections.len() <= 4;
        let (mut healthy, mut messages, mut lag_sum, mut lag_max) = (0usize, 0u64, 0.0f64, f64::MIN);
        for index in 0..self.connections.len() {
            let role = if self.sources[self.group_of(index)] == index { "source" } else { "standby" };
            let connection = &mut self.connections[index];
            let name = connection.name().to_owned();
            let (count, lag) = connection.vitals.take_shown();
            if !connection.is_open() {
                line += &format!(" | [{name}] {role} DOWN");
            } else if !connection.in_sync() {
                line += &format!(" | [{name}] {role} catching up, {} books waiting", connection.waiting());
            } else {
                healthy += 1;
                messages += count;
                let rate = count as f64 / seconds;
                match lag {
                    Some((mean, max)) => {
                        lag_sum += mean;
                        lag_max = lag_max.max(max);
                        if detailed {
                            line += &format!(" | [{name}] {role} {rate:.0} msg/s, lag {mean:.0} ms (max {max:.0})");
                        }
                    }
                    None if detailed => line += &format!(" | [{name}] {role} {rate:.0} msg/s"),
                    None => {}
                }
            }
        }
        if !detailed {
            line += &format!(
                " | {healthy} of {} connections in sync, {:.0} msg/s",
                self.connections.len(),
                messages as f64 / seconds
            );
            if lag_max > f64::MIN {
                line += &format!(", lag {:.0} ms (max {lag_max:.0})", lag_sum / healthy.max(1) as f64);
            }
        }
        eprintln!("{line}");
    }

    // ---- publishing -------------------------------------------------------

    /// The once-a-minute publication of every book.
    fn publish_snapshots(&mut self) {
        let received_at = Utc::now();
        let mut books: Vec<(String, Event)> =
            self.sources.iter().flat_map(|&source| self.books_of(source, received_at)).collect();
        books.sort_by(|a, b| a.0.cmp(&b.0));
        for (_, event) in books {
            self.publish(event);
        }
        // Keep a steady cadence, unless we have fallen a whole interval behind.
        let now = tokio::time::Instant::now();
        let next = self.next_snapshot + self.config.snapshot_interval;
        self.next_snapshot = if next > now { next } else { now + self.config.snapshot_interval };
    }

    /// Every book of `connection` that is in sync, in full, by symbol. A
    /// book still waiting for its snapshot is left out: there is nothing
    /// trustworthy to say about it yet.
    fn books_of(&self, connection: usize, received_at: DateTime<Utc>) -> Vec<(String, Event)> {
        self.connections[connection]
            .books
            .iter()
            .filter(|(_, book)| book.is_live())
            .map(|(symbol, book)| {
                let book = book.state();
                let event = Event::Snapshot {
                    symbol: symbol.clone(),
                    bids: book.bids(),
                    asks: book.asks(),
                    received_at,
                    // The time of the last exchange message the book reflects.
                    exchange_timestamp: book
                        .exchange_ms()
                        .and_then(DateTime::from_timestamp_millis)
                        .unwrap_or(received_at),
                };
                (symbol.clone(), event)
            })
            .collect()
    }

    fn publish(&mut self, event: Event) {
        self.published += 1;
        // A dropped receiver just means nobody is listening.
        let _ = self.events.send(event);
    }
}

/// Waits for whichever connection has something to report first. When
/// several do, the one at position `turn` (wrapping around) wins, and the
/// caller advances `turn` so a busy connection cannot starve the others.
async fn next_heard(connections: &mut [Connection], turn: usize) -> (usize, Heard) {
    let first = turn % connections.len();
    let mut waiting: Vec<_> = connections
        .iter_mut()
        .enumerate()
        .map(|(index, connection)| Box::pin(async move { (index, connection.next().await) }))
        .collect();
    waiting.rotate_left(first);
    let (heard, _, _) = futures_util::future::select_all(waiting).await;
    heard
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
