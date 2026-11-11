//! One WebSocket connection to Kalshi and the books it keeps.
//!
//! A [`Connection`] is told which symbols to follow and reports what it
//! hears, one [`Heard`] at a time, from [`Connection::next`]. Everything to
//! do with the socket stays in here: connecting, reconnecting after a pause,
//! sending commands, checking sequence numbers, and re-fetching books after
//! a gap.
//!
//! Several connections can follow the same symbols. Each keeps its own
//! books, but only the one its owner has made the source reports changes to
//! them; the others stay ready to take over. [`Heard::InSync`] and
//! [`Heard::OutOfSync`] tell the owner which connections could.
//!
//! Commands are queued, never awaited. They are written by the same call
//! that reads, [`Connection::next`], so a socket that is slow to accept
//! them cannot hold up anything else.

use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::task::Poll;
use std::time::{Duration, Instant};

use anyhow::{Context, anyhow, bail};
use backon::{BackoffBuilder, ExponentialBackoff, ExponentialBuilder};
use chrono::{DateTime, Utc};
use futures_util::future::BoxFuture;
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::{self, Message};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};
use tracing::{info, warn};

use super::book::{Book, BookChange};
use super::symbols::{LifecycleEvent, SymbolChange};
use super::synced::Synced;
use super::wire::{BOOK_CHANNEL, Body, LIFECYCLE_CHANNEL, parse_message};
use super::{Auth, Event, auth_headers};

/// Symbols per subscribe / update command.
pub(super) const COMMAND_BATCH: usize = 500;
/// The wait before the first attempt to connect again.
const MIN_BACKOFF: Duration = Duration::from_millis(500);
/// The longest wait before any attempt, however many have failed. Kept
/// short on purpose: while every connection of a group is down, this wait
/// is time without data even once Kalshi is accepting connections again.
const MAX_BACKOFF: Duration = Duration::from_secs(5);
/// The shortest time between two re-fetches of every book. Several sequence
/// gaps close together are answered with one re-fetch, not one each.
const REFETCH_COOLDOWN: Duration = Duration::from_secs(5);
/// How long a closing handshake may take before the socket is just dropped.
const CLOSE_TIMEOUT: Duration = Duration::from_secs(2);
/// How long opening a WebSocket may take before the attempt counts as failed.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Gaps between deltas seen before the usual gap counts as known.
const GAPS_TO_LEARN: u32 = 20;

type Ws = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// How an open connection is doing. Its owner reads these to compare
/// connections with each other; one connection cannot tell a quiet market
/// from a dead subscription by itself.
pub(super) struct Vitals {
    /// When anything last arrived, Kalshi's 10-second pings included.
    pub(super) last_frame_at: tokio::time::Instant,
    /// When an orderbook message last arrived (or when the socket opened).
    pub(super) last_book_at: tokio::time::Instant,
    /// When a delta last arrived. Snapshots do not count: they answer our
    /// own requests and say nothing about whether the market is moving.
    pub(super) last_delta_at: Option<tokio::time::Instant>,
    /// The usual time between deltas, smoothed.
    usual_gap: Duration,
    gaps_seen: u32,
    /// Delay of orderbook messages since the owner last looked: the sum in
    /// milliseconds, and how many.
    recent_lag: (f64, u32),
    /// The same, plus the largest and a count of all messages, since the
    /// last status line.
    shown_lag: (f64, u32, f64),
    shown_messages: u64,
}

impl Vitals {
    fn new() -> Self {
        let now = tokio::time::Instant::now();
        Self {
            last_frame_at: now,
            last_book_at: now,
            last_delta_at: None,
            usual_gap: Duration::ZERO,
            gaps_seen: 0,
            recent_lag: (0.0, 0),
            shown_lag: (0.0, 0, f64::MIN),
            shown_messages: 0,
        }
    }

    /// The usual time between deltas, once enough have been seen to say.
    pub(super) fn usual_gap(&self) -> Option<Duration> {
        (self.gaps_seen >= GAPS_TO_LEARN).then_some(self.usual_gap)
    }

    /// Mean delay in milliseconds of the orderbook messages that arrived
    /// since this was last called, if any did.
    pub(super) fn take_recent_lag(&mut self) -> Option<f64> {
        let (sum, count) = std::mem::take(&mut self.recent_lag);
        (count > 0).then(|| sum / f64::from(count))
    }

    /// For the status line: messages, and the mean and largest delay in
    /// milliseconds, since this was last called.
    pub(super) fn take_shown(&mut self) -> (u64, Option<(f64, f64)>) {
        let (sum, count, max) = std::mem::replace(&mut self.shown_lag, (0.0, 0, f64::MIN));
        let lag = (count > 0).then(|| (sum / f64::from(count), max));
        (std::mem::take(&mut self.shown_messages), lag)
    }

    fn on_book_message(&mut self, delta: bool, lag_ms: Option<f64>) {
        let now = tokio::time::Instant::now();
        self.last_book_at = now;
        if delta {
            if let Some(last) = self.last_delta_at {
                let gap = now - last;
                self.usual_gap = if self.gaps_seen == 0 { gap } else { self.usual_gap.mul_f64(0.95) + gap.mul_f64(0.05) };
                self.gaps_seen += 1;
            }
            self.last_delta_at = Some(now);
        }
        if let Some(lag) = lag_ms {
            self.recent_lag = (self.recent_lag.0 + lag, self.recent_lag.1 + 1);
            self.shown_lag = (self.shown_lag.0 + lag, self.shown_lag.1 + 1, self.shown_lag.2.max(lag));
        }
    }
}

/// What a connection reports to its owner.
pub(super) enum Heard {
    /// The WebSocket is open. Nothing is subscribed on it yet.
    Opened,
    /// The connection failed. Its books are gone, and it will connect again
    /// by itself after a pause.
    Lost(anyhow::Error),
    /// A market lifecycle message.
    Lifecycle(LifecycleEvent),
    /// A message was missed on a subscription other than the order books,
    /// so a lifecycle message may be missing.
    Missed,
    /// A book was loaded or changed. Only the source reports these.
    Book(Event),
    /// Every book that needed a snapshot when the socket opened, or after
    /// the last sequence gap, has loaded one: this connection's books can be
    /// trusted as a whole.
    InSync,
    /// A sequence gap: no book can be trusted until it has been re-fetched.
    OutOfSync,
    /// A message type we do not act on; `raw` is its (shortened) text.
    Unhandled { kind: String, raw: String },
}

enum Link {
    /// Not connected. The next attempt starts at this time.
    Waiting(tokio::time::Instant),
    Connecting(BoxFuture<'static, anyhow::Result<Ws>>),
    Open(Ws),
    /// Closed for good by [`Connection::close`].
    Closed,
}

/// What [`Connection::next`] found to do.
enum Step {
    Connect,
    Connected(anyhow::Result<Ws>),
    Read(Option<Result<Message, tungstenite::Error>>),
    Refetch,
    Stalled,
    Dead,
}

pub(super) struct Connection {
    /// What it is called in log lines.
    name: String,
    /// Whether this is the source, whose book changes are reported.
    reporting: bool,
    /// How long nothing at all may arrive before the socket counts as dead.
    dead_after: Duration,
    /// How long to wait for snapshots that were asked for before asking for
    /// more anyway.
    stalled_after: Duration,
    /// The most books that may be asked for and still be waiting for their
    /// snapshot: see [`super::KalshiConfig::snapshot_window`].
    snapshot_window: usize,
    pub(super) vitals: Vitals,
    /// Health checks in a row that found this connection quiet. Its owner
    /// keeps count here.
    pub(super) strikes: u32,
    /// Health checks in a row that found this connection lagging behind the
    /// fastest of its group. Its owner keeps count here.
    pub(super) slow_checks: u32,
    ws_url: String,
    auth: Option<Arc<Auth>>,
    link: Link,
    /// When the current connection attempt started.
    started: Instant,
    /// How long to wait before each next attempt, while attempts keep
    /// failing: see [`reconnect_pauses`].
    pauses: ExponentialBackoff,
    /// Commands not yet handed to the socket.
    outbox: VecDeque<Message>,
    /// Commands handed to the socket that it has not finished sending.
    unflushed: bool,
    /// Reports waiting to be returned by [`Connection::next`].
    heard: VecDeque<Heard>,

    // Everything below belongs to the open socket and is reset when it is lost.
    /// A book for every symbol this connection follows.
    pub(super) books: HashMap<String, Synced<Book>>,
    /// Symbols whose snapshot is still needed before the books as a whole
    /// can be trusted. Filled when the socket opens and on a sequence gap.
    /// A symbol added at any other time is not included: its snapshot is on
    /// its way, and a steady trickle of new symbols must not keep a
    /// connection from ever counting as in sync.
    catching_up: HashSet<String>,
    /// See [`Heard::InSync`].
    in_sync: bool,
    /// Symbols Kalshi has been asked to send books for.
    subscribed: HashSet<String>,
    /// Symbols to subscribe to, not yet asked for: see `snapshot_window`.
    to_subscribe: BTreeSet<String>,
    /// Subscribed symbols whose snapshot is to be asked for again, not yet
    /// asked for.
    to_refresh: BTreeSet<String>,
    /// Symbols asked for whose snapshot has not arrived yet.
    asked: HashSet<String>,
    /// When a snapshot that was asked for last arrived, or more were asked for.
    progress_at: tokio::time::Instant,
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
    next_command_id: u64,
}

impl Connection {
    /// A connection that makes its first attempt as soon as it is polled.
    pub(super) fn new(
        name: String,
        ws_url: String,
        auth: Option<Arc<Auth>>,
        dead_after: Duration,
        stalled_after: Duration,
        snapshot_window: usize,
    ) -> Self {
        Self {
            name,
            reporting: false,
            dead_after,
            stalled_after,
            snapshot_window,
            vitals: Vitals::new(),
            strikes: 0,
            slow_checks: 0,
            ws_url,
            auth,
            link: Link::Waiting(tokio::time::Instant::now()),
            started: Instant::now(),
            pauses: reconnect_pauses(),
            outbox: VecDeque::new(),
            unflushed: false,
            heard: VecDeque::new(),
            books: HashMap::new(),
            catching_up: HashSet::new(),
            in_sync: false,
            subscribed: HashSet::new(),
            to_subscribe: BTreeSet::new(),
            to_refresh: BTreeSet::new(),
            asked: HashSet::new(),
            progress_at: tokio::time::Instant::now(),
            book_sid: None,
            deferred: Vec::new(),
            last_seq: HashMap::new(),
            refetch_at: None,
            last_refetch: None,
            next_command_id: 0,
        }
    }

    /// What this connection is called in the log.
    pub(super) fn name(&self) -> &str {
        &self.name
    }

    pub(super) fn is_open(&self) -> bool {
        matches!(self.link, Link::Open(_))
    }

    pub(super) fn in_sync(&self) -> bool {
        self.in_sync
    }

    /// How many books are waiting for a snapshot.
    pub(super) fn waiting(&self) -> usize {
        self.books.values().filter(|book| !book.is_live()).count()
    }

    /// Makes this connection the source, or stops it being the source.
    pub(super) fn set_reporting(&mut self, reporting: bool) {
        self.reporting = reporting;
    }

    /// Drops the socket without a closing handshake, as a network failure
    /// would. The connection then reconnects as after any other loss.
    pub(super) fn drop_socket(&mut self, why: &str) {
        if self.is_open() {
            self.lose(anyhow!("{why}"));
        }
    }

    /// Waits for the next thing worth reporting, doing the connection's own
    /// work in the meantime. Safe to drop and call again: nothing is lost.
    pub(super) async fn next(&mut self) -> Heard {
        loop {
            if let Some(heard) = self.heard.pop_front() {
                return heard;
            }
            let stalled_at = self.stalled_at();
            let step = match &mut self.link {
                Link::Waiting(until) => {
                    tokio::time::sleep_until(*until).await;
                    Step::Connect
                }
                Link::Connecting(attempt) => Step::Connected(attempt.await),
                Link::Open(ws) => {
                    let refetch_at = self.refetch_at;
                    let dead_at = self.vitals.last_frame_at + self.dead_after;
                    tokio::select! {
                        // In this order: a re-fetch that is due must not wait
                        // behind a busy socket, and a socket with something
                        // to read is not dead, however late we are to read it.
                        biased;
                        _ = async { tokio::time::sleep_until(refetch_at.expect("guarded")).await },
                            if refetch_at.is_some() => Step::Refetch,
                        read = pump(ws, &mut self.outbox, &mut self.unflushed) => Step::Read(read),
                        _ = async { tokio::time::sleep_until(stalled_at.expect("guarded")).await },
                            if stalled_at.is_some() => Step::Stalled,
                        _ = tokio::time::sleep_until(dead_at) => Step::Dead,
                    }
                }
                Link::Closed => std::future::pending().await,
            };
            // No `await` from here on, so a step is never half done.
            match step {
                Step::Connect => {
                    self.started = Instant::now();
                    self.link = Link::Connecting(self.connect());
                }
                Step::Connected(Ok(ws)) => {
                    self.link = Link::Open(ws);
                    self.vitals = Vitals::new();
                    self.heard.push_back(Heard::Opened);
                }
                Step::Connected(Err(error)) => self.lose(error),
                Step::Read(Some(Ok(message))) => {
                    self.vitals.last_frame_at = tokio::time::Instant::now();
                    if let Err(error) = self.on_message(message) {
                        self.lose(error);
                    }
                    // A socket that always has something to read never
                    // reaches the timer above, so look here as well.
                    self.on_stall();
                }
                Step::Read(Some(Err(error))) => self.lose(error.into()),
                Step::Read(None) => self.lose(anyhow!("websocket closed")),
                Step::Refetch => {
                    // Snapshots on their way may be what was missed, so
                    // those books are asked for again too.
                    self.asked.clear();
                    let stale = self.take_stale_books();
                    self.to_refresh.extend(stale);
                    self.release();
                    self.settle();
                }
                Step::Stalled => self.on_stall(),
                Step::Dead => self.lose(anyhow!(
                    "nothing arrived, not even a ping, for {:?}",
                    self.dead_after
                )),
            }
        }
    }

    /// Closes the WebSocket politely: sends a close frame and waits, briefly,
    /// for Kalshi to acknowledge it. The connection stays closed.
    pub(super) async fn close(&mut self) {
        let Link::Open(mut ws) = std::mem::replace(&mut self.link, Link::Closed) else {
            return;
        };
        let handshake = async {
            if ws.close(None).await.is_ok() {
                // Anything still in flight is read and discarded until
                // Kalshi's own close frame (or the end of the stream).
                while let Some(Ok(message)) = ws.next().await {
                    if message.is_close() {
                        break;
                    }
                }
            }
        };
        if tokio::time::timeout(CLOSE_TIMEOUT, handshake).await.is_err() {
            warn!("[{}] no close acknowledgement; dropping the connection", self.name);
        }
    }

    fn connect(&self) -> BoxFuture<'static, anyhow::Result<Ws>> {
        let (ws_url, auth) = (self.ws_url.clone(), self.auth.clone());
        Box::pin(async move {
            let mut request = ws_url.as_str().into_client_request()?;
            let path = request.uri().path().to_owned();
            for (name, value) in auth_headers(&auth, "GET", &path)? {
                request.headers_mut().insert(name, value.parse()?);
            }
            let (ws, _) = tokio::time::timeout(CONNECT_TIMEOUT, tokio_tungstenite::connect_async(request))
                .await
                .context("websocket connect timed out")?
                .context("websocket connect")?;
            Ok(ws)
        })
    }

    /// Forgets everything about the socket and schedules the next attempt,
    /// after the next of [`reconnect_pauses`].
    fn lose(&mut self, error: anyhow::Error) {
        // A connection that lasted a while has earned a prompt first retry.
        if self.started.elapsed() > Duration::from_secs(60) {
            self.pauses = reconnect_pauses();
        }
        let pause = self.pauses.next().unwrap_or(MAX_BACKOFF);
        self.link = Link::Waiting(tokio::time::Instant::now() + pause);

        self.outbox.clear();
        self.unflushed = false;
        self.books.clear();
        self.catching_up.clear();
        self.in_sync = false;
        self.strikes = 0;
        self.slow_checks = 0;
        self.subscribed.clear();
        self.to_subscribe.clear();
        self.to_refresh.clear();
        self.asked.clear();
        self.book_sid = None;
        self.deferred.clear();
        self.last_seq.clear();
        self.refetch_at = None;
        self.heard.push_back(Heard::Lost(error));
    }

    // ---- commands ---------------------------------------------------------

    fn command(&mut self, cmd: &str, params: Value) {
        if !self.is_open() {
            return;
        }
        self.next_command_id += 1;
        let command = json!({ "id": self.next_command_id, "cmd": cmd, "params": params });
        self.outbox.push_back(Message::Text(command.to_string().into()));
    }

    pub(super) fn subscribe_lifecycle(&mut self) {
        self.command("subscribe", json!({ "channels": [LIFECYCLE_CHANNEL] }));
    }

    /// Follows `symbols` on a socket that has just opened. The connection
    /// is in sync once every one of them has loaded a snapshot, along with
    /// any symbol added in the meantime. `symbols` is empty when the socket
    /// opens before the symbol list is known; the connection is then not in
    /// sync until the list has arrived and its books have loaded.
    pub(super) fn follow_all(&mut self, symbols: Vec<String>) {
        self.catching_up = symbols.iter().cloned().collect();
        let added: Vec<SymbolChange> = symbols.into_iter().map(SymbolChange::Added).collect();
        self.on_symbol_changes(&added);
    }

    /// Creates and drops books to match the symbol list and tells Kalshi.
    /// Does nothing while the socket is not open: once it opens, the owner
    /// calls [`Connection::follow_all`] with every current symbol.
    pub(super) fn on_symbol_changes(&mut self, changes: &[SymbolChange]) {
        if !self.is_open() {
            return;
        }
        let mut unsubscribe = BTreeSet::new();
        for change in changes {
            match change {
                SymbolChange::Added(symbol) => {
                    self.books.insert(symbol.clone(), Synced::new(Book::default()));
                    if !self.in_sync {
                        self.catching_up.insert(symbol.clone());
                    }
                    if self.subscribed.contains(symbol) {
                        // Removed and re-added before we unsubscribed: Kalshi
                        // will not resend a snapshot unless asked.
                        unsubscribe.remove(symbol);
                        self.to_refresh.insert(symbol.clone());
                    } else {
                        self.to_subscribe.insert(symbol.clone());
                    }
                }
                SymbolChange::Removed(symbol) => {
                    self.books.remove(symbol);
                    self.catching_up.remove(symbol);
                    self.to_refresh.remove(symbol);
                    self.asked.remove(symbol);
                    if !self.to_subscribe.remove(symbol) && self.subscribed.contains(symbol) {
                        unsubscribe.insert(symbol.clone());
                    }
                }
            }
        }
        for symbol in &unsubscribe {
            self.subscribed.remove(symbol);
        }
        self.update_books("delete_markets", unsubscribe.into_iter().collect());
        self.release();
        self.settle();
    }

    /// Notices the moment every book has caught up. A connection with no
    /// books has nothing to offer yet, so it does not count.
    fn settle(&mut self) {
        let caught_up = !self.books.is_empty() && self.catching_up.is_empty() && self.refetch_at.is_none();
        if self.is_open() && !self.in_sync && caught_up {
            self.in_sync = true;
            self.heard.push_back(Heard::InSync);
        }
    }

    /// Asks Kalshi for books that are waiting to be asked for, as far as
    /// `snapshot_window` allows. Called whenever there may be room: after
    /// the symbol list changes and after each snapshot that was asked for.
    ///
    /// Books to refresh go first: they are already receiving changes that
    /// cannot be applied until their snapshot arrives.
    fn release(&mut self) {
        while self.is_open() && self.asked.len() + COMMAND_BATCH <= self.snapshot_window {
            let batch = if let (Some(sid), false) = (self.book_sid, self.to_refresh.is_empty()) {
                let batch = take_first(&mut self.to_refresh, COMMAND_BATCH);
                let params = json!({ "sids": [sid], "market_tickers": batch, "action": "get_snapshot" });
                self.command("update_subscription", params);
                batch
            } else if !self.to_subscribe.is_empty() {
                // A repeated subscribe adds markets to the connection's one
                // orderbook subscription, and each new market gets a snapshot.
                let batch = take_first(&mut self.to_subscribe, COMMAND_BATCH);
                let params = json!({
                    "channels": [BOOK_CHANNEL],
                    "market_tickers": batch,
                    // Keep NO levels in NO prices; `Book` converts them to asks.
                    "use_yes_price": false,
                    // Without this, each acknowledgement lists the tickers.
                    "skip_ticker_ack": true,
                });
                self.command("subscribe", params);
                self.subscribed.extend(batch.iter().cloned());
                batch
            } else {
                break;
            };
            self.asked.extend(batch);
            self.progress_at = tokio::time::Instant::now();
        }
    }

    /// When to stop waiting for the snapshots that were asked for, if more
    /// books are waiting behind them.
    fn stalled_at(&self) -> Option<tokio::time::Instant> {
        let more = !self.to_subscribe.is_empty() || !self.to_refresh.is_empty();
        (more && !self.asked.is_empty()).then_some(self.progress_at + self.stalled_after)
    }

    /// Snapshots that were asked for have stopped arriving. A book whose
    /// snapshot never comes must not hold up all the others, so the wait is
    /// given up and the next books are asked for.
    fn on_stall(&mut self) {
        if self.stalled_at().is_some_and(|at| at <= tokio::time::Instant::now()) {
            warn!(
                "[{}] {} books asked for have sent no snapshot in {:?}; asking for the next ones",
                self.name,
                self.asked.len(),
                self.stalled_after,
            );
            self.asked.clear();
            self.release();
        }
    }

    /// Sends `delete_markets` for `symbols`, or defers it until the orderbook
    /// subscription id is known.
    fn update_books(&mut self, action: &'static str, symbols: Vec<String>) {
        if symbols.is_empty() {
            return;
        }
        let Some(sid) = self.book_sid else {
            self.deferred.push((action, symbols));
            return;
        };
        for batch in symbols.chunks(COMMAND_BATCH) {
            let params = json!({ "sids": [sid], "market_tickers": batch, "action": action });
            self.command("update_subscription", params);
        }
    }

    fn learn_book_sid(&mut self, sid: u64) {
        if self.book_sid.is_none() {
            self.book_sid = Some(sid);
            for (action, symbols) in std::mem::take(&mut self.deferred) {
                self.update_books(action, symbols);
            }
            self.release();
        }
    }

    // ---- messages ---------------------------------------------------------

    /// An error means the connection can no longer be trusted.
    fn on_message(&mut self, message: Message) -> anyhow::Result<()> {
        let received_at = Utc::now();
        let Some(frame) = parse_message(message)? else {
            return Ok(());
        };
        self.vitals.shown_messages += 1;
        if frame.body.is_book() {
            let lag_ms = frame.sent_ms.map(|sent| (received_at.timestamp_millis() - sent) as f64);
            self.vitals.on_book_message(matches!(frame.body, Body::BookDelta { .. }), lag_ms);
        }
        if let (true, Some(sid)) = (frame.body.is_book(), frame.sid) {
            self.learn_book_sid(sid);
        }
        // Every message on a subscription is numbered, whatever its type, so
        // every one is checked here before its type is looked at.
        if let (Some(sid), Some(seq)) = (frame.sid, frame.seq) {
            let last = self.last_seq.get(&sid).copied();
            if let (true, Some(last)) = (self.is_gap(sid, seq), last) {
                warn!(
                    "[{}] sequence gap on subscription {sid}: expected {}, got {seq} ({})",
                    self.name,
                    last + 1,
                    frame.body.kind(),
                );
                if self.book_sid == Some(sid) {
                    self.on_book_gap();
                } else {
                    self.heard.push_back(Heard::Missed);
                }
            }
        }
        let exchange_timestamp = frame
            .timestamp_ms
            .and_then(DateTime::from_timestamp_millis)
            .unwrap_or(received_at);

        match frame.body {
            Body::Lifecycle(event) => self.heard.push_back(Heard::Lifecycle(event)),
            Body::Subscribed { channel, sid } => {
                info!("[{}] subscribed to {channel} as subscription {sid}", self.name);
                if channel == BOOK_CHANNEL {
                    self.learn_book_sid(sid);
                }
            }
            // Their sequence numbers were counted above; nothing else to do.
            Body::Ok | Body::EventLifecycle => {}
            // Seen live: Kalshi sent this for the orderbook subscription and
            // closed the socket a moment later. A subscription that is gone
            // is no use, so do not wait for the close.
            Body::Unsubscribed => bail!("Kalshi ended subscription {:?}", frame.sid),
            Body::Other { kind, raw } => self.heard.push_back(Heard::Unhandled { kind, raw }),
            Body::BookSnapshot { symbol, snapshot } => {
                if self.asked.remove(&symbol) {
                    self.progress_at = tokio::time::Instant::now();
                    self.release();
                }
                if let Some(book) = self.books.get_mut(&symbol) {
                    let mut changes = Vec::new();
                    book.on_snapshot(snapshot, &mut changes)?;
                    self.catching_up.remove(&symbol);
                    self.report(&symbol, changes, received_at, exchange_timestamp);
                    self.settle();
                }
            }
            Body::BookDelta { symbol, delta } => {
                // A late delta for a symbol we have dropped is not an error.
                if let Some(book) = self.books.get_mut(&symbol) {
                    let mut changes = Vec::new();
                    let applied = book.on_delta(delta, &mut changes);
                    self.report(&symbol, changes, received_at, exchange_timestamp);
                    if let Err(error) = applied {
                        warn!("[{}] {symbol}: {error:#}; requesting a snapshot", self.name);
                        self.to_refresh.insert(symbol);
                        self.release();
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
        self.catching_up = self.books.keys().cloned().collect();
        if std::mem::take(&mut self.in_sync) {
            self.heard.push_back(Heard::OutOfSync);
        }
        let now = tokio::time::Instant::now();
        let earliest = self.last_refetch.map_or(now, |last| last + REFETCH_COOLDOWN);
        self.refetch_at.get_or_insert(earliest.max(now));
    }

    /// The symbols whose books need a new snapshot, sorted. A book that
    /// loaded a snapshot after the last gap is already correct and is left
    /// alone, and so is one not subscribed to yet: its snapshot comes when
    /// it is.
    fn take_stale_books(&mut self) -> Vec<String> {
        self.refetch_at = None;
        self.last_refetch = Some(tokio::time::Instant::now());
        let mut stale: Vec<String> = self
            .books
            .iter()
            .filter(|(symbol, book)| !book.is_live() && self.subscribed.contains(*symbol))
            .map(|(symbol, _)| symbol.clone())
            .collect();
        stale.sort();
        info!("[{}] re-fetching {} books after a sequence gap", self.name, stale.len());
        stale
    }

    /// Queues what happened to `symbol`'s book as events.
    fn report(
        &mut self,
        symbol: &str,
        changes: Vec<BookChange>,
        received_at: DateTime<Utc>,
        exchange_timestamp: DateTime<Utc>,
    ) {
        let Some(book) = self.books.get(symbol) else {
            return;
        };
        if !self.reporting {
            return;
        }
        for change in changes {
            let event = match change {
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
            };
            self.heard.push_back(Heard::Book(event));
        }
    }
}

/// The waits before successive attempts to connect. The base wait starts at
/// [`MIN_BACKOFF`] and doubles with each failure; each wait is the base
/// stretched by a random amount, up to twice as long, so that connections
/// lost together do not all return together. No wait is longer than
/// [`MAX_BACKOFF`]: 0.5 to 1 s, then 1 to 2 s, 2 to 4 s, and 2.5 to 5 s
/// from then on.
fn reconnect_pauses() -> ExponentialBackoff {
    ExponentialBuilder::new()
        .with_min_delay(MIN_BACKOFF)
        // Half, because the random stretch can double it.
        .with_max_delay(MAX_BACKOFF / 2)
        .with_jitter()
        .without_max_times()
        .build()
}

/// Removes and returns the first `count` of `symbols`, in order.
fn take_first(symbols: &mut BTreeSet<String>, count: usize) -> Vec<String> {
    let batch: Vec<String> = symbols.iter().take(count).cloned().collect();
    for symbol in &batch {
        symbols.remove(symbol);
    }
    batch
}

/// Hands queued commands to the socket, and resolves with the next message
/// read from it (`None` once the stream has ended). Dropping it loses
/// nothing: a command leaves the queue only when the socket has taken it.
async fn pump(
    ws: &mut Ws,
    outbox: &mut VecDeque<Message>,
    unflushed: &mut bool,
) -> Option<Result<Message, tungstenite::Error>> {
    std::future::poll_fn(|cx| {
        while !outbox.is_empty() {
            match ws.poll_ready_unpin(cx) {
                Poll::Ready(Ok(())) => {
                    let command = outbox.pop_front().expect("checked not empty");
                    if let Err(error) = ws.start_send_unpin(command) {
                        return Poll::Ready(Some(Err(error)));
                    }
                    *unflushed = true;
                }
                Poll::Ready(Err(error)) => return Poll::Ready(Some(Err(error))),
                // The socket is busy sending; it will wake us when it can take more.
                Poll::Pending => break,
            }
        }
        if *unflushed {
            match ws.poll_flush_unpin(cx) {
                Poll::Ready(Ok(())) => *unflushed = false,
                Poll::Ready(Err(error)) => return Poll::Ready(Some(Err(error))),
                Poll::Pending => {}
            }
        }
        ws.poll_next_unpin(cx)
    })
    .await
}

#[cfg(test)]
mod tests {
    use super::super::book::BookSnapshot;
    use super::*;

    fn connection() -> Connection {
        Connection::new("A".to_owned(), "ws://unused".to_owned(), None, Duration::from_secs(25), Duration::from_secs(5), 2_000)
    }

    #[test]
    fn sequence_gaps_are_detected_per_subscription() {
        let mut connection = connection();
        assert!(!connection.is_gap(1, 7), "the first message sets the baseline");
        assert!(!connection.is_gap(1, 8));
        assert!(!connection.is_gap(2, 1), "another subscription has its own count");
        assert!(connection.is_gap(1, 10));
        assert!(!connection.is_gap(1, 11), "counting resumes from the new baseline");
    }

    #[test]
    fn gaps_close_together_share_one_refetch() {
        let mut connection = connection();
        let load = |connection: &mut Connection, symbol: &str| {
            let snapshot = BookSnapshot { clock: 1, exchange_ms: None, yes: vec![], no: vec![] };
            let book = connection
                .books
                .entry(symbol.to_owned())
                .or_insert_with(|| Synced::new(Book::default()));
            book.on_snapshot(snapshot, &mut Vec::new()).unwrap();
            connection.subscribed.insert(symbol.to_owned());
        };
        load(&mut connection, "B");
        load(&mut connection, "A");

        // The first gap is answered at once, for every book.
        connection.on_book_gap();
        assert!(connection.refetch_at.unwrap() <= tokio::time::Instant::now());
        assert_eq!(connection.take_stale_books(), ["A", "B"]);
        assert!(connection.refetch_at.is_none());
        let sent = connection.last_refetch.unwrap();

        // A's new snapshot arrives, then two more gaps before B's does.
        load(&mut connection, "A");
        connection.on_book_gap();
        connection.on_book_gap();
        assert_eq!(connection.refetch_at, Some(sent + REFETCH_COOLDOWN), "one re-fetch, after the cooldown");
        assert_eq!(connection.take_stale_books(), ["A", "B"], "a gap taints every book again");

        // A book that loaded a snapshot after the last gap is not asked for again.
        connection.on_book_gap();
        load(&mut connection, "A");
        assert_eq!(connection.take_stale_books(), ["B"]);
    }

    #[test]
    fn commands_are_dropped_while_the_socket_is_not_open() {
        let mut connection = connection();
        connection.subscribe_lifecycle();
        connection.follow_all(vec!["A".to_owned()]);
        assert!(connection.outbox.is_empty() && connection.books.is_empty());
    }
}
