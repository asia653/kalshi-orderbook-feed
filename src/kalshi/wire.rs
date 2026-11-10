//! Kalshi's WebSocket messages, parsed into a [`Frame`].

use anyhow::bail;
use serde::Deserialize;
use serde_json::Value;
use tokio_tungstenite::tungstenite::Message;

use super::book::{BookDelta, BookSide, BookSnapshot, Level, parse_fp, parse_price};
use super::symbols::LifecycleEvent;

pub(super) const LIFECYCLE_CHANNEL: &str = "market_lifecycle_v2";
pub(super) const BOOK_CHANNEL: &str = "orderbook_delta";

/// One parsed WebSocket message.
pub(super) struct Frame {
    pub(super) sid: Option<u64>,
    pub(super) seq: Option<u64>,
    /// Kalshi's time for the message, Unix milliseconds.
    pub(super) timestamp_ms: Option<i64>,
    pub(super) body: Body,
}

pub(super) enum Body {
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
    /// Kalshi ended a subscription. We never ask for that, so it means the
    /// subscription is dead and nothing more will arrive on it.
    Unsubscribed,
    /// A message type we do not act on; `raw` is its (shortened) text.
    Other { kind: String, raw: String },
}

impl Body {
    pub(super) fn kind(&self) -> &str {
        match self {
            Body::Lifecycle(_) => LIFECYCLE_CHANNEL,
            Body::Subscribed { .. } => "subscribed",
            Body::BookSnapshot { .. } => "orderbook_snapshot",
            Body::BookDelta { .. } => "orderbook_delta",
            Body::Ok => "ok",
            Body::EventLifecycle => "event_lifecycle",
            Body::Unsubscribed => "unsubscribed",
            Body::Other { kind, .. } => kind,
        }
    }

    pub(super) fn is_book(&self) -> bool {
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
pub(super) fn parse_message(message: Message) -> anyhow::Result<Option<Frame>> {
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
        ("unsubscribed", _) => Body::Unsubscribed,
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
