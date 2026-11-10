//! One symbol's order book.
//!
//! Kalshi stores bids only: bids for YES and bids for NO. A NO bid at price
//! `p` is an offer to sell YES at `1 - p`, so the NO side becomes the asks.
//!
//! Prices and quantities are fixed point with six decimals, the same scale
//! the SBE schema uses: 1_000_000 = 1 USDC / 1 contract.

use std::collections::BTreeMap;

use anyhow::{Context, ensure};

use super::synced::Syncable;
use crate::Side;

/// 1.0 in fixed point.
pub(crate) const ONE: i64 = 1_000_000;
const DECIMALS: usize = 6;

/// `(price_level, quantity)`.
pub type Level = (i64, i64);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BookSide {
    Yes,
    No,
}

/// An `orderbook_snapshot`: resting quantity at every price on both sides.
#[derive(Debug)]
pub(crate) struct BookSnapshot {
    pub(crate) clock: i64,
    /// Kalshi's time for the message, Unix milliseconds, when it sent one.
    pub(crate) exchange_ms: Option<i64>,
    pub(crate) yes: Vec<Level>,
    pub(crate) no: Vec<Level>,
}

/// An `orderbook_delta`: a signed change to one price level.
#[derive(Debug)]
pub(crate) struct BookDelta {
    pub(crate) clock: i64,
    pub(crate) exchange_ms: Option<i64>,
    pub(crate) side: BookSide,
    pub(crate) price: i64,
    pub(crate) delta: i64,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum BookChange {
    /// The whole book was replaced by a snapshot.
    Replaced,
    /// One level now holds `quantity` (zero means the level is gone).
    Level {
        side: Side,
        price_level: i64,
        quantity: i64,
    },
}

#[derive(Default)]
pub(crate) struct Book {
    /// Price -> quantity, in Kalshi's own terms (bids on each leg).
    yes: BTreeMap<i64, i64>,
    no: BTreeMap<i64, i64>,
    /// Exchange clock of the snapshot last loaded.
    clock: i64,
    /// Exchange time of the last message that changed the book.
    exchange_ms: Option<i64>,
}

impl Book {
    /// YES bids, best (highest) first.
    pub(crate) fn bids(&self) -> Vec<Level> {
        self.yes.iter().rev().map(|(p, q)| (*p, *q)).collect()
    }

    /// YES asks, best (lowest) first, derived from the NO bids.
    pub(crate) fn asks(&self) -> Vec<Level> {
        self.no.iter().rev().map(|(p, q)| (ONE - *p, *q)).collect()
    }

    pub(crate) fn exchange_ms(&self) -> Option<i64> {
        self.exchange_ms
    }
}

impl Syncable for Book {
    type Snapshot = BookSnapshot;
    type Delta = BookDelta;
    type Change = BookChange;

    fn load(&mut self, snapshot: BookSnapshot, changes: &mut Vec<BookChange>) {
        let levels = |side: Vec<Level>| side.into_iter().filter(|(_, q)| *q > 0).collect();
        self.yes = levels(snapshot.yes);
        self.no = levels(snapshot.no);
        self.clock = snapshot.clock;
        self.exchange_ms = snapshot.exchange_ms;
        changes.push(BookChange::Replaced);
    }

    fn apply(&mut self, delta: BookDelta, changes: &mut Vec<BookChange>) -> anyhow::Result<()> {
        let (levels, side, price_level) = match delta.side {
            BookSide::Yes => (&mut self.yes, Side::Buy, delta.price),
            BookSide::No => (&mut self.no, Side::Sell, ONE - delta.price),
        };
        let current = levels.get(&delta.price).copied().unwrap_or(0);
        let quantity = current
            .checked_add(delta.delta)
            .context("level quantity overflow")?;
        // A level cannot hold less than nothing; if it would, a delta was
        // missed or applied twice and the book is wrong.
        ensure!(
            quantity >= 0,
            "level {} would go negative ({current} {:+})",
            delta.price,
            delta.delta
        );
        if quantity == 0 {
            levels.remove(&delta.price);
        } else {
            levels.insert(delta.price, quantity);
        }
        self.exchange_ms = delta.exchange_ms.or(self.exchange_ms);
        changes.push(BookChange::Level {
            side,
            price_level,
            quantity,
        });
        Ok(())
    }

    /// A snapshot includes every change at or below its own clock, so a delta
    /// held while waiting for it only counts if its clock is higher.
    fn is_newer(&self, delta: &BookDelta) -> bool {
        delta.clock > self.clock
    }
}

/// Parses one of Kalshi's decimal strings (`"0.9600"`, `"300.00"`, `"-54.00"`)
/// into fixed point, exactly.
pub(crate) fn parse_fp(text: &str) -> anyhow::Result<i64> {
    let (negative, digits) = match text.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, text),
    };
    let (whole, fraction) = digits.split_once('.').unwrap_or((digits, ""));
    ensure!(
        !(whole.is_empty() && fraction.is_empty())
            && whole.bytes().chain(fraction.bytes()).all(|b| b.is_ascii_digit())
            && fraction.len() <= DECIMALS,
        "`{text}` is not a decimal with at most {DECIMALS} places"
    );
    let parse = |part: &str| -> anyhow::Result<i64> {
        if part.is_empty() {
            Ok(0)
        } else {
            part.parse().with_context(|| format!("`{text}` is out of range"))
        }
    };
    let scale = 10_i64.pow((DECIMALS - fraction.len()) as u32);
    let value = parse(whole)?
        .checked_mul(ONE)
        .and_then(|whole| whole.checked_add(parse(fraction).ok()? * scale))
        .with_context(|| format!("`{text}` is out of range"))?;
    Ok(if negative { -value } else { value })
}

/// Parses a price, which must lie between 0 and 1 dollar.
pub(crate) fn parse_price(text: &str) -> anyhow::Result<i64> {
    let price = parse_fp(text)?;
    ensure!((0..=ONE).contains(&price), "price `{text}` is outside 0..=1");
    Ok(price)
}

#[cfg(test)]
mod tests {
    use super::super::synced::Synced;
    use super::*;

    fn snapshot(clock: i64) -> BookSnapshot {
        BookSnapshot {
            clock,
            exchange_ms: Some(1_000),
            yes: vec![(80_000, 300 * ONE), (400_000, 20 * ONE)],
            no: vec![(540_000, 20 * ONE), (580_000, 10 * ONE)],
        }
    }

    fn delta(clock: i64, side: BookSide, price: i64, contracts: i64) -> BookDelta {
        BookDelta {
            clock,
            exchange_ms: Some(clock * 1_000),
            side,
            price,
            delta: contracts * ONE,
        }
    }

    #[test]
    fn parses_fixed_point_exactly() {
        assert_eq!(parse_fp("0.9600").unwrap(), 960_000);
        assert_eq!(parse_fp("300.00").unwrap(), 300_000_000);
        assert_eq!(parse_fp("-54.00").unwrap(), -54_000_000);
        assert_eq!(parse_fp("1").unwrap(), ONE);
        assert_eq!(parse_fp("0.000001").unwrap(), 1);
        assert_eq!(parse_fp(".5").unwrap(), 500_000);
        for bad in ["", "-", ".", "1.2.3", "1e3", "0.0000001", "abc", "+1", "99999999999999999999"] {
            assert!(parse_fp(bad).is_err(), "{bad:?} should be rejected");
        }
        assert!(parse_price("1.0000").is_ok());
        assert!(parse_price("1.0001").is_err());
        assert!(parse_price("-0.0100").is_err());
    }

    #[test]
    fn no_bids_become_asks() {
        let mut book = Book::default();
        book.load(snapshot(1), &mut Vec::new());
        assert_eq!(book.bids(), [(400_000, 20 * ONE), (80_000, 300 * ONE)]);
        // NO bids at 0.58 and 0.54 are YES asks at 0.42 and 0.46.
        assert_eq!(book.asks(), [(420_000, 10 * ONE), (460_000, 20 * ONE)]);
    }

    #[test]
    fn delta_reports_the_resulting_quantity_and_removes_empty_levels() {
        let mut book = Book::default();
        book.load(snapshot(1), &mut Vec::new());
        let mut changes = Vec::new();

        book.apply(delta(2, BookSide::Yes, 400_000, -5), &mut changes).unwrap();
        book.apply(delta(3, BookSide::No, 580_000, -10), &mut changes).unwrap();
        book.apply(delta(4, BookSide::Yes, 410_000, 7), &mut changes).unwrap();
        assert_eq!(
            changes,
            [
                BookChange::Level { side: Side::Buy, price_level: 400_000, quantity: 15 * ONE },
                BookChange::Level { side: Side::Sell, price_level: 420_000, quantity: 0 },
                BookChange::Level { side: Side::Buy, price_level: 410_000, quantity: 7 * ONE },
            ]
        );
        assert_eq!(book.bids()[0], (410_000, 7 * ONE));
        assert_eq!(book.asks(), [(460_000, 20 * ONE)]);
        assert_eq!(book.exchange_ms(), Some(4_000), "time of the last delta applied");
    }

    #[test]
    fn a_level_going_negative_is_an_error() {
        let mut book = Synced::new(Book::default());
        let mut changes = Vec::new();
        book.on_snapshot(snapshot(1), &mut changes).unwrap();
        let error = book.on_delta(delta(2, BookSide::Yes, 400_000, -21), &mut changes);
        assert!(error.is_err());
        assert!(!book.is_live(), "the book needs a new snapshot");
    }

    #[test]
    fn held_deltas_at_or_below_the_snapshot_clock_are_dropped() {
        let mut book = Synced::new(Book::default());
        let mut changes = Vec::new();
        // Both arrive while waiting for a snapshot whose clock is 10.
        book.on_delta(delta(10, BookSide::Yes, 400_000, -5), &mut changes).unwrap();
        book.on_delta(delta(11, BookSide::Yes, 400_000, 3), &mut changes).unwrap();
        book.on_snapshot(snapshot(10), &mut changes).unwrap();
        assert_eq!(
            changes,
            [
                BookChange::Replaced,
                BookChange::Level { side: Side::Buy, price_level: 400_000, quantity: 23 * ONE },
            ]
        );
    }
}
