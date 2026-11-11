//! The symbol list: every regular Kalshi market that is currently trading.
//!
//! Its snapshot is the open set from REST and its deltas are
//! `market_lifecycle_v2` events. Nothing here does any I/O.

use std::collections::{BTreeSet, HashMap};
use std::time::Duration;

use serde::Deserialize;

use super::synced::Syncable;

/// A change to the symbol list.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SymbolChange {
    Added(String),
    Removed(String),
}

/// The open markets REST returned: ticker -> close time (Unix seconds).
pub(crate) struct OpenSet {
    /// Unix seconds at which the fetch started.
    pub(crate) fetched_at: i64,
    pub(crate) markets: HashMap<String, Option<i64>>,
}

/// The `msg` of a `market_lifecycle_v2` message.
#[derive(Debug, Deserialize)]
pub(crate) struct LifecycleEvent {
    pub(crate) market_ticker: String,
    pub(crate) event_type: String,
    pub(crate) close_ts: Option<i64>,
    pub(crate) is_deactivated: Option<bool>,
}

/// A market we know about. Only `active` ones are symbols; a market that was
/// created but has not opened yet is tracked but not listed.
struct Market {
    active: bool,
    /// Unix seconds.
    close_ts: Option<i64>,
}

pub(crate) struct SymbolList {
    /// Only follow markets in this series. `None` follows everything.
    series: Option<String>,
    markets: HashMap<String, Market>,
    /// Every known close time with its market, soonest first. Kept beside
    /// `markets` so the next close is found without looking at every market:
    /// that question is asked once per message received.
    closes: BTreeSet<(i64, String)>,
    /// How many markets are active.
    active: usize,
}

impl SymbolList {
    pub(crate) fn new(series: Option<String>) -> Self {
        Self {
            series,
            markets: HashMap::new(),
            closes: BTreeSet::new(),
            active: 0,
        }
    }

    /// The current symbols, sorted.
    pub(crate) fn symbols(&self) -> Vec<String> {
        let mut symbols: Vec<String> = self
            .markets
            .iter()
            .filter(|(_, market)| market.active)
            .map(|(ticker, _)| ticker.clone())
            .collect();
        symbols.sort();
        symbols
    }

    /// How many symbols there are.
    pub(crate) fn len(&self) -> usize {
        self.active
    }

    /// Removes every market whose close time has passed. Kalshi sends no
    /// "closed" event, so this is how a symbol stops trading on time.
    pub(crate) fn expire(&mut self, now: i64, changes: &mut Vec<SymbolChange>) {
        while self.closes.first().is_some_and(|(close, _)| *close <= now) {
            let (_, ticker) = self.closes.pop_first().expect("checked not empty");
            self.remove(&ticker, changes);
        }
    }

    pub(crate) fn until_next_close(&self, now: i64) -> Duration {
        match self.closes.first() {
            Some((close, _)) => Duration::from_secs(close.saturating_sub(now).max(0) as u64),
            None => Duration::from_secs(24 * 60 * 60),
        }
    }

    /// Records `ticker`'s close time, if the market is known.
    fn set_close(&mut self, ticker: &str, close_ts: Option<i64>) {
        let Some(market) = self.markets.get_mut(ticker) else {
            return;
        };
        if let Some(old) = std::mem::replace(&mut market.close_ts, close_ts) {
            self.closes.remove(&(old, ticker.to_owned()));
        }
        if let Some(new) = close_ts {
            self.closes.insert((new, ticker.to_owned()));
        }
    }

    fn follows(&self, ticker: &str) -> bool {
        match &self.series {
            // Market tickers begin with their series ticker, e.g. `KXBTC-...`.
            Some(series) => ticker
                .strip_prefix(series.as_str())
                .is_some_and(|rest| rest.starts_with('-')),
            None => true,
        }
    }

    fn set_active(
        &mut self,
        ticker: String,
        active: bool,
        close_ts: Option<i64>,
        changes: &mut Vec<SymbolChange>,
    ) {
        self.markets.entry(ticker.clone()).or_insert(Market { active: false, close_ts: None });
        if close_ts.is_some() {
            self.set_close(&ticker, close_ts);
        }
        let market = self.markets.get_mut(&ticker).expect("just inserted");
        if market.active != active {
            market.active = active;
            if active {
                self.active += 1;
                changes.push(SymbolChange::Added(ticker));
            } else {
                self.active -= 1;
                changes.push(SymbolChange::Removed(ticker));
            }
        }
    }

    fn remove(&mut self, ticker: &str, changes: &mut Vec<SymbolChange>) {
        if let Some(market) = self.markets.remove(ticker) {
            if let Some(close) = market.close_ts {
                self.closes.remove(&(close, ticker.to_owned()));
            }
            if market.active {
                self.active -= 1;
                changes.push(SymbolChange::Removed(ticker.to_owned()));
            }
        }
    }
}

impl Syncable for SymbolList {
    type Snapshot = OpenSet;
    type Delta = LifecycleEvent;
    type Change = SymbolChange;

    /// Makes the list match the open set REST returned.
    fn load(&mut self, open: OpenSet, changes: &mut Vec<SymbolChange>) {
        let gone: Vec<String> = self
            .markets
            .iter()
            .filter(|(ticker, market)| market.active && !open.markets.contains_key(*ticker))
            .map(|(ticker, _)| ticker.clone())
            .collect();
        for ticker in gone {
            self.remove(&ticker, changes);
        }
        for (ticker, close_ts) in open.markets {
            // A market REST still calls open after its close time would be
            // expired again straight away; leave it out instead of flapping.
            let closed = close_ts.is_some_and(|close| close <= open.fetched_at);
            if self.follows(&ticker) && !closed {
                self.set_active(ticker, true, close_ts, changes);
            }
        }
    }

    fn apply(
        &mut self,
        event: LifecycleEvent,
        changes: &mut Vec<SymbolChange>,
    ) -> anyhow::Result<()> {
        if !self.follows(&event.market_ticker) {
            return Ok(());
        }
        let ticker = event.market_ticker;
        match event.event_type.as_str() {
            "created" => {
                if !self.markets.contains_key(&ticker) {
                    self.markets.insert(ticker.clone(), Market { active: false, close_ts: None });
                    self.set_close(&ticker, event.close_ts);
                }
            }
            "activated" => self.set_active(ticker, true, event.close_ts, changes),
            // Sent for both pause and unpause; `is_deactivated` says which.
            "deactivated" => {
                let paused = event.is_deactivated.unwrap_or(true);
                self.set_active(ticker, !paused, event.close_ts, changes);
            }
            "close_date_updated" => self.set_close(&ticker, event.close_ts),
            "determined" | "settled" => self.remove(&ticker, changes),
            _ => {}
        }
        Ok(())
    }

    /// Lifecycle events carry no version, but each one sets a state rather
    /// than adjusting it, so replaying one the snapshot already reflects is
    /// harmless. Replay them all.
    fn is_newer(&self, _: &LifecycleEvent) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeSet, HashSet};

    use super::super::synced::Synced;
    use super::*;

    fn event(ticker: &str, event_type: &str) -> LifecycleEvent {
        LifecycleEvent {
            market_ticker: ticker.to_owned(),
            event_type: event_type.to_owned(),
            close_ts: None,
            is_deactivated: None,
        }
    }

    fn open(tickers: &[&str]) -> OpenSet {
        OpenSet {
            fetched_at: 0,
            markets: tickers.iter().map(|t| (t.to_string(), None)).collect(),
        }
    }

    fn added(ticker: &str) -> SymbolChange {
        SymbolChange::Added(ticker.to_owned())
    }

    fn removed(ticker: &str) -> SymbolChange {
        SymbolChange::Removed(ticker.to_owned())
    }

    /// Runs `f` and returns the changes it reported, ignoring order.
    fn changes(f: impl FnOnce(&mut Vec<SymbolChange>)) -> HashSet<SymbolChange> {
        let mut out = Vec::new();
        f(&mut out);
        out.into_iter().collect()
    }

    #[test]
    fn load_adds_new_and_removes_missing() {
        let mut list = SymbolList::new(None);
        let first = changes(|c| list.load(open(&["A", "B"]), c));
        assert_eq!(first, HashSet::from([added("A"), added("B")]));

        let second = changes(|c| list.load(open(&["B", "C"]), c));
        assert_eq!(second, HashSet::from([removed("A"), added("C")]));
        assert_eq!(list.symbols(), ["B", "C"]);

        let third = changes(|c| list.load(open(&["B", "C"]), c));
        assert!(third.is_empty(), "an unchanged list reports nothing");
    }

    #[test]
    fn created_market_is_listed_only_once_activated() {
        let mut list = SymbolList::new(None);
        let created = changes(|c| list.apply(event("A", "created"), c).unwrap());
        assert!(created.is_empty() && list.symbols().is_empty());

        let activated = changes(|c| list.apply(event("A", "activated"), c).unwrap());
        assert_eq!(activated, HashSet::from([added("A")]));
        assert_eq!(list.symbols(), ["A"]);
    }

    #[test]
    fn settled_and_determined_remove() {
        let mut list = SymbolList::new(None);
        list.load(open(&["A", "B"]), &mut Vec::new());
        let out = changes(|c| {
            list.apply(event("A", "determined"), c).unwrap();
            list.apply(event("A", "settled"), c).unwrap();
            list.apply(event("B", "settled"), c).unwrap();
        });
        assert_eq!(out, HashSet::from([removed("A"), removed("B")]));
        assert!(list.symbols().is_empty());
    }

    #[test]
    fn pause_removes_and_unpause_adds_back() {
        let mut list = SymbolList::new(None);
        list.load(open(&["A"]), &mut Vec::new());

        let mut pause = event("A", "deactivated");
        pause.is_deactivated = Some(true);
        let paused = changes(|c| list.apply(pause, c).unwrap());
        assert_eq!(paused, HashSet::from([removed("A")]));

        let mut unpause = event("A", "deactivated");
        unpause.is_deactivated = Some(false);
        let unpaused = changes(|c| list.apply(unpause, c).unwrap());
        assert_eq!(unpaused, HashSet::from([added("A")]));
    }

    #[test]
    fn unopened_market_survives_a_load() {
        let mut list = SymbolList::new(None);
        list.apply(event("NEW", "created"), &mut Vec::new()).unwrap();
        list.load(open(&["A"]), &mut Vec::new());
        list.apply(event("NEW", "activated"), &mut Vec::new()).unwrap();
        assert_eq!(list.symbols(), ["A", "NEW"]);
    }

    #[test]
    fn event_held_during_the_fetch_wins_over_a_stale_rest_result() {
        // "A" settled and "NEW" opened while REST was paging, so REST still
        // lists "A" as open and has never heard of "NEW".
        let mut list = Synced::new(SymbolList::new(None));
        let mut out = Vec::new();
        list.on_delta(event("A", "settled"), &mut out).unwrap();
        list.on_delta(event("NEW", "activated"), &mut out).unwrap();
        assert!(out.is_empty(), "held until the snapshot");

        list.on_snapshot(open(&["A", "B"]), &mut out).unwrap();
        assert_eq!(list.state().symbols(), ["B", "NEW"]);
    }

    #[test]
    fn expire_removes_closed_markets_and_honours_close_date_updates() {
        let mut list = SymbolList::new(None);
        let set = OpenSet {
            fetched_at: 0,
            markets: HashMap::from([
                ("A".to_owned(), Some(100)),
                ("B".to_owned(), Some(200)),
                ("C".to_owned(), None),
            ]),
        };
        list.load(set, &mut Vec::new());
        assert_eq!(list.until_next_close(40), Duration::from_secs(60));

        let mut moved = event("A", "close_date_updated");
        moved.close_ts = Some(300);
        list.apply(moved, &mut Vec::new()).unwrap();

        let expired = changes(|c| list.expire(200, c));
        assert_eq!(expired, HashSet::from([removed("B")]));
        assert_eq!(list.symbols(), ["A", "C"]);
        assert_eq!(list.until_next_close(400), Duration::ZERO);
    }

    /// The close-time index and the count are kept beside the markets, so
    /// they must agree with them after every kind of change.
    #[test]
    fn the_close_index_and_the_count_follow_every_change() {
        fn check(list: &SymbolList) {
            let closes: BTreeSet<(i64, String)> = list
                .markets
                .iter()
                .filter_map(|(ticker, market)| Some((market.close_ts?, ticker.clone())))
                .collect();
            assert_eq!(list.closes, closes);
            assert_eq!(list.len(), list.symbols().len());
        }
        let mut list = SymbolList::new(None);
        let with_close = |ticker: &str, kind: &str, close: Option<i64>| {
            let mut event = event(ticker, kind);
            event.close_ts = close;
            event
        };
        let set = OpenSet {
            fetched_at: 0,
            markets: HashMap::from([("A".to_owned(), Some(100)), ("B".to_owned(), Some(200)), ("C".to_owned(), None)]),
        };
        list.load(set, &mut Vec::new());
        check(&list);
        let steps = [
            with_close("D", "created", Some(150)),
            with_close("D", "activated", Some(160)),
            with_close("A", "close_date_updated", Some(300)),
            with_close("B", "close_date_updated", None),
            with_close("C", "close_date_updated", Some(50)),
            with_close("D", "deactivated", None),
            with_close("A", "settled", None),
            with_close("E", "activated", None),
        ];
        for step in steps {
            list.apply(step, &mut Vec::new()).unwrap();
            check(&list);
        }
        assert_eq!(list.until_next_close(0), Duration::from_secs(50));
        list.expire(155, &mut Vec::new());
        check(&list);
        assert_eq!(list.symbols(), ["B", "E"], "C closed at 50; D is paused, and closes at 160");
        let reload = OpenSet { fetched_at: 156, markets: HashMap::from([("B".to_owned(), Some(400))]) };
        list.load(reload, &mut Vec::new());
        check(&list);
    }

    #[test]
    fn load_skips_markets_already_past_their_close_time() {
        let mut list = SymbolList::new(None);
        let set = OpenSet {
            fetched_at: 500,
            markets: HashMap::from([
                ("PAST".to_owned(), Some(499)),
                ("LIVE".to_owned(), Some(501)),
            ]),
        };
        list.load(set, &mut Vec::new());
        assert_eq!(list.symbols(), ["LIVE"]);
    }

    #[test]
    fn series_filter_ignores_other_markets() {
        let mut list = SymbolList::new(Some("KXBTC".to_owned()));
        let out = changes(|c| {
            list.apply(event("KXBTC-26NOV10-T100", "activated"), c).unwrap();
            list.apply(event("KXBTCD-26NOV10-T100", "activated"), c).unwrap();
            list.apply(event("OTHER-1", "activated"), c).unwrap();
            list.load(open(&["KXBTC-26NOV10-T100", "OTHER-2"]), c);
        });
        assert_eq!(out, HashSet::from([added("KXBTC-26NOV10-T100")]));
        assert_eq!(list.symbols(), ["KXBTC-26NOV10-T100"]);
    }
}
