//! Reads a file the service wrote and checks that it is self-consistent.
//!
//! ```text
//! cargo run --release --example inspect_output -- out.bin
//! ```
//!
//! It rebuilds every book from the file alone (load each `Snapshot`, apply
//! each `PriceLevelUpdate`) and, whenever a symbol's next snapshot arrives,
//! compares it with the rebuilt book.
//!
//! Snapshots come in two kinds, told apart by their timestamps:
//!
//! * a *round* is a snapshot of every book at once, all stamped with the
//!   same `received_at`. The once-a-minute round must match the rebuilt book
//!   exactly. A round is also written when the service switches to another
//!   connection as its source; that one may differ, which is why it is
//!   written.
//! * a *fetched* snapshot came from Kalshi (first subscribe, or a re-fetch
//!   after a sequence gap). It may differ: that is why it was fetched.

use std::collections::{BTreeMap, HashMap};

use chrono::{DateTime, Utc};
use engineering_challenge::Side;
use engineering_challenge::util::dtos::challenge_orderbook::{
    AnyMessage, PriceLevelUpdateDomain, SnapshotDomain,
};
use engineering_challenge::util::{AlignedSbeReader, OrderbookSchema};

type Levels = BTreeMap<i64, i64>;

#[derive(Default, PartialEq)]
struct Book {
    bids: Levels,
    asks: Levels,
}

/// A run of consecutive snapshots of the same kind.
struct Run {
    /// Updates written between the previous run and this one.
    updates_before: usize,
    round: bool,
    started: DateTime<Utc>,
    snapshots: usize,
    first_seen: usize,
    matched: usize,
    differed: usize,
}

fn main() -> anyhow::Result<()> {
    let path = std::env::args().nth(1).unwrap_or_else(|| "out.bin".to_owned());
    let mut reader = AlignedSbeReader::<OrderbookSchema>::open(&path)?;

    let mut snapshots = Vec::new();
    let mut messages = Vec::new();
    while let Some(message) = reader.read_msg()? {
        match message {
            AnyMessage::Snapshot(decoded) => {
                snapshots.push(SnapshotDomain::try_from_decoder(decoded)?);
                messages.push(Ok(snapshots.len() - 1));
            }
            AnyMessage::PriceLevelUpdate(decoded) => {
                messages.push(Err(PriceLevelUpdateDomain::try_from_decoder(decoded)?));
            }
            _ => anyhow::bail!("a message type this tool does not know"),
        }
    }
    // A round is a `received_at` shared by more than one snapshot.
    let mut shared: HashMap<DateTime<Utc>, usize> = HashMap::new();
    for snapshot in &snapshots {
        *shared.entry(snapshot.received_at).or_default() += 1;
    }

    let mut books: HashMap<String, Book> = HashMap::new();
    let mut runs: Vec<Run> = Vec::new();
    let mut updates_since_run = 0usize;
    let mut total_updates = 0usize;
    let mut unknown_symbol = 0usize;
    let mut bad_round = Vec::new();
    let mut crossed = 0usize;

    for message in messages {
        match message {
            Err(update) => {
                total_updates += 1;
                updates_since_run += 1;
                let Some(book) = books.get_mut(&update.symbol) else {
                    unknown_symbol += 1;
                    continue;
                };
                let side = match update.side {
                    Side::Buy => &mut book.bids,
                    Side::Sell => &mut book.asks,
                };
                if update.quantity == 0 {
                    side.remove(&update.price_level);
                } else {
                    side.insert(update.price_level, update.quantity);
                }
            }
            Ok(index) => {
                let snapshot = &snapshots[index];
                let round = shared[&snapshot.received_at] > 1;
                let fresh = Book {
                    bids: snapshot.bids.iter().map(|l| (l.price_level, l.quantity)).collect(),
                    asks: snapshot.asks.iter().map(|l| (l.price_level, l.quantity)).collect(),
                };
                if let (Some(bid), Some(ask)) = (fresh.bids.keys().next_back(), fresh.asks.keys().next()) {
                    crossed += usize::from(bid >= ask);
                }
                let continues = runs.last().is_some_and(|run| {
                    run.round == round
                        && updates_since_run == 0
                        && (!round || run.started == snapshot.received_at)
                });
                if !continues {
                    runs.push(Run {
                        updates_before: updates_since_run,
                        round,
                        started: snapshot.received_at,
                        snapshots: 0,
                        first_seen: 0,
                        matched: 0,
                        differed: 0,
                    });
                }
                updates_since_run = 0;
                let run = runs.last_mut().expect("just pushed");
                run.snapshots += 1;
                match books.get(&snapshot.symbol) {
                    None => run.first_seen += 1,
                    Some(rebuilt) if *rebuilt == fresh => run.matched += 1,
                    Some(_) => {
                        run.differed += 1;
                        if round {
                            bad_round.push(snapshot.symbol.clone());
                        }
                    }
                }
                books.insert(snapshot.symbol.clone(), fresh);
            }
        }
    }
    for run in &runs {
        if run.updates_before > 0 {
            println!("             {} updates", run.updates_before);
        }
        println!(
            "{} {:<7} {:>5} snapshots: {:>5} new, {:>5} equal to the rebuilt book, {:>5} different",
            run.started.format("%H:%M:%S%.3f"),
            if run.round { "round" } else { "fetched" },
            run.snapshots,
            run.first_seen,
            run.matched,
            run.differed,
        );
    }
    if updates_since_run > 0 {
        println!("             {updates_since_run} updates");
    }
    println!(
        "\n{} snapshots, {total_updates} updates, {} symbols",
        snapshots.len(),
        books.len()
    );
    println!("updates for a symbol with no snapshot yet: {unknown_symbol}");
    println!("snapshots whose best bid is at or above the best ask: {crossed}");
    println!(
        "snapshots in a round that disagree with the rebuilt book: {} (expected only after a switch of source)",
        bad_round.len()
    );
    for symbol in bad_round.iter().take(10) {
        println!("  {symbol}");
    }
    Ok(())
}
