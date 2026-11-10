//! [`Output`] turns [`Event`]s into SBE messages and writes them with the
//! challenge's [`AlignedSbeWriter`].
//!
//! * [`Event::Snapshot`] becomes a `Snapshot` message.
//! * [`Event::Level`] becomes a `PriceLevelUpdate` message.
//! * [`Event::Symbol`] is not written: the schema has no message for it.
//!
//! The writer works in fixed 64 MiB chunks, so bytes reach the file when a
//! chunk fills up and when the output is closed, not after every message.

use std::path::Path;

use crossbeam::channel::Receiver;

use crate::kalshi::Event;
use crate::util::AlignedSbeWriter;
use crate::util::dtos::challenge_orderbook::{
    PriceLevelUpdateDomain, SnapshotAsksEntryDomain, SnapshotBidsEntryDomain, SnapshotDomain,
};

/// Where the service writes. `AlignedSbeWriter` takes a path, and this one
/// is the process's standard output.
pub const STDOUT: &str = "/dev/stdout";

/// The chunk size `AlignedSbeReader::open` expects.
const CHUNK_SIZE_64_MEBIBYTES: usize = 64 << 20;

pub struct Output {
    writer: AlignedSbeWriter,
}

impl Output {
    /// Opens `path` for writing, clearing it if it is a regular file.
    pub fn create(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        Ok(Self {
            writer: AlignedSbeWriter::create(path, CHUNK_SIZE_64_MEBIBYTES)?,
        })
    }

    /// Writes every event until the sending side is dropped, then closes.
    ///
    /// Any error ends the run: after a failed write the writer cannot be
    /// trusted to produce a well-formed file.
    pub fn run(mut self, events: Receiver<Event>) -> anyhow::Result<()> {
        for event in events {
            self.write(event)?;
        }
        self.close()
    }

    pub fn write(&mut self, event: Event) -> anyhow::Result<()> {
        match event {
            Event::Symbol(_) => Ok(()),
            Event::Snapshot { symbol, bids, asks, received_at, exchange_timestamp } => {
                let bids = bids
                    .into_iter()
                    .map(|(price_level, quantity)| SnapshotBidsEntryDomain { price_level, quantity })
                    .collect();
                let asks = asks
                    .into_iter()
                    .map(|(price_level, quantity)| SnapshotAsksEntryDomain { price_level, quantity })
                    .collect();
                self.writer.write_msg(&SnapshotDomain {
                    received_at,
                    exchange_timestamp,
                    bids,
                    asks,
                    symbol,
                })
            }
            Event::Level {
                symbol,
                side,
                price_level,
                quantity,
                received_at,
                exchange_timestamp,
            } => self.writer.write_msg(&PriceLevelUpdateDomain {
                // The time we received the update is the time a backtest may
                // first act on it.
                backtest_timestamp: received_at,
                exchange_timestamp,
                price_level,
                quantity,
                side,
                symbol,
            }),
        }
    }

    /// Writes out the last, partly filled chunk and closes the file.
    pub fn close(mut self) -> anyhow::Result<()> {
        self.writer.flush()
        // Dropping the writer closes the file.
    }
}

#[cfg(test)]
mod tests {
    use chrono::DateTime;

    use super::*;
    use crate::Side;
    use crate::kalshi::SymbolChange;
    use crate::util::dtos::challenge_orderbook::AnyMessage;
    use crate::util::{AlignedSbeReader, OrderbookSchema};

    #[test]
    fn events_read_back_as_the_matching_sbe_messages() {
        let received_at = DateTime::from_timestamp_micros(1_700_000_000_123_456).unwrap();
        let exchange_timestamp = DateTime::from_timestamp_millis(1_700_000_000_000).unwrap();
        let (sender, receiver) = crossbeam::channel::unbounded();
        sender.send(Event::Symbol(SymbolChange::Added("A".to_owned()))).unwrap();
        sender
            .send(Event::Snapshot {
                symbol: "A".to_owned(),
                bids: vec![(400_000, 20_000_000), (80_000, 300_000_000)],
                asks: vec![(420_000, 10_000_000)],
                received_at,
                exchange_timestamp,
            })
            .unwrap();
        sender
            .send(Event::Level {
                symbol: "A".to_owned(),
                side: Side::Sell,
                price_level: 420_000,
                quantity: 0,
                received_at,
                exchange_timestamp,
            })
            .unwrap();
        drop(sender);

        let file = tempfile::NamedTempFile::new().unwrap();
        Output::create(file.path()).unwrap().run(receiver).unwrap();

        let mut reader = AlignedSbeReader::<OrderbookSchema>::open(file.path()).unwrap();
        let Some(AnyMessage::Snapshot(decoded)) = reader.read_msg().unwrap() else {
            panic!("expected a snapshot first; the symbol event writes nothing");
        };
        assert_eq!(
            SnapshotDomain::try_from_decoder(decoded).unwrap(),
            SnapshotDomain {
                received_at,
                exchange_timestamp,
                bids: vec![
                    SnapshotBidsEntryDomain { price_level: 400_000, quantity: 20_000_000 },
                    SnapshotBidsEntryDomain { price_level: 80_000, quantity: 300_000_000 },
                ],
                asks: vec![SnapshotAsksEntryDomain { price_level: 420_000, quantity: 10_000_000 }],
                symbol: "A".to_owned(),
            }
        );
        let Some(AnyMessage::PriceLevelUpdate(decoded)) = reader.read_msg().unwrap() else {
            panic!("expected a price level update");
        };
        assert_eq!(
            PriceLevelUpdateDomain::try_from_decoder(decoded).unwrap(),
            PriceLevelUpdateDomain {
                backtest_timestamp: received_at,
                exchange_timestamp,
                price_level: 420_000,
                quantity: 0,
                side: Side::Sell,
                symbol: "A".to_owned(),
            }
        );
        assert!(reader.read_msg().unwrap().is_none());
    }
}
