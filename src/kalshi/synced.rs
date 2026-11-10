//! [`Synced`] keeps a piece of state correct from a full snapshot plus a
//! stream of deltas, whichever order they arrive in.
//!
//! The rule is the same for the symbol list and for every order book:
//!
//! * Until a snapshot has been loaded, deltas are held rather than applied.
//! * Loading a snapshot replaces the state, then replays the held deltas that
//!   are newer than it and throws the rest away.
//! * From then on deltas are applied as they arrive.
//! * [`Synced::invalidate`] (a sequence gap, a reconnect) goes back to holding
//!   until the next snapshot.

/// State that can be rebuilt from a snapshot and advanced by deltas.
pub(crate) trait Syncable {
    type Snapshot;
    type Delta;
    /// What a load or an apply reports to the outside world.
    type Change;

    /// Replaces the whole state.
    fn load(&mut self, snapshot: Self::Snapshot, changes: &mut Vec<Self::Change>);

    /// Advances the state. An error means the state can no longer be trusted.
    fn apply(
        &mut self,
        delta: Self::Delta,
        changes: &mut Vec<Self::Change>,
    ) -> anyhow::Result<()>;

    /// Whether `delta` happened after the snapshot that was last loaded, i.e.
    /// whether a delta held while waiting for that snapshot must be replayed.
    fn is_newer(&self, delta: &Self::Delta) -> bool;
}

pub(crate) struct Synced<T: Syncable> {
    state: T,
    live: bool,
    held: Vec<T::Delta>,
}

impl<T: Syncable> Synced<T> {
    /// Starts out waiting for a snapshot.
    pub(crate) fn new(state: T) -> Self {
        Self {
            state,
            live: false,
            held: Vec::new(),
        }
    }

    pub(crate) fn state(&self) -> &T {
        &self.state
    }

    pub(crate) fn state_mut(&mut self) -> &mut T {
        &mut self.state
    }

    /// Whether the state reflects a snapshot and every delta since.
    pub(crate) fn is_live(&self) -> bool {
        self.live
    }

    /// Stops trusting the state until the next snapshot. The caller is
    /// responsible for asking for that snapshot.
    pub(crate) fn invalidate(&mut self) {
        self.live = false;
    }

    /// Applies the delta if live, otherwise holds it for the next snapshot.
    /// On an error the state is invalidated and a new snapshot is needed.
    pub(crate) fn on_delta(
        &mut self,
        delta: T::Delta,
        changes: &mut Vec<T::Change>,
    ) -> anyhow::Result<()> {
        if !self.live {
            self.held.push(delta);
            return Ok(());
        }
        self.apply(delta, changes)
    }

    /// Loads the snapshot, replays the held deltas newer than it, and goes
    /// live.
    pub(crate) fn on_snapshot(
        &mut self,
        snapshot: T::Snapshot,
        changes: &mut Vec<T::Change>,
    ) -> anyhow::Result<()> {
        self.state.load(snapshot, changes);
        self.live = true;
        for delta in std::mem::take(&mut self.held) {
            if self.state.is_newer(&delta) {
                self.apply(delta, changes)?;
            }
        }
        Ok(())
    }

    fn apply(&mut self, delta: T::Delta, changes: &mut Vec<T::Change>) -> anyhow::Result<()> {
        let result = self.state.apply(delta, changes);
        if result.is_err() {
            self.live = false;
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A running total with a version, standing in for a book.
    #[derive(Default)]
    struct Total {
        value: i64,
        version: u64,
    }

    impl Syncable for Total {
        type Snapshot = (i64, u64);
        type Delta = (i64, u64);
        type Change = i64;

        fn load(&mut self, (value, version): (i64, u64), changes: &mut Vec<i64>) {
            self.value = value;
            self.version = version;
            changes.push(value);
        }

        fn apply(&mut self, (amount, _): (i64, u64), changes: &mut Vec<i64>) -> anyhow::Result<()> {
            anyhow::ensure!(self.value + amount >= 0, "total went negative");
            self.value += amount;
            changes.push(self.value);
            Ok(())
        }

        fn is_newer(&self, (_, version): &(i64, u64)) -> bool {
            *version > self.version
        }
    }

    #[test]
    fn deltas_before_the_snapshot_are_held_then_replayed_if_newer() {
        let mut total = Synced::new(Total::default());
        let mut changes = Vec::new();

        total.on_delta((5, 9), &mut changes).unwrap(); // already in the snapshot
        total.on_delta((7, 11), &mut changes).unwrap(); // after the snapshot
        assert!(changes.is_empty() && !total.is_live());

        total.on_snapshot((100, 10), &mut changes).unwrap();
        assert!(total.is_live());
        assert_eq!(changes, [100, 107]);

        total.on_delta((1, 12), &mut changes).unwrap();
        assert_eq!(total.state().value, 108);
    }

    #[test]
    fn invalidate_holds_until_the_next_snapshot() {
        let mut total = Synced::new(Total::default());
        let mut changes = Vec::new();
        total.on_snapshot((100, 10), &mut changes).unwrap();

        total.invalidate();
        total.on_delta((-30, 11), &mut changes).unwrap();
        assert_eq!(total.state().value, 100, "held, not applied");

        total.on_snapshot((70, 11), &mut changes).unwrap();
        assert_eq!(total.state().value, 70, "the snapshot already included it");
    }

    #[test]
    fn a_failed_apply_invalidates() {
        let mut total = Synced::new(Total::default());
        let mut changes = Vec::new();
        total.on_snapshot((10, 1), &mut changes).unwrap();

        assert!(total.on_delta((-11, 2), &mut changes).is_err());
        assert!(!total.is_live());

        total.on_snapshot((4, 3), &mut changes).unwrap();
        assert!(total.is_live());
        assert_eq!(total.state().value, 4);
    }
}
