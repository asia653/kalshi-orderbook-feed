//! Follows every regular Kalshi market and writes its order book to stdout
//! as SBE messages: a `PriceLevelUpdate` for every change, and a `Snapshot`
//! of every book once a minute. Logs go to stderr.
//!
//! Configuration comes from the environment; see [`KalshiConfig::from_env`].
//! SIGINT (Ctrl-C) closes the WebSockets, then the output, and exits.
//! SIGUSR1 cuts the source connection, to watch the standby take over.

use anyhow::bail;
use engineering_challenge::kalshi::{Kalshi, KalshiConfig};
use engineering_challenge::output::{Output, STDOUT};
use tokio::signal::unix::{SignalKind, signal};

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let mut sigint = signal(SignalKind::interrupt())?;
    let sigusr1 = signal(SignalKind::user_defined1())?;
    let drop_source = Box::pin(futures_util::stream::unfold(sigusr1, |mut signal| async move {
        signal.recv().await.map(|()| ((), signal))
    }));
    let (mut kalshi, events) = Kalshi::new(KalshiConfig::from_env()?)?;

    // File writes block, so they get their own thread. It ends when the
    // event channel closes, or early if a write fails.
    let output = Output::create(STDOUT)?;
    let mut writing = tokio::task::spawn_blocking(move || output.run(events));

    tokio::select! {
        // Returns once SIGINT has arrived and the WebSockets are closed.
        _ = kalshi.run_with_faults(async { sigint.recv().await; }, drop_source) => {}
        stopped = &mut writing => {
            stopped??;
            bail!("the output stopped while Kalshi was still running");
        }
    }
    eprintln!("shutting down: websockets closed, closing the output");

    // Dropping `kalshi` closes the event channel. The writer finishes what
    // is queued, writes its last chunk, and closes the file.
    drop(kalshi);
    writing.await??;
    Ok(())
}
