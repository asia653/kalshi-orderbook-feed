//! Follows every regular Kalshi market and writes its order book to stdout
//! as SBE messages: a `PriceLevelUpdate` for every change, and a `Snapshot`
//! of every book once a minute. Logs go to stderr.
//!
//! Configuration comes from the environment; see [`KalshiConfig::from_env`].
//! SIGINT (Ctrl-C) closes the WebSocket, then the output, and exits.

use anyhow::bail;
use engineering_challenge::kalshi::{Kalshi, KalshiConfig};
use engineering_challenge::output::{Output, STDOUT};
use tokio::signal::unix::{SignalKind, signal};

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let mut sigint = signal(SignalKind::interrupt())?;
    let (mut kalshi, events) = Kalshi::new(KalshiConfig::from_env()?)?;

    // File writes block, so they get their own thread. It ends when the
    // event channel closes, or early if a write fails.
    let output = Output::create(STDOUT)?;
    let mut writing = tokio::task::spawn_blocking(move || output.run(events));

    tokio::select! {
        // Returns once SIGINT has arrived and the WebSocket is closed.
        _ = kalshi.run(async { sigint.recv().await; }) => {}
        stopped = &mut writing => {
            stopped??;
            bail!("the output stopped while Kalshi was still running");
        }
    }
    eprintln!("shutting down: websocket closed, closing the output");

    // Dropping `kalshi` closes the event channel. The writer finishes what
    // is queued, writes its last chunk, and closes the file.
    drop(kalshi);
    writing.await??;
    Ok(())
}
