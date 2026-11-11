//! Prints every event (symbol changes, snapshots, level updates) as text,
//! for watching what the service sees. Runs until killed.
//!
//! Takes the same flags and environment variables as the service itself;
//! run with `--help`.

use clap::Parser;
use engineering_challenge::kalshi::{Kalshi, KalshiConfig, Options};

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    engineering_challenge::logging::init();
    let (mut kalshi, changes) = Kalshi::new(KalshiConfig::from_options(Options::parse())?)?;
    std::thread::spawn(move || {
        for change in changes {
            println!("{change:?}");
        }
    });
    kalshi.run(std::future::pending()).await;
    Ok(())
}
