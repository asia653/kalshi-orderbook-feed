//! Prints every event (symbol changes, snapshots, level updates) as text,
//! for watching what the service sees. Runs until killed.
//!
//! Needs `KALSHI_API_KEY_ID` and `KALSHI_PRIVATE_KEY_PATH`; see
//! [`KalshiConfig::from_env`] for the optional variables.

use engineering_challenge::kalshi::{Kalshi, KalshiConfig};

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let (mut kalshi, changes) = Kalshi::new(KalshiConfig::from_env()?)?;
    std::thread::spawn(move || {
        for change in changes {
            println!("{change:?}");
        }
    });
    kalshi.run(std::future::pending()).await;
    Ok(())
}
