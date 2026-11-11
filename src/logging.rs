//! Log output, set up the same way by the binary and the examples.

use std::io::IsTerminal;

use tracing_subscriber::EnvFilter;

/// Sends log lines to stderr, each with the time and a level. Never to
/// stdout: that carries the data.
///
/// Lines at `info` and above are shown. `RUST_LOG` overrides that, e.g.
/// `RUST_LOG=warn` to leave out the status lines.
pub fn init() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        // Colour codes only when a person is looking, not in a log file.
        .with_ansi(std::io::stderr().is_terminal())
        .with_target(false)
        .init();
}
