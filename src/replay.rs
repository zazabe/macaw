//! Replay command - load recording and replay through proxies.

use anyhow::Result;
use futures::StreamExt;
use macaw::session::SessionManager;
use std::path::Path;
use tokio::signal::unix::{SignalKind, signal};
use tokio_stream::wrappers::SignalStream;

use crate::config::Config;
use crate::debug;

pub async fn run(config_path: &Path, recording_path: &Path, debug_mode: bool) -> Result<()> {
    let config = Config::from_file(config_path)?;

    let debug_tx = if debug_mode {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let proxy_width = config.proxies.max_name_length();
        debug::spawn_debug_printer(rx, proxy_width);

        Some(tx)
    } else {
        None
    };
    let manager = SessionManager::start();
    let snapshot = manager
        .replay(recording_path, config.session_config(debug_tx))
        .await?;
    let bindings = snapshot.endpoints.clone().into_iter().collect();
    debug::print_replay_summary(&config.proxies, &bindings);

    let mut sig_int = SignalStream::new(signal(SignalKind::interrupt())?);
    let mut sig_term = SignalStream::new(signal(SignalKind::terminate())?);
    let mut sig_quit = SignalStream::new(signal(SignalKind::quit())?);
    let final_snapshot = loop {
        tokio::select! {
            _ = sig_int.next() => {
                break manager.stop_session(snapshot.id).await?;
            }
            _ = sig_term.next() => {
                break manager.stop_session(snapshot.id).await?;
            }
            _ = sig_quit.next() => {
                break manager.stop_session(snapshot.id).await?;
            }
            _ = tokio::time::sleep(std::time::Duration::from_millis(25)) => {
                let current = manager.get(snapshot.id).await?;
                if current.state.is_terminal() {
                    break current;
                }
            }
        }
    };
    let final_error = final_snapshot.error;
    manager.shutdown().await?;
    if let Some(error) = final_error {
        return Err(error.into());
    }
    Ok(())
}
