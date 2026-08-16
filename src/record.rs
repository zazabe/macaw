//! Record command - create proxies and record traffic.

use anyhow::Result;
use futures::StreamExt;
use macaw::core::RecorderOutcome;
use macaw::session::{SessionManager, SessionOutcome};
use std::path::Path;
use tokio::signal::unix::{SignalKind, signal};
use tokio_stream::wrappers::SignalStream;
use tracing::info;

use crate::config::Config;
use crate::debug;

pub async fn run(config_path: &Path, output_path: &Path, debug_mode: bool) -> Result<()> {
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
        .record(output_path, config.session_config(debug_tx))
        .await?;
    let bindings = snapshot
        .endpoints
        .iter()
        .map(|(name, endpoint)| (name.clone(), endpoint.address))
        .collect();
    debug::print_record_summary(&config.proxies, &bindings);

    let mut sig_int = SignalStream::new(signal(SignalKind::interrupt())?);
    let mut sig_term = SignalStream::new(signal(SignalKind::terminate())?);
    let mut sig_quit = SignalStream::new(signal(SignalKind::quit())?);
    tokio::select! {
        _ = sig_int.next() => {}
        _ = sig_term.next() => {}
        _ = sig_quit.next() => {}
    };
    info!("Signal received. Saving recording...");
    let final_snapshot = manager.stop_session(snapshot.id).await?;
    let final_error = final_snapshot.error.clone();
    if let Some(SessionOutcome::Record {
        recording_path,
        total_events,
        total_bytes,
        total_time_millis,
    }) = final_snapshot.outcome
    {
        debug::print_record_outcome(RecorderOutcome {
            recording_path,
            total_events,
            total_bytes,
            total_time: total_time_millis.map(std::time::Duration::from_millis),
        });
    }
    manager.shutdown().await?;
    if let Some(error) = final_error {
        return Err(error.into());
    }
    Ok(())
}
