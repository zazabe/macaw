//! Record command - create proxies and record traffic.

use anyhow::{Context, Result};
use futures::StreamExt;
use macaw::core::*;
use std::collections::HashMap;
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

    let options = RecorderOptions { debug_tx };
    let mut macaw = Macaw::<Recorder>::recorder_with_options(options);
    let mut bindings = HashMap::new();
    for (proxy_id, proxy_config) in config.proxies.iter() {
        let socket = proxy_config
            .bind_to_recorder(proxy_id, &mut macaw)
            .await
            .context("Failed to bind proxy to recorder")?;
        bindings.insert(proxy_id.clone(), socket);
    }

    debug::print_record_summary(&config.proxies, &bindings);

    let exit_handle = macaw.exit_handle();
    tokio::spawn(async move {
        let mut sig_int = SignalStream::new(signal(SignalKind::interrupt()).unwrap());
        let mut sig_term = SignalStream::new(signal(SignalKind::terminate()).unwrap());
        let mut sig_quit = SignalStream::new(signal(SignalKind::quit()).unwrap());
        tokio::select! {
            _ = sig_int.next() => {}
            _ = sig_term.next() => {}
            _ = sig_quit.next() => {}
        };
        info!("Signal received. Saving recording...");
        exit_handle.exit();
    });

    let outcome = macaw.record_when_exit(output_path).await?;
    debug::print_record_outcome(outcome);
    Ok(())
}
