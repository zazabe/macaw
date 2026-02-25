//! Replay command - load recording and replay through proxies.

use anyhow::{Context, Result};
use macaw::core::*;
use std::collections::HashMap;
use std::path::Path;

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
    let options = ReplayerOptions { debug_tx };

    let mut macaw = Macaw::<Replayer>::replayer_with_options(recording_path, options)?;
    let mut bindings = HashMap::new();
    for (proxy_id, proxy_config) in config.proxies.iter() {
        let socket = proxy_config
            .bind_to_replayer(proxy_id, &mut macaw)
            .await
            .context("Failed to bind proxy to recorder")?;
        bindings.insert(proxy_id.clone(), socket);
    }

    debug::print_replay_summary(&config.proxies, &bindings);

    macaw.play()?;
    macaw.wait_until_stopped().await?;
    Ok(())
}
