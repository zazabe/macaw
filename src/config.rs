//! TOML configuration for macaw proxies.

use anyhow::{Context, Result};
use macaw::core::ProxyConfig;
use macaw::session::SessionConfig;
use serde::Deserialize;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

/// Root configuration structure.
#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub proxies: ProxyMap,
    #[serde(skip)]
    root: PathBuf,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ProxyMap(BTreeMap<String, Box<dyn ProxyConfig>>);

impl ProxyMap {
    pub fn max_name_length(&self) -> usize {
        self.0.keys().map(|k| k.len()).max().unwrap_or(0)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&String, &Box<dyn ProxyConfig>)> {
        self.0.iter()
    }
}

impl Config {
    /// Load configuration from a TOML file.
    pub fn from_file(path: &Path) -> Result<Self> {
        let content = std::fs::read_to_string(path)
            .with_context(|| format!("Failed to read config file: {}", path.display()))?;
        let mut config: Config = toml::from_str(&content)
            .with_context(|| format!("Failed to parse config: {}", path.display()))?;
        config.root = path.parent().unwrap_or_else(|| Path::new("")).to_path_buf();
        Ok(config)
    }

    pub fn session_config(
        &self,
        debug_tx: Option<tokio::sync::mpsc::UnboundedSender<macaw::core::RecordedEvent>>,
    ) -> SessionConfig {
        SessionConfig {
            root: self.root.clone(),
            proxies: self.proxies.0.clone(),
            debug_tx,
        }
    }
}
