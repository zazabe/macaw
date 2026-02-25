//! TOML configuration for macaw proxies.

use anyhow::{Context, Result};
use macaw::core::*;
use serde::Deserialize;
use std::collections::BTreeMap;
use std::path::Path;

/// Root configuration structure.
#[derive(Debug, Deserialize)]
pub struct Config {
    pub proxies: ProxyMap,
}

#[derive(Debug, Deserialize)]
pub struct ProxyMap(BTreeMap<String, Box<dyn ProxyConfig>>);

impl ProxyMap {
    pub fn max_name_length(&self) -> usize {
        self.0.keys().map(|k| k.len()).max().unwrap_or(0)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&String, &Box<dyn ProxyConfig>)> {
        self.0.iter()
    }

    fn set_root_path(&mut self, path: &Path) {
        for proxy in self.0.values_mut() {
            proxy.set_root_path(path);
        }
    }
}

impl Config {
    /// Load configuration from a TOML file.
    pub fn from_file(path: &Path) -> Result<Self> {
        let content = std::fs::read_to_string(path)
            .with_context(|| format!("Failed to read config file: {}", path.display()))?;
        let mut config: Config = toml::from_str(&content)
            .with_context(|| format!("Failed to parse config: {}", path.display()))?;
        let root_path = path.parent().unwrap_or_else(|| Path::new(""));
        config.proxies.set_root_path(root_path);
        Ok(config)
    }
}
