use macaw_core::prelude::ProxyConfig;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ProfileId(String);

impl ProfileId {
    pub fn new(value: impl Into<String>) -> Result<Self, String> {
        let value = value.into();
        if value.is_empty() || value.len() > 64 {
            return Err("profile id must contain between 1 and 64 characters".to_owned());
        }
        if matches!(value.as_str(), "." | "..") {
            return Err("profile id must not be '.' or '..'".to_owned());
        }
        if !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
        {
            return Err(
                "profile id may contain only ASCII letters, digits, '-', '_', and '.'".to_owned(),
            );
        }
        Ok(Self(value))
    }
}

impl fmt::Display for ProfileId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl FromStr for ProfileId {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProfileSnapshot {
    pub id: ProfileId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SessionId(Uuid);

impl SessionId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for SessionId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for SessionId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl FromStr for SessionId {
    type Err = uuid::Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        value.parse().map(Self)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum SessionMode {
    Record { output: PathBuf },
    Replay { recording: PathBuf },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionConfig {
    #[serde(default)]
    pub root: PathBuf,
    #[serde(default)]
    pub proxies: BTreeMap<String, Box<dyn ProxyConfig>>,
    #[serde(skip)]
    pub debug_tx: Option<tokio::sync::mpsc::UnboundedSender<macaw_core::prelude::RecordedEvent>>,
}

impl Default for SessionConfig {
    fn default() -> Self {
        Self {
            root: PathBuf::new(),
            proxies: BTreeMap::new(),
            debug_tx: None,
        }
    }
}

impl SessionConfig {
    pub fn resolve_path(&self, path: &Path) -> PathBuf {
        if path.is_absolute() {
            path.to_path_buf()
        } else {
            self.root.join(path)
        }
    }

    pub(crate) fn resolve_proxy_paths(&mut self) {
        for proxy in self.proxies.values_mut() {
            proxy.set_root_path(&self.root);
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionState {
    Starting,
    Running,
    Stopping,
    Stopped,
    Failed,
}

impl SessionState {
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Stopped | Self::Failed)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionEndpoint {
    pub protocol: String,
    pub address: SocketAddr,
    pub url: String,
}

impl SessionEndpoint {
    pub(crate) fn new(protocol: impl Into<String>, address: SocketAddr) -> Self {
        let protocol = protocol.into();
        let url = format!("{protocol}://{address}");
        Self {
            protocol,
            address,
            url,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionSnapshot {
    pub id: SessionId,
    pub profile_id: ProfileId,
    pub mode: SessionMode,
    pub state: SessionState,
    pub endpoints: BTreeMap<String, SessionEndpoint>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub outcome: Option<SessionOutcome>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<SessionError>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum SessionOutcome {
    Record {
        recording_path: PathBuf,
        total_events: usize,
        total_bytes: Option<usize>,
        total_time_millis: Option<u64>,
    },
    Replay,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionErrorCode {
    InvalidConfig,
    StartupFailed,
    RuntimeFailed,
    NotFound,
    Duplicate,
    NotTerminal,
    ActorUnavailable,
    Unsupported,
    ShuttingDown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[error("{code:?}: {message}")]
pub struct SessionError {
    pub code: SessionErrorCode,
    pub message: String,
}

impl SessionError {
    pub(crate) fn new(code: SessionErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: sanitize(message.into()),
        }
    }

    pub(crate) fn invalid(message: impl Into<String>) -> Self {
        Self::new(SessionErrorCode::InvalidConfig, message)
    }

    pub(crate) fn startup(message: impl Into<String>) -> Self {
        Self::new(SessionErrorCode::StartupFailed, message)
    }

    pub(crate) fn runtime(message: impl Into<String>) -> Self {
        Self::new(SessionErrorCode::RuntimeFailed, message)
    }

    pub(crate) fn actor(error: anyhow::Error) -> Self {
        Self::new(SessionErrorCode::ActorUnavailable, error.to_string())
    }
}

fn sanitize(message: String) -> String {
    const MAX_ERROR_LENGTH: usize = 512;
    let normalized = message.replace(['\r', '\n'], " ");
    normalized.chars().take(MAX_ERROR_LENGTH).collect()
}

#[cfg(test)]
mod tests {
    use super::ProfileId;

    #[test]
    fn profile_ids_are_safe_for_urls_and_filenames() {
        for valid in ["local", "payment-api", "staging_v2", "api.example"] {
            assert!(ProfileId::new(valid).is_ok());
        }
        for invalid in ["", ".", "..", "contains space", "contains/slash"] {
            assert!(ProfileId::new(invalid).is_err());
        }
    }
}
