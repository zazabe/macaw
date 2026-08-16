use macaw::core::RecordedEvent;
use macaw::session::{
    ProfileId, ProfileProxySnapshot, SessionEndpoint, SessionErrorCode, SessionId, SessionName,
    SessionState,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

#[derive(Debug, Deserialize, Serialize)]
pub struct HealthResponse {
    pub api_version: String,
    pub package_version: String,
    pub ready: bool,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ProfileResponse {
    pub id: ProfileId,
    pub config_root: std::path::PathBuf,
    pub proxies: BTreeMap<String, ProfileProxySnapshot>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct SessionResponse {
    pub id: SessionId,
    pub name: Option<SessionName>,
    pub profile_id: ProfileId,
    pub mode: Value,
    pub state: SessionState,
    pub proxies: BTreeMap<String, SessionEndpoint>,
    pub outcome: Option<Value>,
    pub error: Option<ErrorDetail>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ErrorResponse {
    pub error: ErrorDetail,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ErrorDetail {
    pub code: SessionErrorCode,
    pub message: String,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TrafficStreamEvent {
    Traffic {
        sequence: u64,
        #[serde(flatten)]
        event: RecordedEvent,
    },
    DroppedEvents {
        count: u64,
    },
}
