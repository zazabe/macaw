use macaw::core::ProxyConfig;
use macaw::session::{
    CreateSession, ProfileId, ProfileSnapshot, SessionConfig, SessionEndpoint, SessionError,
    SessionErrorCode, SessionMode, SessionOutcome, SessionSnapshot, SessionState,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::path::PathBuf;

pub const API_VERSION: &str = "v1";

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateSessionRequest {
    pub mode: ModeRequest,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateProfileRequest {
    pub id: String,
    #[serde(default)]
    pub config_root: PathBuf,
    pub proxies: BTreeMap<String, ProxyRequest>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ModeRequest {
    Record { output: PathBuf },
    Replay { recording: PathBuf },
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProxyRequest {
    #[serde(rename = "type")]
    implementation: String,
    config: Value,
}

#[derive(Debug)]
pub enum ModelError {
    Invalid(String),
    Unsupported(String),
}

impl CreateProfileRequest {
    pub fn into_profile(self) -> Result<(ProfileId, SessionConfig), ModelError> {
        let id = self.id.parse().map_err(ModelError::Invalid)?;
        let mut proxies = BTreeMap::new();
        for (name, proxy) in self.proxies {
            proxies.insert(name, proxy.into_proxy()?);
        }
        Ok((
            id,
            SessionConfig {
                root: self.config_root,
                proxies,
                debug_tx: None,
            },
        ))
    }
}

impl CreateSessionRequest {
    pub fn into_actor_request(self, profile_id: ProfileId) -> CreateSession {
        let mode = match self.mode {
            ModeRequest::Record { output } => SessionMode::Record { output },
            ModeRequest::Replay { recording } => SessionMode::Replay { recording },
        };
        CreateSession::new(profile_id, mode)
    }
}

impl ProxyRequest {
    fn into_proxy(self) -> Result<Box<dyn ProxyConfig>, ModelError> {
        let mut config = match self.config {
            Value::Object(config) => config,
            _ => {
                return Err(ModelError::Invalid(
                    "proxy config must be a JSON object".to_owned(),
                ));
            }
        };
        config.insert("type".to_owned(), Value::String(self.implementation));
        let proxy = serde_json::from_value::<Box<dyn ProxyConfig>>(Value::Object(config)).map_err(
            |error| {
                if error.to_string().contains("unknown variant") {
                    ModelError::Unsupported(
                        "proxy implementation is not supported by this build".to_owned(),
                    )
                } else {
                    ModelError::Invalid("invalid proxy configuration".to_owned())
                }
            },
        )?;
        proxy.validate(false).map_err(ModelError::Invalid)?;
        Ok(proxy)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[cfg(feature = "http")]
    #[test]
    fn opaque_proxy_config_deserializes_through_trait_object() {
        let request: CreateProfileRequest = serde_json::from_value(json!({
            "id": "test",
            "proxies": {
                "api": {
                    "type": "http",
                    "config": {
                        "bind": "127.0.0.1:0",
                        "target": "https://example.com"
                    }
                }
            }
        }))
        .unwrap();
        let (_, config) = request.into_profile().unwrap();
        let proxy = config.proxies.get("api").unwrap();
        assert_eq!(proxy.protocol(), "http");
        assert_eq!(proxy.target(), Some("https://example.com"));
    }

    #[test]
    fn proxy_envelope_rejects_non_object_config() {
        let request: CreateProfileRequest = serde_json::from_value(json!({
            "id": "test",
            "proxies": {
                "api": {"type": "anything", "config": "not-an-object"}
            }
        }))
        .unwrap();
        assert!(matches!(
            request.into_profile(),
            Err(ModelError::Invalid(_))
        ));
    }

    #[test]
    fn proxy_envelope_rejects_unknown_fields() {
        let result = serde_json::from_value::<CreateProfileRequest>(json!({
            "id": "test",
            "proxies": {
                "api": {
                    "type": "anything",
                    "config": {},
                    "unexpected": true
                }
            }
        }));
        assert!(result.is_err());
    }
}

#[derive(Debug, Serialize)]
pub struct HealthResponse {
    pub api_version: &'static str,
    pub package_version: &'static str,
    pub ready: bool,
}

#[derive(Debug, Serialize)]
pub struct ProfileResponse {
    pub id: ProfileId,
}

impl From<ProfileSnapshot> for ProfileResponse {
    fn from(profile: ProfileSnapshot) -> Self {
        Self { id: profile.id }
    }
}

#[derive(Debug, Serialize)]
pub struct SessionResponse {
    pub id: macaw::session::SessionId,
    pub profile_id: ProfileId,
    pub mode: ModeResponse,
    pub state: SessionState,
    pub proxies: BTreeMap<String, SessionEndpoint>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub outcome: Option<OutcomeResponse>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<ErrorDetail>,
}

#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ModeResponse {
    Record { output: PathBuf },
    Replay { recording: PathBuf },
}

#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum OutcomeResponse {
    Record {
        recording_path: PathBuf,
        total_events: usize,
        total_bytes: Option<usize>,
        total_time_millis: Option<u64>,
    },
    Replay,
}

impl From<SessionSnapshot> for SessionResponse {
    fn from(snapshot: SessionSnapshot) -> Self {
        let mode = match snapshot.mode {
            SessionMode::Record { output } => ModeResponse::Record { output },
            SessionMode::Replay { recording } => ModeResponse::Replay { recording },
        };
        let outcome = snapshot.outcome.map(|outcome| match outcome {
            SessionOutcome::Record {
                recording_path,
                total_events,
                total_bytes,
                total_time_millis,
            } => OutcomeResponse::Record {
                recording_path,
                total_events,
                total_bytes,
                total_time_millis,
            },
            SessionOutcome::Replay => OutcomeResponse::Replay,
        });
        Self {
            id: snapshot.id,
            profile_id: snapshot.profile_id,
            mode,
            state: snapshot.state,
            proxies: snapshot.endpoints,
            outcome,
            error: snapshot.error.as_ref().map(ErrorDetail::from_session),
        }
    }
}

#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    pub error: ErrorDetail,
}

#[derive(Debug, Serialize)]
pub struct ErrorDetail {
    pub code: SessionErrorCode,
    pub message: String,
}

impl ErrorDetail {
    pub fn from_session(error: &SessionError) -> Self {
        Self {
            code: error.code,
            message: safe_error_message(error),
        }
    }
}

fn safe_error_message(error: &SessionError) -> String {
    match error.code {
        SessionErrorCode::InvalidConfig
        | SessionErrorCode::NotFound
        | SessionErrorCode::Duplicate
        | SessionErrorCode::NotTerminal
        | SessionErrorCode::Unsupported
        | SessionErrorCode::ShuttingDown => error.message.clone(),
        SessionErrorCode::StartupFailed => "session startup failed".to_owned(),
        SessionErrorCode::RuntimeFailed => "session runtime failed".to_owned(),
        SessionErrorCode::ActorUnavailable => "session manager unavailable".to_owned(),
    }
}
