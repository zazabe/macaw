use itertools::Itertools;
use macaw_core::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct WsUpstreamMessageEvent {
    message: Content,
}

#[typetag::serde]
impl RecordEvent for WsUpstreamMessageEvent {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct WsDownstreamMessageEvent {
    message: Content,
}

#[typetag::serde]
impl RecordEvent for WsDownstreamMessageEvent {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct WsOpenEvent {
    pub(crate) request: HttpRequest,
}

#[typetag::serde]
impl RecordEvent for WsOpenEvent {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct WsCloseEvent {
    pub(crate) reason: String,
    pub(crate) code: u16,
}

#[typetag::serde]
impl RecordEvent for WsCloseEvent {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HttpRequest {
    pub(crate) method: String,
    pub(crate) uri: String,
    pub(crate) version: String,
    pub(crate) headers: Vec<(String, String)>,
}

impl HttpRequest {
    pub(crate) fn from_request(
        req: &tokio_tungstenite::tungstenite::handshake::client::Request,
    ) -> Result<Self, anyhow::Error> {
        Ok(Self {
            method: req.method().to_string(),
            uri: req.uri().to_string(),
            version: match req.version() {
                http::Version::HTTP_09 => "HTTP/0.9",
                http::Version::HTTP_10 => "HTTP/1.0",
                http::Version::HTTP_11 => "HTTP/1.1",
                http::Version::HTTP_2 => "HTTP/2",
                http::Version::HTTP_3 => "HTTP/3",
                _ => return Err(anyhow::anyhow!("Unsupported HTTP version")),
            }
            .to_string(),
            headers: req
                .headers()
                .iter()
                .map(|(k, v)| Ok::<_, anyhow::Error>((k.to_string(), v.to_str()?.to_string())))
                .try_collect()?,
        })
    }
}
