use tokio_tungstenite::tungstenite::{
    self,
    handshake::client::Request,
    protocol::{CloseFrame, frame::coding::CloseCode},
};

use crate::lib::*;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct WsUpstreamEvent(WsEvent);

#[typetag::serde(name = "WsUpstream")]
impl RecordEvent for WsUpstreamEvent {}

impl WsUpstreamEvent {
    pub(crate) fn new(event: WsEvent) -> Self {
        Self(event)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct WsDownstreamEvent(WsEvent);

#[typetag::serde(name = "WsDownstream")]
impl RecordEvent for WsDownstreamEvent {}

impl WsDownstreamEvent {
    pub(crate) fn new(event: WsEvent) -> Self {
        Self(event)
    }
}

// ------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum WsEvent {
    Message(WsDownstreamMessageEvent),
    Open(WsOpenEvent),
    Disconnect,
}

impl WsEvent {
    pub(crate) fn open(request: HttpRequest) -> Self {
        Self::Open(WsOpenEvent { request })
    }

    pub(crate) fn message(message: WsMessage) -> Self {
        Self::Message(WsDownstreamMessageEvent { message })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum WsMessage {
    Text(String),
    Binary(Bytes),
    Ping(Bytes),
    Pong(Bytes),
    Close(Option<(u16, String)>),
}

impl From<WsMessage> for tungstenite::Message {
    fn from(message: WsMessage) -> Self {
        match message {
            WsMessage::Text(text) => tungstenite::Message::from(text),
            WsMessage::Binary(binary) => tungstenite::Message::from(binary),
            WsMessage::Ping(ping) => tungstenite::Message::Ping(ping),
            WsMessage::Pong(pong) => tungstenite::Message::Pong(pong),
            WsMessage::Close(close) => {
                tungstenite::Message::Close(close.map(|(code, reason)| CloseFrame {
                    code: CloseCode::from(code),
                    reason: reason.into(),
                }))
            }
        }
    }
}

impl TryFrom<tungstenite::Message> for WsMessage {
    type Error = anyhow::Error;

    fn try_from(message: tungstenite::Message) -> Result<Self, Self::Error> {
        match message {
            tungstenite::Message::Text(text) => Ok(Self::Text(text.to_string())),
            tungstenite::Message::Binary(binary) => Ok(Self::Binary(binary)),
            tungstenite::Message::Ping(ping) => Ok(Self::Ping(ping)),
            tungstenite::Message::Pong(pong) => Ok(Self::Pong(pong)),
            tungstenite::Message::Close(close) => {
                Ok(Self::Close(close.map(|close| {
                    (close.code.into(), close.reason.to_string())
                })))
            }
            tungstenite::Message::Frame(..) => {
                Err(anyhow::anyhow!("Unsupported message type: Frame"))
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct WsUpstreamMessageEvent {
    pub(crate) message: WsMessage,
}

#[typetag::serde(name = "WsUpstreamMessage")]
impl RecordEvent for WsUpstreamMessageEvent {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct WsDownstreamMessageEvent {
    pub(crate) message: WsMessage,
}

#[typetag::serde(name = "WsDownstreamMessage")]
impl RecordEvent for WsDownstreamMessageEvent {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct WsOpenEvent {
    pub(crate) request: HttpRequest,
}

#[typetag::serde(name = "WsOpen")]
impl RecordEvent for WsOpenEvent {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct WsCloseEvent {
    pub(crate) reason: String,
    pub(crate) code: u16,
}

#[typetag::serde(name = "WsClose")]
impl RecordEvent for WsCloseEvent {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HttpRequest {
    #[serde(with = "http_method_serde")]
    pub(crate) method: http::Method,
    #[serde(with = "http_uri_serde")]
    pub(crate) uri: http::Uri,
    #[serde(with = "http_version_serde")]
    pub(crate) version: http::Version,
    pub(crate) headers: BTreeMap<String, String>,
}

impl HttpRequest {
    pub(crate) fn update(mut self, target_url: &TargetUrl) -> Self {
        self.uri = target_url.apply(&self.uri);
        self.headers.insert(
            http::header::HOST.to_string(),
            target_url.authority.to_string(),
        );
        // TODO: support websocket extensions
        self.headers.remove("sec-websocket-extensions");
        self
    }

    pub(crate) fn from_request(
        req: &tokio_tungstenite::tungstenite::handshake::client::Request,
    ) -> Result<Self, anyhow::Error> {
        Ok(Self {
            method: req.method().clone(),
            uri: req.uri().clone(),
            version: req.version(),
            headers: req
                .headers()
                .iter()
                .map(|(k, v)| {
                    Ok::<_, anyhow::Error>((k.to_string().to_lowercase(), v.to_str()?.to_string()))
                })
                .try_collect()?,
        })
    }

    pub(crate) fn into_request(self) -> Result<Request, anyhow::Error> {
        let mut request = Request::builder()
            .uri(self.uri)
            .method(self.method)
            .version(self.version);
        for (key, value) in self.headers.iter() {
            request = request.header(key.as_str(), value.as_str());
        }
        Ok(request.body(())?)
    }
}
