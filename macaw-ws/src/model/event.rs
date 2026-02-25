use std::hash::{Hash, Hasher};
use tokio_tungstenite::tungstenite::{
    self,
    handshake::client::Request,
    protocol::{CloseFrame, frame::coding::CloseCode},
};

use crate::lib::*;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct WsUpstreamEvent {
    pub(crate) peer_id: WsPeerId,
    pub(crate) event: WsEvent,
}

fn format_ws_event(event: &WsEvent) -> String {
    match event {
        WsEvent::Open(open) => format!("Open({})", open.request.uri.path()),
        WsEvent::Message(msg) => format_ws_message(&msg.message),
        WsEvent::Disconnect => "Disconnect".to_string(),
    }
}

fn format_ws_message(msg: &WsMessage) -> String {
    match msg {
        WsMessage::Text(s) => {
            let single = to_single_line(s);
            if single.len() > 2000 {
                format!("{}...", &single[..1997])
            } else {
                single
            }
        }
        WsMessage::Binary(_) => "<binary>".to_string(),
        WsMessage::Ping(_) => "<ping>".to_string(),
        WsMessage::Pong(_) => "<pong>".to_string(),
        WsMessage::Close(_) => "<close>".to_string(),
    }
}

#[typetag::serde(name = "WsUpstream")]
impl RecordEvent for WsUpstreamEvent {
    fn format_debug(&self) -> RecordFormatter {
        RecordFormatter::new(
            DebugDirection::UpstreamToDownstream,
            vec![
                RecordPart::StreamType("WS".to_string()),
                RecordPart::Content(format_ws_event(&self.event)),
            ],
        )
    }
}

impl WsUpstreamEvent {
    pub(crate) fn new(peer_id: WsPeerId, event: WsEvent) -> Self {
        Self { peer_id, event }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct WsDownstreamEvent {
    pub(crate) peer_id: WsPeerId,
    pub(crate) event: WsEvent,
}

#[typetag::serde(name = "WsDownstream")]
impl RecordEvent for WsDownstreamEvent {
    fn format_debug(&self) -> RecordFormatter {
        RecordFormatter::new(
            DebugDirection::DownstreamToUpstream,
            vec![
                RecordPart::StreamType("WS".to_string()),
                RecordPart::Content(format_ws_event(&self.event)),
            ],
        )
    }
}

impl WsDownstreamEvent {
    pub(crate) fn new(peer_id: WsPeerId, event: WsEvent) -> Self {
        Self { peer_id, event }
    }
}

// ------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WsEvent {
    Message(WsMessageEvent),
    Open(WsOpenEvent),
    Disconnect,
}

impl HasFingerprint for WsEvent {
    fn hash_into(&self, hasher: &mut impl Hasher) {
        core::mem::discriminant(self).hash(hasher);
        match self {
            Self::Message(message) => message.hash_into(hasher),
            Self::Open(open) => open.hash_into(hasher),
            Self::Disconnect => (),
        }
    }
}

impl WsEvent {
    pub fn open(request: HttpRequest) -> Self {
        Self::Open(WsOpenEvent { request })
    }

    pub fn message(message: WsMessage) -> Self {
        Self::Message(WsMessageEvent { message })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Hash)]
pub enum WsMessage {
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

#[derive(Debug, Clone, Serialize, Deserialize, Hash)]
pub struct WsMessageEvent {
    pub message: WsMessage,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WsOpenEvent {
    pub request: HttpRequest,
}

impl HasFingerprint for WsOpenEvent {
    fn hash_into(&self, hasher: &mut impl Hasher) {
        self.request.hash_into(hasher);
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
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
    pub(crate) fn update(mut self, target_url: &TargetUrl) -> Result<Self, anyhow::Error> {
        self.uri = target_url.apply(&self.uri)?;
        self.headers.insert(
            http::header::HOST.to_string(),
            target_url.authority.to_string(),
        );
        // TODO: support websocket extensions
        self.headers.remove("sec-websocket-extensions");
        Ok(self)
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

impl HasFingerprint for HttpRequest {
    fn hash_into(&self, hasher: &mut impl Hasher) {
        self.uri.hash(hasher);
        self.method.hash(hasher);
        self.version.hash(hasher);
        for (key, value) in remove_standard_headers(&self.headers).iter() {
            key.to_lowercase().hash(hasher);
            value.to_lowercase().hash(hasher);
        }
    }
}

// ------------------------------------------------------------

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub(crate) struct Fingerprint(u64);

pub(crate) trait HasFingerprint {
    fn hash_into(&self, hasher: &mut impl Hasher);

    fn fingerprint(&self) -> Fingerprint {
        let mut hasher = ahash::AHasher::default();
        self.hash_into(&mut hasher);
        Fingerprint(hasher.finish())
    }
}

impl<T> HasFingerprint for T
where
    T: Hash,
{
    fn hash_into(&self, hasher: &mut impl Hasher) {
        self.hash(hasher);
    }
}
