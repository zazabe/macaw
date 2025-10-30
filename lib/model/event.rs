use crate::lib::*;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum RecordEvent {
    Header(RecordHeader),
    HttpRequest(Event<HttpRequestEvent>),
    HttpResponse(Event<HttpResponseEvent>),
    WsUpstreamMessage(Event<WsUpstreamMessageEvent>),
    WsDownstreamMessage(Event<WsDownstreamMessageEvent>),
    WsOpen(Event<WsOpenEvent>),
    WsClose(Event<WsCloseEvent>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct RecordHeader {
    pub(crate) record_id: String,
    pub(crate) record_seed: String,
    pub(crate) timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Event<D> {
    id: Uuid,
    timestamp: DateTime<Utc>,
    data: D,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct HttpRequestEvent {
    #[serde(with = "http_method_serde")]
    pub(crate) method: http::Method,
    pub(crate) uri: String,
    #[serde(with = "http_version_serde")]
    pub(crate) version: http::Version,
    pub(crate) headers: Vec<(String, String)>,
    pub(crate) body: Option<Content>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct HttpResponseEvent {
    #[serde(with = "http_status_serde")]
    pub(crate) status: http::StatusCode,
    #[serde(with = "http_version_serde")]
    pub(crate) version: http::Version,
    pub(crate) headers: Vec<(String, String)>,
    pub(crate) body: Option<Content>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct WsUpstreamMessageEvent {
    message: Content,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct WsDownstreamMessageEvent {
    message: Content,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct WsOpenEvent {
    pub(crate) request: HttpRequestEvent,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct WsCloseEvent {
    pub(crate) reason: String,
    pub(crate) code: u16,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Content {
    encoding: ContentEncoding,
    data: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum ContentEncoding {
    Plain,
    Base64,
}
