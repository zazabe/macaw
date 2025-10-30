use base64::{Engine, prelude::BASE64_STANDARD};

use crate::lib::*;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum RecordEvent {
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

impl RecordHeader {
    pub(crate) fn new() -> Self {
        Self {
            record_id: Uuid::new_v4().to_string(),
            record_seed: Uuid::new_v4().to_string(),
            timestamp: Utc::now(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Event<D> {
    proxy_id: ProxyId,
    timestamp: DateTime<Utc>,
    data: D,
}

impl<D> Event<D> {
    pub(crate) fn new(proxy_id: ProxyId, data: D) -> Self {
        Self {
            proxy_id,
            timestamp: Utc::now(),
            data,
        }
    }
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

impl HttpRequestEvent {
    pub(crate) fn from_request(req: &HttpRequest) -> Result<Self, anyhow::Error> {
        Ok(Self {
            method: req.method().clone(),
            uri: req.uri().to_string(),
            version: req.version(),
            headers: req
                .headers()
                .iter()
                .map(|(k, v)| Ok::<_, anyhow::Error>((k.to_string(), v.to_str()?.to_string())))
                .try_collect()?,
            body: Some(Content::from_body(req.body())),
        })
    }
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

impl HttpResponseEvent {
    pub(crate) fn from_response(res: &HttpResponse) -> Result<Self, anyhow::Error> {
        Ok(Self {
            status: res.status(),
            version: res.version(),
            headers: res
                .headers()
                .iter()
                .map(|(k, v)| Ok::<_, anyhow::Error>((k.to_string(), v.to_str()?.to_string())))
                .try_collect()?,
            body: Some(Content::from_body(res.body())),
        })
    }
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

impl Content {
    pub(crate) fn from_body(body: &BodyBytes) -> Self {
        let bytes = body.to_bytes().to_vec();
        match String::from_utf8(bytes) {
            Ok(s) => Self {
                encoding: ContentEncoding::Plain,
                data: s,
            },
            Err(e) => Self {
                encoding: ContentEncoding::Base64,
                data: BASE64_STANDARD.encode(e.as_bytes()),
            },
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum ContentEncoding {
    Plain,
    Base64,
}
