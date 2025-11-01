use std::any::Any;

use base64::{Engine, prelude::BASE64_STANDARD};

use crate::lib::*;

#[dyn_clonable::clonable]
#[typetag::serde(tag = "type")]
pub(crate) trait RecordEvent: Any + fmt::Debug + Clone {}

impl dyn RecordEvent {
    pub fn downcast<T: RecordEvent + 'static>(self: Box<Self>) -> Result<Box<T>, Box<Self>> {
        if (*self).as_any().is::<T>() {
            // It is sound to convert; the trait object is actually T
            Ok(self.downcast_unchecked())
        } else {
            Err(self)
        }
    }

    // Helper for unchecked downcast (only call if is::<T>() successful)
    fn downcast_unchecked<T: RecordEvent + 'static>(self: Box<Self>) -> Box<T> {
        unsafe { Box::from_raw(Box::into_raw(self) as *mut T) }
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
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
    #[serde(with = "http_uri_serde")]
    pub(crate) uri: http::Uri,
    #[serde(with = "http_version_serde")]
    pub(crate) version: http::Version,
    pub(crate) headers: Vec<(String, String)>,
    pub(crate) body: Option<Content>,
}

impl HttpRequestEvent {
    pub(crate) fn from_request(req: &HttpRequest) -> Result<Self, anyhow::Error> {
        Ok(Self {
            method: req.method().clone(),
            uri: req.uri().clone(),
            version: req.version(),
            headers: req
                .headers()
                .iter()
                .map(|(k, v)| Ok::<_, anyhow::Error>((k.to_string(), v.to_str()?.to_string())))
                .try_collect()?,
            body: Some(Content::from_body(req.body())),
        })
    }

    pub(crate) fn to_request(&self) -> Result<HttpRequest, anyhow::Error> {
        let mut builder = http::Request::builder()
            .method(self.method.clone())
            .uri(self.uri.clone())
            .version(self.version);

        for (key, value) in &self.headers {
            builder = builder.header(key, value);
        }

        let body = match &self.body {
            Some(content) => content.clone().into_body()?,
            None => BodyBytes::new(Bytes::new()),
        };

        Ok(builder.body(body)?)
    }
}

#[typetag::serde]
impl RecordEvent for HttpRequestEvent {}

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

    pub(crate) fn to_response(&self) -> Result<HttpResponse, anyhow::Error> {
        let mut builder = http::Response::builder()
            .status(self.status)
            .version(self.version);

        for (key, value) in &self.headers {
            builder = builder.header(key, value);
        }

        let body = match &self.body {
            Some(content) => content.clone().into_body()?,
            None => BodyBytes::new(Bytes::new()),
        };

        Ok(builder.body(body)?)
    }
}

#[typetag::serde]
impl RecordEvent for HttpResponseEvent {}

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
    pub(crate) request: HttpRequestEvent,
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
    pub(crate) fn into_body(self) -> Result<BodyBytes, anyhow::Error> {
        match self.encoding {
            ContentEncoding::Plain => Ok(BodyBytes::from(self.data)),
            ContentEncoding::Base64 => Ok(BodyBytes::from(BASE64_STANDARD.decode(self.data)?)),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum ContentEncoding {
    Plain,
    Base64,
}
