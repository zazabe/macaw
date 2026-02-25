use crate::lib::*;

#[derive(Debug, Clone)]
pub(crate) enum HttpEvent {
    HttpRequest(HttpRequestEvent),
    HttpResponse(HttpResponseEvent),
}

impl HttpEvent {
    pub(crate) fn downcast(event: Box<dyn RecordEvent>) -> Result<Self, anyhow::Error>
    where
        Self: Sized,
    {
        match event.downcast::<HttpRequestEvent>() {
            Ok(request) => Ok(HttpEvent::HttpRequest(*request)),
            Err(event) => match event.downcast::<HttpResponseEvent>() {
                Ok(response) => Ok(HttpEvent::HttpResponse(*response)),
                Err(event) => Err(anyhow::anyhow!(
                    "Failed to downcast to HttpEvent, invalid event: {:?}",
                    event
                )),
            },
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HttpRequestEvent {
    pub request_id: Uuid,
    #[serde(with = "http_method_serde")]
    pub method: http::Method,
    #[serde(with = "http_uri_serde")]
    pub uri: http::Uri,
    #[serde(with = "http_version_serde")]
    pub version: http::Version,
    pub headers: BTreeMap<String, String>,
    pub body: Content,
}

impl HttpRequestEvent {
    pub fn from_request(req: &HttpRequest) -> Result<Self, anyhow::Error> {
        Ok(Self {
            request_id: Uuid::new_v4(),
            method: req.method().clone(),
            uri: req.uri().clone(),
            version: req.version(),
            headers: req
                .headers()
                .iter()
                .map(|(k, v)| Ok::<_, anyhow::Error>((k.to_string(), v.to_str()?.to_string())))
                .try_collect()?,
            body: Content::from_bytes(req.body().to_bytes()),
        })
    }

    pub fn to_request(&self) -> Result<HttpRequest, anyhow::Error> {
        let mut builder = http::Request::builder()
            .method(self.method.clone())
            .uri(self.uri.clone())
            .version(self.version);

        for (key, value) in &self.headers {
            builder = builder.header(key, value);
        }

        let body = BodyBytes::new(self.body.to_bytes());

        Ok(builder.body(body)?)
    }

    pub(crate) fn matches(&self, other: &HttpRequestEvent) -> bool {
        self.method == other.method
            && self.uri == other.uri
            && self.version == other.version
            && remove_standard_headers(&self.headers) == remove_standard_headers(&other.headers)
    }
}

#[typetag::serde(name = "HttpRequest")]
impl RecordEvent for HttpRequestEvent {
    fn format_debug(&self) -> RecordFormatter {
        RecordFormatter::new(
            DebugDirection::DownstreamToUpstream,
            vec![
                RecordPart::StreamType("HTTP".to_string()),
                RecordPart::Id(self.request_id.simple().to_string()),
                RecordPart::Meta(self.method.to_string()),
                RecordPart::Meta(self.uri.path().to_string()),
                RecordPart::Content(body_preview(&self.body)),
            ],
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HttpResponseEvent {
    pub request_id: Uuid,
    #[serde(with = "http_status_serde")]
    pub status: http::StatusCode,
    #[serde(with = "http_version_serde")]
    pub version: http::Version,
    pub headers: BTreeMap<String, String>,
    pub body: Content,
}

impl HttpResponseEvent {
    pub fn from_response(res: &HttpResponse, request_id: Uuid) -> Result<Self, anyhow::Error> {
        Ok(Self {
            request_id,
            status: res.status(),
            version: res.version(),
            headers: res
                .headers()
                .iter()
                .map(|(k, v)| Ok::<_, anyhow::Error>((k.to_string(), v.to_str()?.to_string())))
                .try_collect()?,
            body: Content::from_bytes(res.body().to_bytes()),
        })
    }

    pub fn to_response(&self) -> Result<HttpResponse, anyhow::Error> {
        let mut builder = http::Response::builder()
            .status(self.status)
            .version(self.version);

        for (key, value) in &self.headers {
            builder = builder.header(key, value);
        }

        let body = BodyBytes::new(self.body.to_bytes());

        Ok(builder.body(body)?)
    }
}

fn body_preview(body: &Content) -> String {
    match body {
        Content::Text(t) => {
            let s = to_single_line(t.as_str());
            if s.len() > 1000 {
                format!("{}...", &s[..997])
            } else {
                s
            }
        }
        Content::Bytes(_) => "<binary>".to_string(),
        Content::Empty => "<empty>".to_string(),
    }
}

#[typetag::serde(name = "HttpResponse")]
impl RecordEvent for HttpResponseEvent {
    fn format_debug(&self) -> RecordFormatter {
        RecordFormatter::new(
            DebugDirection::UpstreamToDownstream,
            vec![
                RecordPart::StreamType("HTTP".to_string()),
                RecordPart::Id(self.request_id.simple().to_string()),
                RecordPart::Meta(self.status.to_string()),
                RecordPart::Content(body_preview(&self.body)),
            ],
        )
    }
}
