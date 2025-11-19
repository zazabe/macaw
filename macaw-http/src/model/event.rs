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
pub(crate) struct HttpRequestEvent {
    pub(crate) request_id: Uuid,
    #[serde(with = "http_method_serde")]
    pub(crate) method: http::Method,
    #[serde(with = "http_uri_serde")]
    pub(crate) uri: http::Uri,
    #[serde(with = "http_version_serde")]
    pub(crate) version: http::Version,
    pub(crate) headers: BTreeMap<String, String>,
    pub(crate) body: Box<dyn Content>,
}

impl HttpRequestEvent {
    pub(crate) fn from_request(req: &HttpRequest) -> Result<Self, anyhow::Error> {
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
            body: <dyn Content>::from_bytes(req.body().to_bytes().as_ref()),
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

        let body = BodyBytes::new(self.body.to_bytes()?);

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
impl RecordEvent for HttpRequestEvent {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct HttpResponseEvent {
    pub(crate) request_id: Uuid,
    #[serde(with = "http_status_serde")]
    pub(crate) status: http::StatusCode,
    #[serde(with = "http_version_serde")]
    pub(crate) version: http::Version,
    pub(crate) headers: BTreeMap<String, String>,
    pub(crate) body: Box<dyn Content>,
}

impl HttpResponseEvent {
    pub(crate) fn from_response(
        res: &HttpResponse,
        request_id: Uuid,
    ) -> Result<Self, anyhow::Error> {
        Ok(Self {
            request_id,
            status: res.status(),
            version: res.version(),
            headers: res
                .headers()
                .iter()
                .map(|(k, v)| Ok::<_, anyhow::Error>((k.to_string(), v.to_str()?.to_string())))
                .try_collect()?,
            body: <dyn Content>::from_bytes(res.body().to_bytes().as_ref()),
        })
    }

    pub(crate) fn to_response(&self) -> Result<HttpResponse, anyhow::Error> {
        let mut builder = http::Response::builder()
            .status(self.status)
            .version(self.version);

        for (key, value) in &self.headers {
            builder = builder.header(key, value);
        }

        let body = BodyBytes::new(self.body.to_bytes()?);

        Ok(builder.body(body)?)
    }
}

#[typetag::serde(name = "HttpResponse")]
impl RecordEvent for HttpResponseEvent {}
