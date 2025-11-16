use http::header;

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

// ----------------------------------------

/// Remove standard HTTP headers from a HeaderMap
pub(crate) fn remove_standard_headers(
    headers: &BTreeMap<String, String>,
) -> BTreeMap<String, String> {
    let headers_to_keep = [
        header::ACCEPT_ENCODING,
        header::CONTENT_ENCODING,
        header::USER_AGENT,
        header::CONTENT_TYPE,
        header::DATE,
    ];
    let mut cleaned_headers = headers.clone();
    for header_name in standard_http_headers() {
        if !headers_to_keep.contains(&header_name) {
            cleaned_headers.remove(&header_name.to_string());
        }
    }
    cleaned_headers
}

/// Returns a list of all standard HTTP headers as defined by the 'http' crate.
fn standard_http_headers() -> Vec<HeaderName> {
    vec![
        header::ACCEPT,
        header::ACCEPT_CHARSET,
        header::ACCEPT_ENCODING,
        header::ACCEPT_LANGUAGE,
        header::ACCEPT_RANGES,
        header::ACCESS_CONTROL_ALLOW_CREDENTIALS,
        header::ACCESS_CONTROL_ALLOW_HEADERS,
        header::ACCESS_CONTROL_ALLOW_METHODS,
        header::ACCESS_CONTROL_ALLOW_ORIGIN,
        header::ACCESS_CONTROL_EXPOSE_HEADERS,
        header::ACCESS_CONTROL_MAX_AGE,
        header::ACCESS_CONTROL_REQUEST_HEADERS,
        header::ACCESS_CONTROL_REQUEST_METHOD,
        header::AGE,
        header::ALLOW,
        header::ALT_SVC,
        header::AUTHORIZATION,
        header::CACHE_CONTROL,
        header::CONNECTION,
        header::CONTENT_DISPOSITION,
        header::CONTENT_ENCODING,
        header::CONTENT_LANGUAGE,
        header::CONTENT_LENGTH,
        header::CONTENT_LOCATION,
        header::CONTENT_RANGE,
        header::CONTENT_SECURITY_POLICY,
        header::CONTENT_SECURITY_POLICY_REPORT_ONLY,
        header::CONTENT_TYPE,
        header::COOKIE,
        header::DATE,
        header::DNT,
        header::ETAG,
        header::EXPECT,
        header::EXPIRES,
        header::FORWARDED,
        header::FROM,
        header::HOST,
        header::IF_MATCH,
        header::IF_MODIFIED_SINCE,
        header::IF_NONE_MATCH,
        header::IF_RANGE,
        header::IF_UNMODIFIED_SINCE,
        header::LAST_MODIFIED,
        header::LINK,
        header::LOCATION,
        header::MAX_FORWARDS,
        header::ORIGIN,
        header::PRAGMA,
        header::PROXY_AUTHENTICATE,
        header::PROXY_AUTHORIZATION,
        header::PUBLIC_KEY_PINS,
        header::PUBLIC_KEY_PINS_REPORT_ONLY,
        header::RANGE,
        header::REFERER,
        header::REFERRER_POLICY,
        header::REFRESH,
        header::RETRY_AFTER,
        header::SEC_WEBSOCKET_ACCEPT,
        header::SEC_WEBSOCKET_EXTENSIONS,
        header::SEC_WEBSOCKET_KEY,
        header::SEC_WEBSOCKET_PROTOCOL,
        header::SEC_WEBSOCKET_VERSION,
        header::SERVER,
        header::SET_COOKIE,
        header::STRICT_TRANSPORT_SECURITY,
        header::TE,
        header::TRAILER,
        header::TRANSFER_ENCODING,
        header::UPGRADE,
        header::UPGRADE_INSECURE_REQUESTS,
        header::USER_AGENT,
        header::VARY,
        header::VIA,
        header::WARNING,
        header::WWW_AUTHENTICATE,
        header::X_CONTENT_TYPE_OPTIONS,
        header::X_DNS_PREFETCH_CONTROL,
        header::X_FRAME_OPTIONS,
        header::X_XSS_PROTECTION,
    ]
}
