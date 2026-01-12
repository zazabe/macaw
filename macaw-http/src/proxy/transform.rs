use crate::lib::*;

/// Redact HTTP requests before they are recorded or compared to recorded requests.
/// Use case: redact nondeterministic parts, remove sensitive data, etc...
#[dyn_clonable::clonable]
pub trait HttpRedact: Clone + Send + Sync {
    fn http_redact_request(&self, request: HttpRequestEvent) -> HttpRequestEvent {
        request
    }
}

impl fmt::Debug for Box<dyn HttpRedact> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "HttpRedact")?;
        Ok(())
    }
}

#[derive(Clone)]
pub struct NoopHttpRedact;

impl HttpRedact for NoopHttpRedact {
    fn http_redact_request(&self, request: HttpRequestEvent) -> HttpRequestEvent {
        request
    }
}

impl Default for Box<dyn HttpRedact> {
    fn default() -> Self {
        Box::new(NoopHttpRedact)
    }
}

/// Transform HTTP requests/responses when they enter or leave Macaw in destination of a remote client/server.
/// Use case: resign requests, custom compression/decompression of the body message, etc...
#[dyn_clonable::clonable]
pub trait HttpTransform: Send + Sync + Clone {
    fn encode_request(&self, request: HttpRequestEvent) -> Result<HttpRequestEvent, anyhow::Error> {
        Ok(request)
    }

    fn decode_request(&self, request: HttpRequestEvent) -> Result<HttpRequestEvent, anyhow::Error> {
        Ok(request)
    }

    fn encode_response(
        &self,
        response: HttpResponseEvent,
    ) -> Result<HttpResponseEvent, anyhow::Error> {
        Ok(response)
    }

    fn decode_response(
        &self,
        response: HttpResponseEvent,
    ) -> Result<HttpResponseEvent, anyhow::Error> {
        Ok(response)
    }
}

impl fmt::Debug for Box<dyn HttpTransform> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "HttpTransform")?;
        Ok(())
    }
}

#[derive(Clone)]
pub struct NoopWireTransform;

impl HttpTransform for NoopWireTransform {}

impl Default for Box<dyn HttpTransform> {
    fn default() -> Self {
        Box::new(NoopWireTransform)
    }
}
