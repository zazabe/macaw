use std::io::BufReader;

use itertools::Either;

use crate::lib::*;

#[derive(Deserialize, Debug)]
#[serde(from = "HttpOverrideRulesRaw")]
pub struct HttpOverrideRules {
    request: OverrideRulesChain<HttpRequestRule>,
    response: OverrideRulesChain<HttpResponseRule>,
}

impl HttpOverrideRules {
    pub fn from_file(path: &Path) -> Result<Self, anyhow::Error> {
        let file = fs_err::File::open(path)?;
        let reader = BufReader::new(file);
        let rules: HttpOverrideRules = serde_json::from_reader(reader)?;
        Ok(rules)
    }
}

impl HttpOverride for HttpOverrideRules {
    fn http_override_request(&self, request: HttpRequestEvent) -> HttpRequestEvent {
        match self.request.apply_rules(request, ()) {
            OverrideOutput::Suppress => panic!("HTTP messages don't support 'SuppressMessage'."),
            OverrideOutput::Message(request) => request,
        }
    }

    fn http_override_response(
        &self,
        response: HttpResponseEvent,
        request: HttpRequestEvent,
    ) -> HttpResponseEvent {
        match self.response.apply_rules(response, request) {
            OverrideOutput::Suppress => panic!("HTTP messages don't support 'SuppressMessage'."),
            OverrideOutput::Message(response) => response,
        }
    }
}

impl From<HttpOverrideRulesRaw> for HttpOverrideRules {
    fn from(raw: HttpOverrideRulesRaw) -> Self {
        let (request, response): (Vec<HttpRequestRule>, Vec<HttpResponseRule>) =
            raw.0.into_iter().partition_map(|rule| match rule {
                HttpOverrideRuleRaw::HttpRequest(rule) => Either::Left(rule),
                HttpOverrideRuleRaw::HttpResponse(rule) => Either::Right(rule),
            });
        Self {
            request: OverrideRulesChain::from_iter(request),
            response: OverrideRulesChain::from_iter(response),
        }
    }
}

#[derive(Deserialize)]
struct HttpOverrideRulesRaw(Vec<HttpOverrideRuleRaw>);

#[derive(Deserialize)]
enum HttpOverrideRuleRaw {
    HttpRequest(HttpRequestRule),
    HttpResponse(HttpResponseRule),
}

/// Apply overriding rules to HTTP requests/responses.
/// Use case: override request/response with custom logic, e.g. add/remove headers, change body, etc...
pub trait HttpOverride: Send + Sync {
    fn http_override_request(&self, request: HttpRequestEvent) -> HttpRequestEvent {
        request
    }

    fn http_override_response(
        &self,
        response: HttpResponseEvent,
        _request: HttpRequestEvent,
    ) -> HttpResponseEvent {
        response
    }
}

impl fmt::Debug for Box<dyn HttpOverride> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "HttpOverride")?;
        Ok(())
    }
}

pub struct NoopHttpOverride;

impl HttpOverride for NoopHttpOverride {
    fn http_override_request(&self, request: HttpRequestEvent) -> HttpRequestEvent {
        request
    }

    fn http_override_response(
        &self,
        response: HttpResponseEvent,
        _request: HttpRequestEvent,
    ) -> HttpResponseEvent {
        response
    }
}

impl Default for Box<dyn HttpOverride> {
    fn default() -> Self {
        Box::new(NoopHttpOverride)
    }
}

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
