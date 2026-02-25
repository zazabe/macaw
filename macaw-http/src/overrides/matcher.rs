use crate::lib::*;

#[derive(Serialize, Deserialize, PartialEq, Debug, Default, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct HttpRequestMatcher {
    #[serde(default, skip_serializing_if = "is_default")]
    method: RegexMatcher,
    #[serde(default, skip_serializing_if = "is_default")]
    path: RegexMatcher,
    #[serde(default, skip_serializing_if = "is_default")]
    body: RegexMatcher,
    #[serde(default, skip_serializing_if = "is_default")]
    headers: MapMatcher,
}

impl HttpRequestMatcher {
    pub(crate) fn is_match(&self, request: &HttpRequestEvent) -> bool {
        let body_as_text = request.body.to_text().ok().unwrap_or_default();
        self.method.is_match(request.method.as_str())
            && self.path.is_match(&request.uri.to_string())
            && self.body.is_match(&body_as_text)
            && self.headers.is_match(request.headers.iter())
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Default, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct HttpResponseMatcher {
    #[serde(default, skip_serializing_if = "is_default")]
    request: HttpRequestMatcher,
    #[serde(default, skip_serializing_if = "is_default")]
    status: RegexMatcher,
    #[serde(default, skip_serializing_if = "is_default")]
    body: RegexMatcher,
    #[serde(default, skip_serializing_if = "is_default")]
    headers: MapMatcher,
}

impl HttpResponseMatcher {
    pub(crate) fn is_match(
        &self,
        response: &HttpResponseEvent,
        request: &HttpRequestEvent,
    ) -> bool {
        let body_as_text = response.body.to_text().ok().unwrap_or_default();
        self.status.is_match(response.status.as_str())
            && self.body.is_match(&body_as_text)
            && self.headers.is_match(response.headers.iter())
            && self.request.is_match(request)
    }
}
