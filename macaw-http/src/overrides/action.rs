use crate::lib::*;

#[derive(Serialize, Deserialize, PartialEq, Default, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct HttpRequestAction {
    #[serde(default, skip_serializing_if = "is_default")]
    method: HttpMethodAction,
    #[serde(default, skip_serializing_if = "is_default")]
    path: FieldAction,
    #[serde(default, skip_serializing_if = "is_default")]
    body: FieldAction,
    #[serde(default, skip_serializing_if = "is_default")]
    headers: MapAction,
}

impl TransformAction for HttpRequestAction {
    type Message = HttpRequestEvent;

    fn apply(&self, request: Self::Message) -> Self::Message {
        let HttpRequestEvent {
            method,
            uri,
            body,
            headers,
            request_id,
            version,
        } = request;
        let uri_new_str = self.path.transform(uri.to_string());
        let uri_new = uri_new_str
            .parse()
            .unwrap_or_else(|e| panic!("Cannot parse `Uri` '{}': {}", uri_new_str, e));
        let body_text = body.to_text().expect("Body content must be a text");
        HttpRequestEvent {
            method: self.method.transform(method),
            uri: uri_new,
            body: Content::Text(PlainText::new(self.body.transform(body_text))),
            headers: self.headers.transform(headers.into_iter()).collect(),
            request_id,
            version,
        }
    }
}

#[derive(Serialize, Deserialize, PartialEq, Default, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct HttpResponseAction {
    #[serde(default, skip_serializing_if = "is_default")]
    status: StatusAction,
    #[serde(default, skip_serializing_if = "is_default")]
    body: BodyAction,
    #[serde(default, skip_serializing_if = "is_default")]
    headers: MapAction,
}

impl TransformAction for HttpResponseAction {
    type Message = HttpResponseEvent;

    fn apply(&self, response: Self::Message) -> Self::Message {
        let HttpResponseEvent {
            status,
            headers,
            body,
            request_id,
            version,
        } = response;

        HttpResponseEvent {
            status: self.status.transform(status),
            body: self
                .body
                .try_apply(body)
                .expect("Body content must be a text"),
            headers: self.headers.transform(headers.into_iter()).collect(),
            request_id,
            version,
        }
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Default, Clone)]
struct HttpMethodAction(#[serde(default)] Option<HttpMethodRaw>);

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
struct HttpMethodRaw(#[serde(with = "http_method_serde")] http::Method);

impl HttpMethodAction {
    fn transform(&self, method: http::Method) -> http::Method {
        match &self.0 {
            Some(method) => method.0.clone(),
            None => method,
        }
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
struct StatusCodeRaw(#[serde(with = "http_status_serde")] http::StatusCode);

#[derive(Serialize, Deserialize, PartialEq, Debug, Default, Clone)]
struct StatusAction(#[serde(default)] Option<StatusCodeRaw>);

impl StatusAction {
    fn transform(&self, status: http::StatusCode) -> http::StatusCode {
        match &self.0 {
            Some(replacement) => replacement.0,
            None => status,
        }
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Default, Clone)]
struct BodyAction(FieldAction);

impl TryTransformAction for BodyAction {
    type Error = anyhow::Error;
    type Message = Content;

    fn try_apply(&self, content: Content) -> Result<Content, Self::Error> {
        match content {
            Content::Text(text) => {
                let text = text.as_str().to_string();
                let text_transformed = self.0.transform(text);
                Ok(Content::Text(PlainText::new(text_transformed)))
            }
            Content::Bytes(..) => Err(anyhow::anyhow!("Cannot transform bytes content")),
            Content::Empty => Err(anyhow::anyhow!("Cannot transform empty content")),
        }
    }
}
