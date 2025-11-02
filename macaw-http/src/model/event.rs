use std::fmt;

use http;
use itertools::Itertools;
use serde::{Deserialize, Serialize};

use crate::io::http::{BodyBytes, HttpRequest, HttpResponse};
use crate::parsing::http::{
    http_method_serde, http_status_serde, http_uri_serde, http_version_serde,
};
use bytes::Bytes;
use macaw_core::prelude::*;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HttpRequestEvent {
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
            body: Some(Content::from_bytes(req.body().to_bytes().as_ref())),
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
            Some(content) => {
                let bytes = content.to_bytes()?;
                BodyBytes::new(Bytes::from(bytes))
            }
            None => BodyBytes::new(Bytes::new()),
        };

        Ok(builder.body(body)?)
    }
}

#[typetag::serde]
impl RecordEvent for HttpRequestEvent {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HttpResponseEvent {
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
            body: Some(Content::from_bytes(res.body().to_bytes().as_ref())),
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
            Some(content) => {
                let bytes = content.to_bytes()?;
                BodyBytes::new(Bytes::from(bytes))
            }
            None => BodyBytes::new(Bytes::new()),
        };

        Ok(builder.body(body)?)
    }
}

#[typetag::serde]
impl RecordEvent for HttpResponseEvent {}
