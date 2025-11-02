use std::fmt;
use std::str::FromStr;
use std::task::{Context, Poll};
use std::{collections::HashMap, pin::Pin};

use futures::{Stream, stream};
use http;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::model::RecordEvent;

#[derive(Debug, Eq, PartialEq, Hash, Clone, Copy, Serialize, Deserialize)]
pub enum ProxyId {
    Uuid(Uuid),
}

impl ProxyId {
    pub fn uuid() -> Self {
        Self::Uuid(Uuid::new_v4())
    }
}

#[async_trait::async_trait(?Send)]
pub trait Proxy: fmt::Debug + 'static {
    fn id(&self) -> ProxyId;

    async fn redact_downstream_message(
        &self,
        message: Box<dyn RecordEvent>,
    ) -> Result<Box<dyn RecordEvent>, anyhow::Error> {
        Ok(message)
    }

    async fn process_downstream_message(
        &self,
        message: Box<dyn RecordEvent>,
    ) -> Result<Option<Box<dyn RecordEvent>>, anyhow::Error>;

    async fn redact_upstream_message(
        &self,
        message: Box<dyn RecordEvent>,
    ) -> Result<Box<dyn RecordEvent>, anyhow::Error> {
        Ok(message)
    }

    async fn process_upstream_message(
        &self,
        message: Box<dyn RecordEvent>,
    ) -> Result<(), anyhow::Error>;
}

pub(crate) trait ProxyStream: Stream<Item = ()> + 'static {}

impl<T> ProxyStream for T where T: Stream<Item = ()> + 'static {}

pub(crate) type BoxedProxyStream = Pin<Box<dyn ProxyStream>>;

pub(crate) struct Proxies {
    proxies: stream::SelectAll<BoxedProxyStream>,
}

impl Proxies {
    pub(crate) fn default() -> Self {
        Self {
            proxies: stream::SelectAll::new(),
        }
    }

    pub(crate) fn add_proxy(&mut self, stream: BoxedProxyStream) {
        self.proxies.push(stream);
    }
}

impl fmt::Debug for Proxies {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Proxies")
    }
}

impl Stream for Proxies {
    type Item = ();
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.get_mut().proxies).poll_next(cx)
    }
}

#[derive(Debug, Clone)]
pub struct TargetUrl {
    scheme: http::uri::Scheme,
    authority: http::uri::Authority,
}

impl TargetUrl {
    pub fn apply(&self, other: &http::Uri) -> Result<http::Uri, anyhow::Error> {
        Ok(http::uri::Builder::from(other.clone())
            .scheme(self.scheme.clone())
            .authority(self.authority.clone())
            .build()?)
    }
}

impl FromStr for TargetUrl {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let uri = http::Uri::from_str(s)?;
        Self::try_from(uri)
    }
}

impl TryFrom<http::Uri> for TargetUrl {
    type Error = anyhow::Error;
    fn try_from(url: http::Uri) -> Result<Self, Self::Error> {
        Ok(Self {
            scheme: url
                .scheme()
                .cloned()
                .ok_or(anyhow::anyhow!("No scheme in target url"))?,
            authority: url
                .authority()
                .cloned()
                .ok_or(anyhow::anyhow!("No authority in target url"))?,
        })
    }
}

#[derive(Debug, Clone)]
pub struct Sender {
    pub id: ProxyId,
    pub tx: tokio::sync::mpsc::UnboundedSender<crate::processor::Message>,
}

impl Sender {
    pub fn new(
        id: ProxyId,
        tx: tokio::sync::mpsc::UnboundedSender<crate::processor::Message>,
    ) -> Self {
        Self { id, tx }
    }
}
