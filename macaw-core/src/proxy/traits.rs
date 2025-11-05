use crate::lib::*;
use arrayvec::ArrayString;
use futures::future;

#[derive(Debug, Eq, PartialEq, Hash, Clone, Copy, Serialize, Deserialize)]
pub enum ProxyId {
    Uuid(Uuid),
    Named(ArrayString<64>),
}

impl ProxyId {
    pub fn uuid() -> Self {
        Self::Uuid(Uuid::new_v4())
    }

    pub fn named(name: &str) -> Result<Self, anyhow::Error> {
        Ok(Self::Named(ArrayString::from_str(name)?))
    }
}

impl FromStr for ProxyId {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::named(s)
    }
}

#[derive(Debug)]
pub struct DownstreamMessage<P: Proxy>(<P as ProxyDownstream>::IncomingMessage);

impl<P: Proxy> DownstreamMessage<P> {
    pub fn from(message: <P as ProxyDownstream>::IncomingMessage) -> Self {
        Self(message)
    }

    pub fn into_inner(self) -> <P as ProxyDownstream>::IncomingMessage {
        self.0
    }
}

#[derive(Debug)]
pub struct UpstreamMessage<P: Proxy>(<P as ProxyUpstream>::IncomingMessage);

impl<P: Proxy> UpstreamMessage<P> {
    pub fn from(message: <P as ProxyUpstream>::IncomingMessage) -> Self {
        Self(message)
    }

    pub fn into_inner(self) -> <P as ProxyUpstream>::IncomingMessage {
        self.0
    }
}

pub trait Proxy: Send + 'static
where
    Self: ProxyDownstream + ProxyUpstream + ProxyHandler + fmt::Debug + 'static,
    <Self as ProxyDownstream>::IncomingMessage: RecordEvent,
    <Self as ProxyDownstream>::OutgoingMessage: RecordEvent,
    <Self as ProxyUpstream>::IncomingMessage: RecordEvent,
    <Self as ProxyHandler>::Message: RecordEventUntagged,
{
    fn id(&self) -> ProxyId;
}

/// Implemented by proxies sending outgoing messages to downstream (e.g. HTTP/WS server receiving requests).
pub trait ProxyDownstream {
    type IncomingMessage: RecordEvent + Clone;
    type OutgoingMessage: RecordEvent + Clone;

    fn downstream_incoming_redact(
        &mut self,
        message: Self::IncomingMessage,
    ) -> impl Future<Output = Result<Self::IncomingMessage, anyhow::Error>> + Send {
        future::ready(Ok(message))
    }

    fn downstream_incoming_process(
        &mut self,
        message: Self::IncomingMessage,
    ) -> impl Future<Output = Result<Option<Self::OutgoingMessage>, anyhow::Error>> + Send {
        future::ready(Ok(None))
    }

    fn downstream_outgoing_redact(
        &mut self,
        message: Self::OutgoingMessage,
    ) -> impl Future<Output = Result<Self::OutgoingMessage, anyhow::Error>> + Send {
        future::ready(Ok(message))
    }
}

/// Implemented by proxies receiving incoming messages from upstream (e.g. WebSocket client stream).
pub trait ProxyUpstream {
    type IncomingMessage: RecordEvent + Clone;

    fn upstream_incoming_redact(
        &mut self,
        message: Self::IncomingMessage,
    ) -> impl Future<Output = Result<Self::IncomingMessage, anyhow::Error>> + Send {
        future::ready(Ok(message))
    }

    fn upstream_incoming_process(
        &mut self,
        message: Self::IncomingMessage,
    ) -> impl Future<Output = Result<(), anyhow::Error>> + Send {
        future::ready(Err(anyhow::anyhow!("Unexpected upstream message")))
    }
}

/// Handle recorded events coming from the replayer actor.
pub trait ProxyHandler {
    type Message: RecordEventUntagged;

    fn handle_message<'a>(
        &'a mut self,
        message: Self::Message,
    ) -> Pin<Box<dyn Future<Output = Result<(), anyhow::Error>> + Send + 'a>> {
        Box::pin(future::ready(Err(anyhow::anyhow!(
            "Unexpected incoming message"
        ))))
    }
}
