use arrayvec::ArrayString;

use crate::lib::*;

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

pub trait Proxy
where
    Self: ProxyDownstream + ProxyUpstream + ProxyHandler + Clone + fmt::Debug + 'static,
    <Self as ProxyDownstream>::IncomingMessage: RecordEvent,
    <Self as ProxyDownstream>::OutgoingMessage: RecordEvent,
    <Self as ProxyUpstream>::IncomingMessage: RecordEvent,
    <Self as ProxyHandler>::Message: RecordEventUntagged,
{
    fn id(&self) -> ProxyId;
}

#[async_trait::async_trait(?Send)]
pub trait ProxyDownstream {
    type IncomingMessage: RecordEvent;
    type OutgoingMessage: RecordEvent;

    async fn downstream_incoming_redact(
        &self,
        message: Self::IncomingMessage,
    ) -> Result<Self::IncomingMessage, anyhow::Error> {
        Ok(message)
    }

    async fn downstream_incoming_process(
        &self,
        message: Self::IncomingMessage,
    ) -> Result<Option<Self::OutgoingMessage>, anyhow::Error> {
        Ok(None)
    }

    async fn downstream_outgoing_redact(
        &self,
        message: Self::OutgoingMessage,
    ) -> Result<Self::OutgoingMessage, anyhow::Error> {
        Ok(message)
    }
}

#[async_trait::async_trait(?Send)]
pub trait ProxyUpstream {
    type IncomingMessage: RecordEvent;

    async fn upstream_incoming_redact(
        &self,
        message: Self::IncomingMessage,
    ) -> Result<Self::IncomingMessage, anyhow::Error> {
        Err(anyhow::anyhow!("Unexpected upstream message"))
    }

    async fn upstream_incoming_prepare(
        &self,
        message: Self::IncomingMessage,
    ) -> Result<Self::IncomingMessage, anyhow::Error> {
        Err(anyhow::anyhow!("Unexpected upstream message"))
    }

    async fn upstream_incoming_process(
        &self,
        message: Self::IncomingMessage,
    ) -> Result<(), anyhow::Error> {
        Err(anyhow::anyhow!("Unexpected upstream message"))
    }
}

/// Forwards RecordEvents to downstream connection.
#[async_trait::async_trait(?Send)]
pub trait ProxyHandler {
    type Message: RecordEventUntagged;

    async fn handle_message(&self, message: Self::Message) -> Result<(), anyhow::Error> {
        Err(anyhow::anyhow!("Unexpected incoming message"))
    }
}

#[async_trait::async_trait(?Send)]
pub trait ProxyMessageHandler {
    async fn handle(&self, message: Box<dyn RecordEvent>) -> Result<(), anyhow::Error>;
}

#[async_trait::async_trait(?Send)]
impl<T> ProxyMessageHandler for T
where
    T: Fn(Box<dyn RecordEvent>) -> Pin<Box<dyn Future<Output = Result<(), anyhow::Error>>>>
        + 'static,
{
    async fn handle(&self, message: Box<dyn RecordEvent>) -> Result<(), anyhow::Error> {
        (self)(message).await
    }
}

pub(crate) trait ProxyStream: Stream<Item = ()> + 'static {}

impl<T> ProxyStream for T where T: Stream<Item = ()> + 'static {}

pub(crate) type BoxedProxyStream = Pin<Box<dyn ProxyStream>>;

pub(crate) struct Proxies {
    pub(crate) receivers: stream::SelectAll<BoxedProxyStream>,
    pub(crate) handlers: ProxyHandlers,
}

impl Proxies {
    pub(crate) fn default() -> Self {
        Self {
            receivers: stream::SelectAll::new(),
            handlers: ProxyHandlers::new(),
        }
    }

    pub(crate) fn add_proxy<S: ProxyMessageHandler + 'static>(
        &mut self,
        id: ProxyId,
        handler: S,
        stream: BoxedProxyStream,
    ) {
        self.handlers.add_handler(id, handler);
        self.receivers.push(stream);
    }
}

#[derive(Default)]
pub(crate) struct ProxyHandlers {
    handlers: HashMap<ProxyId, Box<dyn ProxyMessageHandler>>,
}

impl ProxyHandlers {
    pub(crate) fn new() -> Self {
        Self {
            handlers: HashMap::new(),
        }
    }

    pub(crate) fn add_handler<S: ProxyMessageHandler + 'static>(
        &mut self,
        id: ProxyId,
        handler: S,
    ) {
        self.handlers.insert(id, Box::new(handler));
    }

    pub(crate) fn get_handler(
        &self,
        id: ProxyId,
    ) -> Result<&dyn ProxyMessageHandler, anyhow::Error> {
        self.handlers
            .get(&id)
            .map(|b| b.as_ref())
            .ok_or(anyhow::anyhow!("Proxy {:?} not found", id))
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
        Pin::new(&mut self.get_mut().receivers).poll_next(cx)
    }
}

#[derive(Debug)]
pub enum Message<
    DownstreamInput: RecordEvent,
    DownstreamOutput: RecordEvent,
    UpstreamInput: RecordEvent,
> {
    Downstream(DownstreamMessage<DownstreamInput, DownstreamOutput>),
    Upstream(UpstreamMessage<UpstreamInput>),
}

#[derive(Debug)]
pub struct DownstreamMessage<Input: RecordEvent, Output: RecordEvent> {
    pub proxy_id: ProxyId,
    pub event: Input,
    pub response_tx: Option<oneshot::Sender<Output>>,
}

#[derive(Debug)]
pub struct UpstreamMessage<Input: RecordEvent> {
    pub(crate) proxy_id: ProxyId,
    pub(crate) event: Input,
}

pub struct Record<M> {
    pub proxy_id: ProxyId,
    pub event: M,
    pub reply_tx: oneshot::Sender<Result<(), anyhow::Error>>,
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
pub struct Sender<DIN: RecordEvent, DOUT: RecordEvent, UIN: RecordEvent> {
    pub id: ProxyId,
    pub tx: tokio::sync::mpsc::UnboundedSender<crate::processor::Message<DIN, DOUT, UIN>>,
}

impl<DIN: RecordEvent, DOUT: RecordEvent, UIN: RecordEvent> Sender<DIN, DOUT, UIN> {
    pub fn new(
        id: ProxyId,
        tx: tokio::sync::mpsc::UnboundedSender<crate::processor::Message<DIN, DOUT, UIN>>,
    ) -> Self {
        Self { id, tx }
    }
}
