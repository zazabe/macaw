use std::{borrow::Cow, marker::PhantomData};

use crate::lib::*;
use arrayvec::ArrayString;
use futures::future;

#[derive(Debug, Eq, PartialEq, Hash, Clone, Copy, Serialize, Deserialize)]
pub struct ProxyId(ArrayString<64>);

impl ProxyId {
    pub fn new(name: &str) -> Result<Self, anyhow::Error> {
        Ok(Self(ArrayString::from_str(name)?))
    }
}

impl fmt::Display for ProxyId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0.as_str())
    }
}

impl FromStr for ProxyId {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::new(s)
    }
}

#[derive(Debug)]
pub struct DownstreamMessageWithResponseSender<M, R> {
    pub(crate) message: M,
    pub(crate) response_sender: ResponseSender<R>,
}

impl<M, R> DownstreamMessageWithResponseSender<M, R> {
    pub fn new(message: M, response_sender: ResponseSender<R>) -> Self {
        Self {
            message,
            response_sender,
        }
    }
}

#[derive(Debug)]
pub struct DownstreamMessage<M> {
    pub(crate) message: M,
}

impl<M> DownstreamMessage<M> {
    pub fn new(message: M) -> Self {
        Self { message }
    }
}

#[derive(Debug)]
pub struct UpstreamMessage<M> {
    pub(crate) message: M,
}

impl<M> UpstreamMessage<M> {
    pub fn new(message: M) -> Self {
        Self { message }
    }
}

// ------------------------------------------------------------

pub trait ProxyRecorder: Send + 'static {
    type DownstreamIncomingMessage: RecordEvent + Clone;
    type DownstreamOutgoingMessage: RecordEvent + Clone;
    type UpstreamIncomingMessage: RecordEvent + Clone;

    fn id(&self) -> ProxyId;

    fn downstream_incoming_redact(
        &mut self,
        message: Self::DownstreamIncomingMessage,
    ) -> impl Future<Output = Result<Self::DownstreamIncomingMessage, anyhow::Error>> + Send {
        future::ok(message)
    }

    fn downstream_incoming_process(
        &mut self,
        message: Self::DownstreamIncomingMessage,
    ) -> impl Future<Output = Result<Option<Self::DownstreamOutgoingMessage>, anyhow::Error>> + Send
    {
        future::err(anyhow::anyhow!("Unexpected downstream message"))
    }

    fn downstream_outgoing_redact(
        &mut self,
        message: Self::DownstreamOutgoingMessage,
    ) -> impl Future<Output = Result<Self::DownstreamOutgoingMessage, anyhow::Error>> + Send {
        future::ok(message)
    }

    fn upstream_incoming_redact(
        &mut self,
        message: Self::UpstreamIncomingMessage,
    ) -> impl Future<Output = Result<Self::UpstreamIncomingMessage, anyhow::Error>> + Send {
        future::ok(message)
    }

    fn upstream_incoming_process(
        &mut self,
        message: Self::UpstreamIncomingMessage,
    ) -> impl Future<Output = Result<(), anyhow::Error>> + Send {
        future::ready(Err(anyhow::anyhow!("Unexpected upstream message")))
    }
}

pub trait ProxyReplayer: Send + 'static {
    type DownstreamIncomingMessage: RecordEvent + Clone;
    type DownstreamOutgoingMessage: RecordEvent + Clone;
    type RecordedMessage: RecordEventUntagged + Clone;

    fn id(&self) -> ProxyId;

    fn downstream_incoming_redact(
        &mut self,
        message: Self::DownstreamIncomingMessage,
    ) -> impl Future<Output = Result<Self::DownstreamIncomingMessage, anyhow::Error>> + Send {
        future::ok(message)
    }

    fn downstream_incoming_process(
        &mut self,
        message: Self::DownstreamIncomingMessage,
        response_sender: Option<ResponseSender<Self::DownstreamOutgoingMessage>>,
    ) -> impl Future<Output = Result<(), anyhow::Error>> + Send {
        future::err(anyhow::anyhow!("Unexpected downstream message"))
    }

    fn handle_recorded_message(
        &mut self,
        message: Self::RecordedMessage,
        replay_lock: ReplayLockHolder,
    ) -> impl Future<Output = Result<(), anyhow::Error>> + Send;
}
