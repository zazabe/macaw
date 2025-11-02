use anyhow::Result;
use async_stream::stream;
use futures::StreamExt;
use tokio::sync::{mpsc, oneshot};
use tracing::error;

use crate::io::fs::EventStore;
use crate::macaw::{MacawCommand, MacawCommandKind, Processor};
use crate::model::RecordEvent;
use crate::processor::proxy::*;

pub enum Message<
    DownstreamInput: RecordEvent,
    DownstreamOutput: RecordEvent,
    UpstreamInput: RecordEvent,
> {
    Downstream(DownstreamMessage<DownstreamInput, DownstreamOutput>),
    Upstream(UpstreamMessage<UpstreamInput>),
}

pub struct DownstreamMessage<Input: RecordEvent, Output: RecordEvent> {
    pub proxy_id: ProxyId,
    pub event: Input,
    pub response_tx: Option<oneshot::Sender<Output>>,
}

pub struct UpstreamMessage<Input: RecordEvent> {
    pub(crate) proxy_id: ProxyId,
    pub(crate) event: Input,
}

#[derive(Debug)]
pub struct Recorder {
    pub(crate) events: EventStore,
    pub(crate) proxies: Proxies,
}

impl Recorder {
    pub fn new() -> Self {
        Self {
            proxies: Proxies::default(),
            events: EventStore::new(),
        }
    }

    async fn handle_command(
        &mut self,
        kind: MacawCommandKind,
        reply_tx: oneshot::Sender<Result<(), anyhow::Error>>,
    ) -> Result<(), anyhow::Error> {
        let result = self.try_handle_command(kind).await;
        reply_tx
            .send(result)
            .map_err(|_| anyhow::anyhow!("Failed to send response"))?;
        Ok(())
    }

    async fn try_handle_command(&mut self, kind: MacawCommandKind) -> Result<(), anyhow::Error> {
        match kind {
            MacawCommandKind::Record(path) => self.events.save_file(path)?,
        }
        Ok(())
    }

    fn handle_proxy_message<P>(
        &self,
        mut rx: mpsc::UnboundedReceiver<
            Message<P::DownstreamInputMessage, P::DownstreamOutputMessage, P::UpstreamInputMessage>,
        >,
        proxy: P,
    ) -> BoxedProxyStream
    where
        P: Proxy,
        P::DownstreamInputMessage: RecordEvent + Clone,
        P::DownstreamOutputMessage: RecordEvent + Clone,
        P::UpstreamInputMessage: RecordEvent + Clone,
    {
        let events = self.events.clone();
        Box::pin(stream! {
            while let Some(message) = rx.recv().await {
                let events = events.clone();
                match process_proxy_message(events, message, &proxy).await {
                    Ok(()) => yield (),
                    Err(e) => {
                        // TODO: better error handling
                        error!("Failed to process proxy message: {}", e);
                        yield ();
                    }
                }
            }
        })
    }
}

async fn process_proxy_message<DIN, DOUT, UIN>(
    events: EventStore,
    message: Message<DIN, DOUT, UIN>,
    proxy: &dyn Proxy<
        DownstreamInputMessage = DIN,
        DownstreamOutputMessage = DOUT,
        UpstreamInputMessage = UIN,
    >,
) -> Result<(), anyhow::Error>
where
    DIN: RecordEvent + Clone,
    DOUT: RecordEvent + Clone,
    UIN: RecordEvent + Clone,
{
    match message {
        Message::Downstream(message) => {
            let request = proxy.redact_downstream_message(message.event).await?;
            events.push(message.proxy_id, request.clone());
            let result = proxy.process_downstream_message(request).await?;
            match (result, message.response_tx) {
                (Some(response), Some(response_tx)) => {
                    events.push(message.proxy_id, response.clone());
                    response_tx
                        .send(response)
                        .map_err(|_| anyhow::anyhow!("Failed to send response"))?;
                }
                (Some(result), None) => {
                    error!("Result returned but no response tx: {:?}", result);
                }
                (None, Some(response_tx)) => {
                    error!("Response expected not no result returned");
                }
                (None, None) => {
                    // No response expected
                }
            }
        }
        Message::Upstream(message) => {
            let event = proxy.redact_upstream_message(message.event).await?;
            events.push(message.proxy_id, event.clone());
            proxy.process_upstream_message(event).await?;
        }
    }
    Ok(())
}

impl Default for Recorder {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait(?Send)]
impl Processor for Recorder {
    fn add_proxy<P: Proxy>(
        &mut self,
        rx: mpsc::UnboundedReceiver<
            Message<P::DownstreamInputMessage, P::DownstreamOutputMessage, P::UpstreamInputMessage>,
        >,
        proxy: P,
    ) where
        P::DownstreamInputMessage: RecordEvent + Clone,
        P::DownstreamOutputMessage: RecordEvent + Clone,
        P::UpstreamInputMessage: RecordEvent + Clone,
    {
        let proxy_stream = self.handle_proxy_message(rx, proxy);
        self.proxies.add_proxy(proxy_stream);
    }

    async fn start(
        &mut self,
        mut rx: mpsc::UnboundedReceiver<MacawCommand>,
    ) -> Result<(), anyhow::Error> {
        loop {
            tokio::select! {
                Some(event) = rx.recv() => {
                    match event {
                        MacawCommand { kind, reply_tx } => {
                            self.handle_command(kind, reply_tx).await?;
                        }
                    }
                }
                _ = self.proxies.next() => {}
            }
        }
    }
}
