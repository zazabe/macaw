use anyhow::Result;
use tokio::sync::{mpsc, oneshot};
use tracing::error;

use crate::io::fs::EventStore;
use crate::macaw::{MacawCommand, MacawCommandKind, MacawInterface};
use crate::model::RecordEvent;
use crate::proxy::{Proxies, Proxy, ProxyId};

pub enum Message {
    Downstream(DownstreamMessage),
    Upstream(UpstreamMessage),
}

pub struct DownstreamMessage {
    pub proxy_id: ProxyId,
    pub event: Box<dyn RecordEvent>,
    pub response_tx: Option<oneshot::Sender<Box<dyn RecordEvent>>>,
}

pub struct UpstreamMessage {
    pub(crate) proxy_id: ProxyId,
    pub(crate) event: Box<dyn RecordEvent>,
}

#[derive(Debug)]
pub struct Recorder {
    pub(crate) tx: mpsc::UnboundedSender<Message>,
    pub(crate) rx: mpsc::UnboundedReceiver<Message>,
    pub(crate) events: EventStore,
    pub(crate) proxies: Proxies,
}

impl Recorder {
    pub fn new() -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        Self {
            tx,
            rx,
            proxies: Proxies::default(),
            events: EventStore::new(),
        }
    }
}

impl Default for Recorder {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait(?Send)]
impl MacawInterface for Recorder {
    fn add_proxy<P: Proxy + 'static>(&mut self, proxy: P) {
        self.proxies.insert_proxy(proxy);
    }

    fn sender(&self) -> &mpsc::UnboundedSender<Message> {
        &self.tx
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
                Some(message) = self.rx.recv() => match message {
                    Message::Downstream(message) => {
                        let proxy = self.proxies.get_proxy(&message.proxy_id)?;
                        let request_event = proxy.redact_downstream_message(message.event).await?;
                        self.events.push(message.proxy_id, request_event.clone());
                        let result = proxy.process_downstream_message(request_event).await?;
                        match (result, message.response_tx) {
                            (Some(response_event), Some(response_tx)) => {
                                self.events.push(message.proxy_id, response_event.clone());
                                response_tx.send(response_event).map_err(|_| anyhow::anyhow!("Failed to send response"))?;
                            }
                            (_, _) => {
                                error!("Failed to process downstream message");
                            }
                        }

                    }
                    Message::Upstream(message) => {
                        let proxy = self.proxies.get_proxy(&message.proxy_id)?;
                        let event = proxy.redact_upstream_message(message.event).await?;
                        self.events.push(message.proxy_id, event.clone());
                        proxy.process_upstream_message(event).await?;
                    }
                }
            }
        }
    }
}

impl Recorder {
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
}
