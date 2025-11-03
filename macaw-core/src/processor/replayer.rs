use crate::lib::*;

#[derive(Debug)]
pub struct Replayer {
    pub(crate) events: EventStore,
    pub(crate) proxies: Proxies,
}

impl Replayer {
    pub fn new(path: PathBuf) -> Result<Self, anyhow::Error> {
        Ok(Self {
            events: EventStore::from_file(path)?,
            proxies: Proxies::default(),
        })
    }

    async fn handle_command(
        &mut self,
        kind: ReplayerCommand,
        reply_tx: oneshot::Sender<Result<(), anyhow::Error>>,
    ) -> Result<(), anyhow::Error> {
        let result = self.try_handle_command(kind).await;
        reply_tx
            .send(result)
            .map_err(|_| anyhow::anyhow!("Failed to send response"))?;
        Ok(())
    }

    async fn try_handle_command(&mut self, kind: ReplayerCommand) -> Result<(), anyhow::Error> {
        match kind {
            ReplayerCommand::Play => self.play().await,
        }
    }

    async fn play(&mut self) -> Result<(), anyhow::Error> {
        let events = self.events.clone();
        for event in events {
            let Event { proxy_id, data, .. } = event;
            let handler = self.proxies.get_handler(proxy_id)?;
            handler.handle(data).await?;
        }
        Ok(())
    }

    fn handle_proxy_message<P>(
        &self,
        mut rx: mpsc::UnboundedReceiver<
            Message<
                <P as ProxyDownstream>::IncomingMessage,
                <P as ProxyDownstream>::OutgoingMessage,
                <P as ProxyUpstream>::IncomingMessage,
            >,
        >,
        proxy: P,
    ) -> BoxedProxyStream
    where
        P: Proxy,
        <P as ProxyDownstream>::IncomingMessage: RecordEvent + Clone,
        <P as ProxyDownstream>::OutgoingMessage: RecordEvent + Clone,
        <P as ProxyUpstream>::IncomingMessage: RecordEvent + Clone,
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

async fn process_proxy_message<P, DIN, DOUT, UIN>(
    events: EventStore,
    message: Message<DIN, DOUT, UIN>,
    proxy: &P,
) -> Result<(), anyhow::Error>
where
    P: ProxyDownstream<IncomingMessage = DIN, OutgoingMessage = DOUT>
        + ProxyUpstream<IncomingMessage = UIN>,
    DIN: RecordEvent + Clone,
    DOUT: RecordEvent + Clone,
    UIN: RecordEvent + Clone,
{
    debug!("Proxy message: {:?}", message);

    match message {
        Message::Downstream(message) => {
            let request = proxy.downstream_incoming_redact(message.event).await?;
            let result = proxy.downstream_incoming_process(request).await?;
            match (result, message.response_tx) {
                (Some(response), Some(response_tx)) => {
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
            let event = proxy.upstream_incoming_redact(message.event).await?;
            events.push(message.proxy_id, event.clone());
            proxy.upstream_incoming_process(event).await?;
        }
    }
    Ok(())
}
#[async_trait::async_trait(?Send)]
impl Processor for Replayer {
    type Command = ReplayerCommand;
    async fn start(
        &mut self,
        mut rx: mpsc::UnboundedReceiver<MacawCommand<ReplayerCommand>>,
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

impl Replayer {
    pub fn add_proxy<P>(
        &mut self,
        rx: mpsc::UnboundedReceiver<
            Message<
                <P as ProxyDownstream>::IncomingMessage,
                <P as ProxyDownstream>::OutgoingMessage,
                <P as ProxyUpstream>::IncomingMessage,
            >,
        >,
        proxy: P,
    ) where
        P: Proxy + Clone,
        <P as ProxyDownstream>::IncomingMessage: RecordEvent + Clone,
        <P as ProxyDownstream>::OutgoingMessage: RecordEvent + Clone,
        <P as ProxyUpstream>::IncomingMessage: RecordEvent + Clone,
        <P as ProxyHandler>::Message: RecordEventUntagged,
    {
        let proxy_id = proxy.id();
        let proxy_stream = self.handle_proxy_message(rx, proxy.clone());
        let proxy_sender = move |message: Box<dyn RecordEvent>|  -> Pin<Box<dyn Future<Output = Result<(), anyhow::Error>>>> {
            let proxy = proxy.clone();
            Box::pin(async move {
                let downstream_message = <P as ProxyHandler>::Message::downcast(message)
                    .map_err(|e| anyhow::anyhow!("Failed to downcast message: {:?}", e))?;
                proxy.handle_message(downstream_message).await?;
                Ok(())
            })
        };
        self.proxies.add_proxy(proxy_id, proxy_sender, proxy_stream);
    }
}

#[derive(Debug, Clone)]
pub enum ReplayerCommand {
    Play,
}

impl ProcessorCommand for ReplayerCommand {}

impl Macaw<ReplayerCommand> {
    pub async fn play(&mut self) -> Result<(), anyhow::Error> {
        self.send_command(ReplayerCommand::Play).await
    }
}
