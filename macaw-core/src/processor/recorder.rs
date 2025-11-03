use crate::lib::*;

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
}

async fn handle_command(
    events: EventStore,
    kind: RecorderCommand,
    reply_tx: oneshot::Sender<Result<(), anyhow::Error>>,
) -> Result<(), anyhow::Error> {
    let result = try_handle_command(events, kind).await;
    reply_tx
        .send(result)
        .map_err(|_| anyhow::anyhow!("Failed to send response"))?;
    Ok(())
}

async fn try_handle_command(
    events: EventStore,
    kind: RecorderCommand,
) -> Result<(), anyhow::Error> {
    match kind {
        RecorderCommand::Record(path) => events.save_file(path)?,
    }
    Ok(())
}

fn handle_proxy_message<P>(
    events: EventStore,
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
    let events = events.clone();
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
    match message {
        Message::Downstream(message) => {
            let request = proxy.downstream_incoming_redact(message.event).await?;
            events.push(message.proxy_id, request.clone());
            let result = proxy.downstream_incoming_process(request).await?;
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
            let event = proxy.upstream_incoming_redact(message.event).await?;
            events.push(message.proxy_id, event.clone());
            proxy.upstream_incoming_process(event).await?;
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
    type Command = RecorderCommand;

    async fn start(
        self,
        rx: mpsc::UnboundedReceiver<MacawCommand<RecorderCommand>>,
    ) -> Result<(), anyhow::Error> {
        let Self { events, proxies } = self;

        let rx_stream =
            tokio_stream::wrappers::UnboundedReceiverStream::new(rx).then(move |event| {
                let events = events.clone();
                async move {
                    let MacawCommand { kind, reply_tx } = event;
                    if let Err(e) = handle_command(events, kind, reply_tx).await {
                        error!("Failed to handle command: {}", e);
                    }
                }
            });

        let proxies_stream: Pin<Box<dyn Stream<Item = ()>>> = Box::pin(proxies);
        let rx_stream: Pin<Box<dyn Stream<Item = ()>>> = Box::pin(rx_stream);
        let mut streams = stream::select_all([rx_stream, proxies_stream]);
        while streams.next().await.is_some() {
            // Wait for the next event
        }
        Ok(())
    }
}

impl Recorder {
    pub fn add_proxy<P: Proxy>(
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
        <P as ProxyDownstream>::IncomingMessage: RecordEvent + Clone,
        <P as ProxyDownstream>::OutgoingMessage: RecordEvent + Clone,
        <P as ProxyUpstream>::IncomingMessage: RecordEvent + Clone,
    {
        let proxy_id = proxy.id();
        let events = self.events.clone();
        let proxy_stream = handle_proxy_message(events, rx, proxy);
        let proxy_sender = |_| -> Pin<Box<dyn Future<Output = Result<(), anyhow::Error>>>> {
            Box::pin(futures::future::err(anyhow::anyhow!(
                "Proxy in recording mode are not expected to forward synthetic messages"
            )))
        };
        self.proxies.add_proxy(proxy_id, proxy_sender, proxy_stream);
    }
}

#[derive(Debug, Clone)]
pub enum RecorderCommand {
    Record(PathBuf),
}

impl ProcessorCommand for RecorderCommand {}

impl Macaw<RecorderCommand> {
    pub async fn record(&mut self, path: PathBuf) -> Result<(), anyhow::Error> {
        self.send_command(RecorderCommand::Record(path)).await
    }
}
