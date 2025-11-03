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
}

async fn handle_command(
    events: EventStore,
    handlers: &ProxyHandlers,
    kind: ReplayerCommand,
    reply_tx: oneshot::Sender<Result<(), anyhow::Error>>,
) -> Result<(), anyhow::Error> {
    let result = try_handle_command(events, handlers, kind).await;
    reply_tx
        .send(result)
        .map_err(|_| anyhow::anyhow!("Failed to send response"))?;
    Ok(())
}

async fn try_handle_command(
    events: EventStore,
    handlers: &ProxyHandlers,
    kind: ReplayerCommand,
) -> Result<(), anyhow::Error> {
    match kind {
        ReplayerCommand::Play => play(events, handlers).await,
    }
}

async fn play(events: EventStore, handlers: &ProxyHandlers) -> Result<(), anyhow::Error> {
    for event in events {
        let Event { proxy_id, data, .. } = event;
        let handler = handlers.get_handler(proxy_id)?;
        handler.handle(data).await?;
    }
    Ok(())
}

fn handle_proxy_message<P>(
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
    Box::pin(stream! {
        while let Some(message) = rx.recv().await {
            match process_proxy_message(message, &proxy).await {
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
            let outcome = proxy.downstream_incoming_process(request).await?;
            match (outcome, message.response_tx) {
                (Some(response), Some(response_tx)) => {
                    response_tx
                        .send(response)
                        .map_err(|_| anyhow::anyhow!("Failed to send response"))?;
                }
                (Some(response), None) => {
                    error!("Result returned but no response tx: {:?}", response);
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
            proxy.upstream_incoming_process(event).await?;
        }
    }
    Ok(())
}

#[async_trait::async_trait(?Send)]
impl Processor for Replayer {
    type Command = ReplayerCommand;
    async fn start(
        self,
        rx: mpsc::UnboundedReceiver<MacawCommand<ReplayerCommand>>,
    ) -> Result<(), anyhow::Error> {
        let Self { events, proxies } = self;
        let Proxies {
            receivers,
            handlers,
        } = proxies;

        let rx_stream = command_stream(rx, handlers, events);

        let proxies_stream: Pin<Box<dyn Stream<Item = ()>>> = Box::pin(receivers);
        let rx_stream: Pin<Box<dyn Stream<Item = ()>>> = Box::pin(rx_stream);
        let mut streams = stream::select(rx_stream, proxies_stream);
        while streams.next().await.is_some() {
            // Wait for the next event
        }
        Ok(())
    }
}

struct CommandStreamState {
    rx: mpsc::UnboundedReceiver<MacawCommand<ReplayerCommand>>,
    handlers: ProxyHandlers,
    events: EventStore,
}

fn command_stream(
    rx: mpsc::UnboundedReceiver<MacawCommand<ReplayerCommand>>,
    handlers: ProxyHandlers,
    events: EventStore,
) -> impl Stream<Item = ()> {
    futures::stream::unfold(
        CommandStreamState {
            rx,
            handlers,
            events,
        },
        |mut state| async move {
            let event = state.rx.recv().await?;
            let MacawCommand { kind, reply_tx } = event;
            let events = state.events.clone();
            if let Err(e) = handle_command(events, &state.handlers, kind, reply_tx).await {
                error!("Failed to handle command: {}", e);
            }
            Some(((), state))
        },
    )
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
        let proxy_stream = handle_proxy_message(rx, proxy.clone());

        let proxy_handle = move |message: Box<dyn RecordEvent>|  -> Pin<Box<dyn Future<Output = Result<(), anyhow::Error>>>> {
            let proxy = proxy.clone();
            Box::pin(async move {
                let downstream_message = <P as ProxyHandler>::Message::downcast(message)
                    .map_err(|e| anyhow::anyhow!("Failed to downcast message: {:?}", e))?;
                proxy.handle_message(downstream_message).await?;
                Ok(())
            })
        };
        self.proxies.add_proxy(proxy_id, proxy_handle, proxy_stream);
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
