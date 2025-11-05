use crate::lib::*;

pub struct ProxyReplayerActor<P: Proxy> {
    proxy: P,
}

impl<P: Proxy> ProxyReplayerActor<P> {
    pub fn new(proxy: P) -> Self {
        Self { proxy }
    }
}

impl<P: Proxy> Actor for ProxyReplayerActor<P> {}

impl<P: Proxy> ProxyActor for ProxyReplayerActor<P> {
    fn id(&self) -> ProxyId {
        self.proxy.id()
    }
}

/// Handle recorded events, processing them through ProxyHandler trait.
/// The replayer actor blocks the flow until a reply is received, allowing to wait
/// for the downstream incoming message (request) to be received and match the expected request.
impl<P: Proxy> ActorHandler<Record> for ProxyReplayerActor<P> {
    type Reply = Result<(), anyhow::Error>;

    async fn handle(&mut self, msg: Record) -> Self::Reply {
        self.handle_event(msg.event).await
    }
}

/// Handle downstream incoming messages.
/// The proxy implements the logic to match request/response.
/// Optionally, returns the response received from the replayer actor. (e.g. for HTTP proxy).
impl<P: Proxy> ActorHandler<DownstreamMessage<P>> for ProxyReplayerActor<P> {
    type Reply = Option<P::OutgoingMessage>;

    async fn handle(&mut self, msg: DownstreamMessage<P>) -> Self::Reply {
        debug!("Handling downstream message: {:?}", msg);
        match self.handle_downstream_incoming(msg.into_inner()).await {
            Err(e) => {
                error!("Failed to handle downstream incoming message: {}", e);
                None
            }
            Ok(response) => response,
        }
    }
}

impl<P: Proxy> ProxyReplayerActor<P> {
    async fn handle_event(&mut self, event: Box<dyn RecordEvent>) -> Result<(), anyhow::Error>
    where
        Recorder: ActorHandler<Record, Reply = ()>,
    {
        let message = <<P as ProxyHandler>::Message as RecordEventUntagged>::downcast(event)
            .map_err(|_| anyhow::anyhow!("Unexpected event"))?;
        debug!("Handling downstream message: {:?}", message);
        self.proxy.handle_message(message).await?;
        Ok(())
    }

    async fn handle_downstream_incoming(
        &mut self,
        msg: <P as ProxyDownstream>::IncomingMessage,
    ) -> Result<Option<P::OutgoingMessage>, anyhow::Error>
    where
        Recorder: ActorHandler<Record, Reply = ()>,
    {
        let request = self.proxy.downstream_incoming_redact(msg).await?;
        let response = self.proxy.downstream_incoming_process(request).await?;
        match response {
            Some(response) => {
                let response = self.proxy.downstream_outgoing_redact(response).await?;
                Ok(Some(response))
            }
            None => Ok(None),
        }
    }
}
