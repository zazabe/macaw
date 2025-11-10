use crate::lib::*;

pub struct ProxyReplayerActor<P: ProxyReplayer> {
    name: String,
    proxy: P,
}

impl<P: ProxyReplayer> ProxyReplayerActor<P> {
    pub fn new(proxy: P) -> Self {
        let name = format!("macaw:replayer:proxy:{}", proxy.id());
        Self { name, proxy }
    }
}

impl<P: ProxyReplayer> Actor for ProxyReplayerActor<P> {
    fn name(&self) -> &str {
        &self.name
    }
}

impl<P: ProxyReplayer> ProxyActor for ProxyReplayerActor<P> {
    fn id(&self) -> ProxyId {
        self.proxy.id()
    }
}

/// Handle recorded events, processing them through ProxyHandler trait.
/// The replayer actor blocks the flow until a reply is received, allowing to wait
/// for the downstream incoming message (request) to be received and match the expected request.
impl<P: ProxyReplayer> ActorHandler<RecordedEventWithLock> for ProxyReplayerActor<P> {
    type Reply = ();

    async fn handle(&mut self, event: RecordedEventWithLock) -> Self::Reply {
        let RecordedEventWithLock {
            event, replay_lock, ..
        } = event;
        if let Err(e) = self.handle_event(event, replay_lock).await {
            error!("Failed to handle recorded event: {:?}", e);
        }
    }
}

/// Handle downstream incoming messages.
/// The proxy implements the logic to match request/response.
/// Optionally, returns the response received from the replayer actor. (e.g. for HTTP proxy).
impl<P: ProxyReplayer>
    ActorHandler<
        DownstreamMessageWithResponseSender<
            P::DownstreamIncomingMessage,
            P::DownstreamOutgoingMessage,
        >,
    > for ProxyReplayerActor<P>
{
    type Reply = ();

    async fn handle(
        &mut self,
        downstream: DownstreamMessageWithResponseSender<
            P::DownstreamIncomingMessage,
            P::DownstreamOutgoingMessage,
        >,
    ) -> Self::Reply {
        let DownstreamMessageWithResponseSender {
            message,
            response_sender,
        } = downstream;
        if let Err(e) = self
            .handle_downstream_incoming(message, response_sender)
            .await
        {
            error!("Failed to handle downstream incoming message: {:?}", e);
        }
    }
}

impl<P: ProxyReplayer> ProxyReplayerActor<P> {
    async fn handle_event(
        &mut self,
        event: Box<dyn RecordEvent>,
        replay_lock: ReplayLockHolder,
    ) -> Result<(), anyhow::Error> {
        let message =
            P::RecordedMessage::downcast(event).map_err(|_| anyhow::anyhow!("Unexpected event"))?;

        debug!(
            "ProxyReplayerActor - Handling downstream message: {:?}",
            message
        );
        self.proxy
            .handle_recorded_message(message, replay_lock)
            .await?;
        Ok(())
    }

    async fn handle_downstream_incoming(
        &mut self,
        msg: P::DownstreamIncomingMessage,
        response_sender: ResponseSender<P::DownstreamOutgoingMessage>,
    ) -> Result<(), anyhow::Error> {
        debug!(
            "ProxyReplayerActor - Handling downstream incoming message: {:?}",
            msg
        );
        let request = self.proxy.downstream_incoming_redact(msg).await?;

        self.proxy
            .downstream_incoming_process(request, response_sender)
            .await?;
        Ok(())
    }
}
