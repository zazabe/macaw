use crate::lib::*;

pub struct ProxyRecorderActor<P: ProxyRecorder> {
    proxy: P,
    recorder: ActorHandle<Recorder>,
}

impl<P: ProxyRecorder> ProxyRecorderActor<P> {
    pub fn new(proxy: P, recorder: ActorHandle<Recorder>) -> Self {
        Self { proxy, recorder }
    }
}

impl<P: ProxyRecorder> Actor for ProxyRecorderActor<P> {}

impl<P: ProxyRecorder> ProxyActor for ProxyRecorderActor<P> {
    fn id(&self) -> ProxyId {
        self.proxy.id()
    }
}

/// Handle downstream incoming messages.
/// The proxy sends incoming message (request) to upstream and returns the outgoing message (response).
/// Redacted outgoing/incoming messages (request/response) are sent to the recorder actor to be stored.
impl<P: ProxyRecorder> ActorHandler<DownstreamMessage<P::DownstreamIncomingMessage>>
    for ProxyRecorderActor<P>
{
    type Reply = Option<P::DownstreamOutgoingMessage>;

    async fn handle(
        &mut self,
        downstream: DownstreamMessage<P::DownstreamIncomingMessage>,
    ) -> Self::Reply {
        match self.handle_downstream_incoming(downstream.message).await {
            Err(e) => {
                error!("Failed to handle downstream message: {}", e);
                None
            }
            Ok(response) => response,
        }
    }
}

/// Handle upstream incoming messages.
/// The proxy sends incoming message from upstream (e.g. client) to downstream (e.g. server).
/// Redacted incoming message is sent to the recorder actor to be stored.
impl<P: ProxyRecorder> ActorHandler<UpstreamMessage<P::UpstreamIncomingMessage>>
    for ProxyRecorderActor<P>
{
    type Reply = ();

    async fn handle(
        &mut self,
        upstream: UpstreamMessage<P::UpstreamIncomingMessage>,
    ) -> Self::Reply {
        if let Err(e) = self.handle_upstream_incoming(upstream.message).await {
            error!("Failed to handle upstream message: {}", e);
        }
    }
}

impl<P: ProxyRecorder> ProxyRecorderActor<P> {
    async fn handle_downstream_incoming(
        &mut self,
        msg: P::DownstreamIncomingMessage,
    ) -> Result<Option<P::DownstreamOutgoingMessage>, anyhow::Error> {
        let proxy_id = self.proxy.id();
        let request = self.proxy.downstream_incoming_redact(msg).await?;
        self.recorder
            .send(RecordedEvent::new(proxy_id, request.clone()))?;
        let response = self.proxy.downstream_incoming_process(request).await?;
        match response {
            Some(response) => {
                let response = self.proxy.downstream_outgoing_redact(response).await?;
                self.recorder
                    .send(RecordedEvent::new(proxy_id, response.clone()))?;
                Ok(Some(response))
            }
            None => Ok(None),
        }
    }

    async fn handle_upstream_incoming(
        &mut self,
        msg: P::UpstreamIncomingMessage,
    ) -> Result<(), anyhow::Error> {
        let proxy_id = self.proxy.id();
        let message = self.proxy.upstream_incoming_redact(msg).await?;
        self.recorder
            .send(RecordedEvent::new(proxy_id, message.clone()))?;
        self.proxy.upstream_incoming_process(message).await?;
        Ok(())
    }
}
