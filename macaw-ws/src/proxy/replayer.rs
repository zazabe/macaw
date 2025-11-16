use crate::lib::*;

#[derive(Debug)]
pub(crate) struct WsProxyReplayerActor {
    context: ActorContext,
    proxy_id: ProxyId,
    downstream: WsServer,
}

impl WsProxyReplayerActor {
    pub(crate) fn new(
        context: ActorContext,
        proxy_id: ProxyId,
        addr: SocketAddr,
        sender: ActorChannelSender<Self>,
    ) -> Result<Self, anyhow::Error> {
        let downstream_sender: Box<dyn ActorSender<WsDownstreamPeerEvent>> = Box::new(sender);
        let downstream = WsServer::new(addr, Box::new(downstream_sender));

        Ok(Self {
            context,
            proxy_id,
            downstream,
        })
    }

    pub(crate) async fn start(&self) -> Result<SocketAddr, anyhow::Error> {
        self.downstream.start(self.context()).await
    }

    pub(crate) async fn handle_downstream_event(
        &mut self,
        _peer_id: WsPeerId,
        _event: WsEvent,
    ) -> Result<(), anyhow::Error> {
        Ok(())
    }

    pub(crate) async fn handle_recorded_message(
        &mut self,
        _event: RecordedEventWithLock,
    ) -> Result<(), anyhow::Error> {
        Ok(())
    }
}

impl Actor for WsProxyReplayerActor {
    fn context(&self) -> &ActorContext {
        &self.context
    }
}

impl ProxyActor for WsProxyReplayerActor {
    fn proxy_id(&self) -> ProxyId {
        self.proxy_id
    }
}

impl ActorHandler<WsDownstreamPeerEvent> for WsProxyReplayerActor {
    type Reply = ();
    async fn handle(&mut self, event: WsDownstreamPeerEvent) {
        if let Err(e) = self
            .handle_downstream_event(event.peer_id, event.event)
            .await
        {
            error!("Failed to handle event: {:?}", e);
        }
    }
}

impl ActorHandler<RecordedEventWithLock> for WsProxyReplayerActor {
    type Reply = ();
    async fn handle(&mut self, event: RecordedEventWithLock) {
        if let Err(e) = self.handle_recorded_message(event).await {
            error!("Failed to handle recorded message: {:?}", e);
        }
    }
}

// ------------------------------------------------------------

#[derive(Debug)]
struct StartProxyEvent;

impl ActorHandler<StartProxyEvent> for WsProxyReplayerActor {
    type Reply = Result<SocketAddr, anyhow::Error>;
    async fn handle(&mut self, _event: StartProxyEvent) -> Result<SocketAddr, anyhow::Error> {
        self.start().await
    }
}

pub trait MacawWsReplayerSetup {
    fn add_ws_proxy(
        &mut self,
        proxy_id: &str,
        addr: &str,
    ) -> impl Future<Output = Result<SocketAddr, anyhow::Error>>;
}

impl MacawWsReplayerSetup for Macaw<Replayer> {
    async fn add_ws_proxy(
        &mut self,
        proxy_id: &str,
        addr: &str,
    ) -> Result<SocketAddr, anyhow::Error> {
        let (_proxy_id, handle) = self
            .add_proxy(move |_replayer, actor_context| {
                let (tx, rx) = actor_channel::<WsProxyReplayerActor>();
                let proxy_id: ProxyId = proxy_id.parse()?;
                let context = actor_context.create_child(&proxy_id.to_string());
                let addr: SocketAddr = addr.parse()?;
                let actor = WsProxyReplayerActor::new(context, proxy_id, addr, tx.clone())?;
                Ok((proxy_id, actor.run_with_channel(tx, rx)))
            })
            .await?;
        let listen_addr = handle.request(StartProxyEvent).await??;
        Ok(listen_addr)
    }
}
