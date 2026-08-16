use crate::lib::*;

#[derive(Debug)]
pub(crate) struct WsProxyRecorderActor {
    context: ActorContext,
    proxy_id: ProxyId,
    target_url: TargetUrl,
    downstream: WsServer,
    peers: WsPeers,
    recorder: ActorHandle<Recorder>,
    upstream_sender: Box<dyn ActorSender<WsUpstreamPeerEvent>>,
    options: WsProxyOptions,
}

impl Actor for WsProxyRecorderActor {
    fn context(&self) -> &ActorContext {
        &self.context
    }
}

impl ProxyActor for WsProxyRecorderActor {
    fn proxy_id(&self) -> ProxyId {
        self.proxy_id
    }
}

impl WsProxyRecorderActor {
    fn new(
        context: ActorContext,
        proxy_id: ProxyId,
        addr: SocketAddr,
        target_url: TargetUrl,
        recorder: ActorHandle<Recorder>,
        sender: ActorChannelSender<Self>,
        options: WsProxyOptions,
    ) -> Result<Self, anyhow::Error> {
        let downstream_sender: Box<dyn ActorSender<WsDownstreamPeerEvent>> =
            Box::new(sender.clone());
        let upstream_sender: Box<dyn ActorSender<WsUpstreamPeerEvent>> = Box::new(sender);
        let downstream = WsServer::new(addr, Box::new(downstream_sender));
        let peers = WsPeers::new();
        Ok(Self {
            context,
            proxy_id,
            target_url,
            downstream,
            peers,
            recorder,
            upstream_sender,
            options,
        })
    }

    async fn start(&self) -> Result<SocketAddr, anyhow::Error> {
        self.downstream.start(self.context()).await
    }

    fn record_gate(&self, peer_id: WsPeerId, event: WsEvent) -> Result<(), anyhow::Error> {
        self.recorder.send(RecordedEvent::new(
            self.proxy_id,
            LogicalStreamId::new("ws", peer_id.to_string()),
            ReplayRole::Gate,
            WsRecordedEvent::new(peer_id, event),
        ))
    }

    fn record_emission(&self, peer_id: WsPeerId, event: WsEvent) -> Result<(), anyhow::Error> {
        self.recorder.send(RecordedEvent::new(
            self.proxy_id,
            LogicalStreamId::new("ws", peer_id.to_string()),
            ReplayRole::Emit,
            WsRecordedEvent::new(peer_id, event),
        ))
    }

    async fn handle_downstream_event(
        &mut self,
        peer_id: WsPeerId,
        event: WsEvent,
    ) -> Result<(), anyhow::Error> {
        debug!(
            "Received downstream event for peer {}: {:?}",
            peer_id, event
        );
        let event_decoded = self.options.transform.decode_event(event)?;
        let event_overridden = self
            .options
            .overrides
            .ws_downstream_override_event(event_decoded);
        if let Some(event_overridden) = event_overridden {
            let event_redacted = self
                .options
                .redact
                .ws_redact_event(event_overridden.clone());
            self.record_gate(peer_id, event_redacted)?;
            let event_encoded = self.options.transform.encode_event(event_overridden)?;
            match event_encoded {
                WsEvent::Message(message_event) => {
                    self.peers.get(peer_id)?.send(message_event.message)?;
                }
                WsEvent::Open(event) => {
                    println!("request:\n{:?}", event.request);
                    println!("target:\n{:?}", self.target_url);

                    let request = event.request.update(&self.target_url)?;
                    let context = self
                        .context
                        .create_child(&format!("peer-upstream-{}", peer_id));
                    let sender = Box::new(self.upstream_sender.clone());
                    let peer = WsPeerActor::connect(peer_id, context, sender, request).await?;
                    let peer_handle = peer.run();
                    self.peers.insert(peer_id, peer_handle);
                }
                WsEvent::Disconnect => {
                    let peer = self.peers.get(peer_id)?;
                    peer.stop();
                    self.peers.remove(peer_id);
                }
            }
        }
        Ok(())
    }

    async fn handle_upstream_event(
        &mut self,
        peer_id: WsPeerId,
        event: WsEvent,
    ) -> Result<(), anyhow::Error> {
        debug!("Received upstream event for peer {}", peer_id);

        match event {
            WsEvent::Message(message_event) => {
                let event_decoded = self
                    .options
                    .transform
                    .decode_event(WsEvent::Message(message_event))?;

                let event_overridden = self
                    .options
                    .overrides
                    .ws_upstream_override_event(event_decoded);

                if let Some(event_overridden) = event_overridden {
                    self.record_emission(peer_id, event_overridden.clone())?;
                    let encoded_event = self.options.transform.encode_event(event_overridden)?;
                    if let WsEvent::Message(message_event) = encoded_event {
                        self.downstream.send(peer_id, message_event.message)?;
                    }
                }
            }
            WsEvent::Open(..) => {
                // Upstream peer open received
            }
            WsEvent::Disconnect => {
                self.downstream.disconnect(peer_id)?;
            }
        }

        Ok(())
    }
}

impl ActorHandler<WsUpstreamPeerEvent> for WsProxyRecorderActor {
    type Reply = ();
    async fn handle(&mut self, event: WsUpstreamPeerEvent) {
        if let Err(e) = self.handle_upstream_event(event.peer_id, event.event).await {
            error!("Failed to handle event: {:?}", e);
        }
    }
}

impl ActorHandler<WsDownstreamPeerEvent> for WsProxyRecorderActor {
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

// ------------------------------------------------------------

#[derive(Debug)]
struct StartProxyEvent;

impl ActorHandler<StartProxyEvent> for WsProxyRecorderActor {
    type Reply = Result<SocketAddr, anyhow::Error>;
    async fn handle(&mut self, _event: StartProxyEvent) -> Result<SocketAddr, anyhow::Error> {
        self.start().await
    }
}

pub trait MacawWsRecorderSetup {
    fn add_ws_proxy(
        &mut self,
        proxy_id: &str,
        addr: &str,
        target_url: &str,
        options: WsProxyOptions,
    ) -> impl Future<Output = Result<SocketAddr, anyhow::Error>>;
}

impl MacawWsRecorderSetup for Macaw<Recorder> {
    async fn add_ws_proxy(
        &mut self,
        proxy_id: &str,
        addr: &str,
        target_url: &str,
        options: WsProxyOptions,
    ) -> Result<SocketAddr, anyhow::Error> {
        let (_proxy_id, handle) = self.add_proxy(move |recorder, actor_context| {
            let proxy_id: ProxyId = proxy_id.parse()?;
            let addr: SocketAddr = addr.parse()?;
            let target_url: TargetUrl = target_url.parse()?;
            let context = actor_context.create_child(&proxy_id.to_string());
            let (tx, rx) = actor_channel::<WsProxyRecorderActor>();
            let actor = WsProxyRecorderActor::new(
                context,
                proxy_id,
                addr,
                target_url,
                recorder.clone(),
                tx.clone(),
                options,
            )?;
            Ok((proxy_id, actor.run_with_channel(tx, rx)))
        })?;
        let listen_addr = handle.request(StartProxyEvent).await??;
        Ok(listen_addr)
    }
}
