use crate::lib::*;

#[derive(Debug)]
pub(crate) struct WsProxyReplayerActor {
    context: ActorContext,
    proxy_id: ProxyId,
    downstream: WsServer,
    events: WsPendingEvents,
    options: WsProxyOptions,
}

impl WsProxyReplayerActor {
    fn new(
        context: ActorContext,
        proxy_id: ProxyId,
        addr: SocketAddr,
        sender: ActorChannelSender<Self>,
        options: WsProxyOptions,
    ) -> Result<Self, anyhow::Error> {
        let downstream_sender: Box<dyn ActorSender<WsDownstreamPeerEvent>> = Box::new(sender);
        let downstream = WsServer::new(addr, Box::new(downstream_sender));

        Ok(Self {
            context,
            proxy_id,
            downstream,
            events: WsPendingEvents::new(),
            options,
        })
    }

    async fn start(&self) -> Result<SocketAddr, anyhow::Error> {
        self.downstream.start(self.context()).await
    }

    async fn handle_downstream_event(
        &mut self,
        peer_id: WsPeerId,
        event: WsEvent,
    ) -> Result<(), anyhow::Error> {
        let event_decoded = self.options.transform.decode_event(event)?;
        let event_overridden = self
            .options
            .overrides
            .ws_downstream_override_event(event_decoded);
        if let Some(event_overridden) = event_overridden {
            let event_redacted = self.options.redact.ws_redact_event(event_overridden);
            let event_replay = self.events.remove_replay(peer_id, &event_redacted);
            match event_replay {
                Some(event_replay) => {
                    // replay event found, release the lock
                    event_replay.lock.release();
                }
                None => {
                    // Wait for the downstream event to be replayed
                    self.events.insert_downstream(peer_id, event_redacted);
                }
            }
        }
        Ok(())
    }

    async fn handle_recorded_message(
        &mut self,
        record: RecordedEventWithLock,
    ) -> Result<(), anyhow::Error> {
        let RecordedEventWithLock {
            event, replay_lock, ..
        } = record;

        match event.downcast::<WsUpstreamEvent>() {
            Ok(event) => {
                self.handle_recorded_upstream_message(*event, replay_lock)
                    .await
            }
            Err(event) => match event.downcast::<WsDownstreamEvent>() {
                Ok(event) => {
                    self.handle_recorded_downstream_message(*event, replay_lock)
                        .await
                }
                Err(event) => Err(anyhow::anyhow!(
                    "Failed to downcast to WsEvent: {:?}",
                    event
                )),
            },
        }
    }

    async fn handle_recorded_upstream_message(
        &mut self,
        record: WsUpstreamEvent,
        replay_lock: ReplayLockHolder,
    ) -> Result<(), anyhow::Error> {
        // upstream messages should already have a connection established
        // the replay can be released immediately
        replay_lock.release();
        let peer_id = record.peer_id;
        let encoded_event = self.options.transform.encode_event(record.event)?;
        match encoded_event {
            WsEvent::Message(message_event) => {
                self.downstream.send(peer_id, message_event.message)?;
            }
            WsEvent::Open(..) => {
                // ignored, upstream open is not recorded
            }
            WsEvent::Disconnect => {
                self.downstream.disconnect(peer_id)?;
            }
        }
        Ok(())
    }

    async fn handle_recorded_downstream_message(
        &mut self,
        record: WsDownstreamEvent,
        replay_lock: ReplayLockHolder,
    ) -> Result<(), anyhow::Error> {
        // block replay until the downstream message is received
        let downstream_event = self.events.remove_downstream(record.peer_id, &record.event);
        match downstream_event {
            Some(..) => {
                replay_lock.release();
            }
            None => {
                self.events
                    .insert_replay(record.peer_id, record.event, replay_lock);
            }
        }
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
struct WsPendingDownstreamEvent {
    peer_id: WsPeerId,
    event: WsEvent,
}

#[derive(Debug)]
struct WsPendingReplayEvent {
    peer_id: WsPeerId,
    event: WsEvent,
    lock: ReplayLockHolder,
}

#[derive(Debug)]
struct WsPendingEvents {
    downstream: Vec<WsPendingDownstreamEvent>,
    replay: Vec<WsPendingReplayEvent>,
}

impl WsPendingEvents {
    fn new() -> Self {
        Self {
            downstream: Vec::new(),
            replay: Vec::new(),
        }
    }

    fn remove_downstream(
        &mut self,
        peer_id: WsPeerId,
        event: &WsEvent,
    ) -> Option<WsPendingDownstreamEvent> {
        self.downstream
            .iter()
            .position(|e| e.peer_id == peer_id && e.event.fingerprint() == event.fingerprint())
            .map(|index| self.downstream.remove(index))
    }

    fn remove_replay(
        &mut self,
        peer_id: WsPeerId,
        event: &WsEvent,
    ) -> Option<WsPendingReplayEvent> {
        self.replay
            .iter()
            .position(|e| e.peer_id == peer_id && e.event.fingerprint() == event.fingerprint())
            .map(|index| self.replay.remove(index))
    }

    fn insert_downstream(&mut self, peer_id: WsPeerId, event: WsEvent) {
        self.downstream
            .push(WsPendingDownstreamEvent { peer_id, event });
    }

    fn insert_replay(&mut self, peer_id: WsPeerId, event: WsEvent, lock: ReplayLockHolder) {
        self.replay.push(WsPendingReplayEvent {
            peer_id,
            event,
            lock,
        });
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
        options: WsProxyOptions,
    ) -> impl Future<Output = Result<SocketAddr, anyhow::Error>>;
}

impl MacawWsReplayerSetup for Macaw<Replayer> {
    async fn add_ws_proxy(
        &mut self,
        proxy_id: &str,
        addr: &str,
        options: WsProxyOptions,
    ) -> Result<SocketAddr, anyhow::Error> {
        let (_proxy_id, handle) = self
            .add_proxy(move |_replayer, actor_context| {
                let (tx, rx) = actor_channel::<WsProxyReplayerActor>();
                let proxy_id: ProxyId = proxy_id.parse()?;
                let context = actor_context.create_child(&proxy_id.to_string());
                let addr: SocketAddr = addr.parse()?;
                let actor =
                    WsProxyReplayerActor::new(context, proxy_id, addr, tx.clone(), options)?;
                Ok((proxy_id, actor.run_with_channel(tx, rx)))
            })
            .await?;
        let listen_addr = handle.request(StartProxyEvent).await??;
        Ok(listen_addr)
    }
}
