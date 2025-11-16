use crate::lib::*;

#[derive(Debug, Clone)]
pub(crate) struct WsUpstreamPeerEvent {
    pub(crate) peer_id: WsPeerId,
    pub(crate) event: WsEvent,
}

impl WsUpstreamPeerEvent {
    pub(crate) fn new(peer_id: WsPeerId, event: WsEvent) -> Self {
        Self { peer_id, event }
    }
}

impl WsPeerEventSender for Box<dyn ActorSender<WsUpstreamPeerEvent>> {
    fn send_event(&self, peer_id: WsPeerId, event: WsEvent) -> Result<(), anyhow::Error> {
        self.send(WsUpstreamPeerEvent::new(peer_id, event))
    }
}

#[derive(Debug, Clone)]
pub(crate) struct WsDownstreamPeerEvent {
    pub(crate) peer_id: WsPeerId,
    pub(crate) event: WsEvent,
}

impl WsDownstreamPeerEvent {
    pub(crate) fn new(peer_id: WsPeerId, event: WsEvent) -> Self {
        Self { peer_id, event }
    }
}

impl WsPeerEventSender for Box<dyn ActorSender<WsDownstreamPeerEvent>> {
    fn send_event(&self, peer_id: WsPeerId, event: WsEvent) -> Result<(), anyhow::Error> {
        self.send(WsDownstreamPeerEvent::new(peer_id, event))
    }
}
