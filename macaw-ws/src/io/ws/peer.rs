use std::sync::{Arc, Mutex};

use crate::lib::*;

use futures::{SinkExt, StreamExt, TryStreamExt};
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite;

#[dyn_clonable::clonable]
pub(crate) trait WsPeerEventSender: Send + Sync + Clone + 'static {
    fn send_event(&self, peer_id: WsPeerId, event: WsEvent) -> Result<(), anyhow::Error>;
}

impl fmt::Debug for Box<dyn WsPeerEventSender> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "WsPeerEventSender")
    }
}

// ------------------------------------------------------------

#[derive(Debug, Clone, Copy, Hash, Eq, PartialEq)]
pub(crate) struct WsPeerId(Uuid);

impl WsPeerId {
    pub(crate) fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl fmt::Display for WsPeerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", &self.0.simple().to_string()[..8])
    }
}

// ------------------------------------------------------------

pub(crate) struct WsPeerActor {
    peer_id: WsPeerId,
    context: ActorContext,
    sink: WsTlsSink,
    stream_handle: JoinHandle<TerminationReason<Result<(), anyhow::Error>>>,
}

impl WsPeerActor {
    pub(crate) async fn connect(
        peer_id: WsPeerId,
        context: ActorContext,
        sender: Box<dyn WsPeerEventSender>,
        request: HttpRequest,
    ) -> Result<Self, anyhow::Error> {
        debug!("Peer {} connecting to {:?}", peer_id, request);
        let (sink, stream) = connect(request.clone().into_request()?).await?;
        Self::new(peer_id, context, sender, request, sink, stream)
    }

    pub(crate) fn new(
        peer_id: WsPeerId,
        context: ActorContext,
        sender: Box<dyn WsPeerEventSender>,
        request: HttpRequest,
        sink: WsTlsSink,
        stream: WsTlsStream,
    ) -> Result<Self, anyhow::Error> {
        let stream_handle = context.spawn(&format!("ws-server-conn-stream-{}", peer_id), {
            let sender = sender.clone();
            async move {
                stream
                    .map(|message| {
                        debug!("Peer {} RX: {:?}", peer_id, message);
                        message
                            .map_err(anyhow::Error::from)
                            .and_then(WsMessage::try_from)
                            .map(WsEvent::message)
                    })
                    .try_for_each(|event| {
                        let sender = sender.clone();
                        async move {
                            sender.send_event(peer_id, event)?;
                            Ok::<(), anyhow::Error>(())
                        }
                    })
                    .await?;
                sender.send_event(peer_id, WsEvent::Disconnect)?;
                Ok::<(), anyhow::Error>(())
            }
        })?;
        sender.send_event(peer_id, WsEvent::open(request))?;
        Ok(Self {
            peer_id,
            context,
            sink,
            stream_handle,
        })
    }
}

impl Actor for WsPeerActor {
    fn context(&self) -> &ActorContext {
        &self.context
    }

    fn on_error(&mut self, error: anyhow::Error) {
        error!(
            "[{}] Peer {} error: {}",
            self.context().name(),
            self.peer_id,
            error
        );
    }

    async fn on_stop(&mut self, reason: ActorStopReason) {
        debug!(
            "[{}] Peer {} stopped: {:?}",
            self.context().name(),
            self.peer_id,
            reason
        );
        self.stream_handle.abort();
    }
}

impl ActorHandler<WsMessage> for WsPeerActor {
    type Reply = ();
    async fn handle(&mut self, message: WsMessage) {
        let message = tungstenite::Message::from(message);
        if let Err(e) = self.sink.send(message).await {
            error!("Failed to send message to ws server peer: {:?}", e);
        }
    }
}

impl fmt::Debug for WsPeerActor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "WsServerPeerActor {{ peer_id: {} }}", self.peer_id)
    }
}

// ------------------------------------------------------------

#[derive(Debug, Clone)]
pub(crate) struct WsPeers(Arc<Mutex<HashMap<WsPeerId, ActorHandle<WsPeerActor>>>>);

impl WsPeers {
    pub(crate) fn new() -> Self {
        Self(Default::default())
    }

    pub(crate) fn insert(&self, peer_id: WsPeerId, handle: ActorHandle<WsPeerActor>) {
        self.0.lock().unwrap().insert(peer_id, handle);
    }

    pub(crate) fn remove(&self, peer_id: WsPeerId) {
        self.0.lock().unwrap().remove(&peer_id);
    }

    pub(crate) fn get(&self, peer_id: WsPeerId) -> Result<ActorHandle<WsPeerActor>, anyhow::Error> {
        self.0
            .lock()
            .unwrap()
            .get(&peer_id)
            .cloned()
            .ok_or(anyhow::anyhow!("Connection {} not found", peer_id))
    }
}
