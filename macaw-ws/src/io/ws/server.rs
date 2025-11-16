use futures::StreamExt;
use std::pin::pin;
use tokio::net::TcpListener;

use crate::lib::*;

#[derive(Debug)]
pub(crate) struct WsServer {
    addr: SocketAddr,
    sender: Box<dyn WsPeerEventSender>,
    peers: WsPeers,
}

impl WsServer {
    pub(crate) fn new(addr: SocketAddr, sender: Box<dyn WsPeerEventSender>) -> Self {
        Self {
            addr,
            sender,
            peers: WsPeers::new(),
        }
    }

    pub(crate) fn send(&self, peer_id: WsPeerId, message: WsMessage) -> Result<(), anyhow::Error> {
        let conn = self.peers.get(peer_id)?;
        conn.send(message)
    }

    pub(crate) fn disconnect(&self, peer_id: WsPeerId) -> Result<(), anyhow::Error> {
        let conn = self.peers.get(peer_id)?;
        conn.stop();
        self.peers.remove(peer_id);
        Ok(())
    }

    pub(crate) async fn start(&self, context: &ActorContext) -> Result<SocketAddr, anyhow::Error> {
        let listener = TcpListener::bind(self.addr).await?;
        let local_addr = listener.local_addr()?;
        info!("Listening on ws://{}", local_addr);

        context.spawn("ws-server", {
            let sender = self.sender.clone();
            let peers = self.peers.clone();
            let context = context.clone();
            async move {
                let context = context.clone();
                let mut conns = pin!(listen_tcp(listener).await);
                while let Some(Ok(((sink, stream), request))) = conns.next().await {
                    let peer_id = WsPeerId::new();
                    let peer_actor = WsPeerActor::new(
                        peer_id,
                        context.create_child(&format!("peer-downstream-{}", peer_id)),
                        sender.clone(),
                        HttpRequest::from_request(&request)?,
                        sink,
                        stream,
                    )?;
                    let peer_handle = peer_actor.run();
                    peers.insert(peer_id, peer_handle);
                }
                Ok::<(), anyhow::Error>(())
            }
        })?;
        Ok(local_addr)
    }
}
