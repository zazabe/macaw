use std::net::SocketAddr;
use tokio::net::TcpStream;

#[derive(Debug)]
pub(crate) struct WsServer {
    addr: SocketAddr,
}

impl WsServer {
    pub(crate) fn new(addr: SocketAddr) -> Self {
        Self { addr }
    }

    pub(crate) async fn serve(&self, stream: TcpStream) -> Result<(), anyhow::Error> {
        Ok(())
    }
}
