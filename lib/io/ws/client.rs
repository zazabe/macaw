use crate::lib::*;

#[derive(Debug)]
pub(crate) struct WsClient {
    addr: SocketAddr,
}

impl WsClient {
    pub(crate) fn new(addr: SocketAddr) -> Self {
        Self { addr }
    }

    pub(crate) async fn connect(&self, stream: TcpStream) -> Result<(), anyhow::Error> {
        Ok(())
    }
}
