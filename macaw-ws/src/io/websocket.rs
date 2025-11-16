use std::sync::{Arc, Mutex};

use async_stream::stream;
use futures::{
    Stream, StreamExt,
    stream::{SplitSink, SplitStream},
};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream,
    tungstenite::{
        self,
        handshake::{
            client::Request,
            server::{Callback, ErrorResponse, Response},
        },
        protocol::WebSocketConfig,
    },
};

pub(crate) type WsTlsStream = SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>;
pub(crate) type WsTlsSink =
    SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, tungstenite::Message>;

pub(crate) async fn connect(request: Request) -> Result<(WsTlsSink, WsTlsStream), anyhow::Error> {
    let config = Some(WebSocketConfig::default());
    let (ws_stream, _response) =
        tokio_tungstenite::connect_async_tls_with_config(request, config, true, None).await?;
    Ok(ws_stream.split())
}

pub(crate) async fn listen_tcp(
    listener: TcpListener,
) -> impl Stream<Item = Result<((WsTlsSink, WsTlsStream), Request), anyhow::Error>> {
    stream! {
        let config = Some(WebSocketConfig::default());
        loop {
            let (stream, _addr) = listener.accept().await?;
            let callback = RequestCallback::default();
            let ws_stream = tokio_tungstenite:: accept_hdr_async_with_config(MaybeTlsStream::Plain(stream), callback.clone(), config)
        .await?;
            let req = callback.get_request()?;
            yield Ok::<((WsTlsSink, WsTlsStream), Request), anyhow::Error>((ws_stream.split(), req))
        }
    }
}

#[derive(Clone, Default)]
struct RequestCallback(Arc<Mutex<Option<Request>>>);

impl RequestCallback {
    fn get_request(&self) -> Result<Request, anyhow::Error> {
        let req = self
            .0
            .lock()
            .unwrap()
            .clone()
            .ok_or_else(|| anyhow::anyhow!("No request found"))?;
        Ok(req)
    }
    fn set_request(&self, req: Request) {
        *self.0.lock().unwrap() = Some(req);
    }
}

impl Callback for RequestCallback {
    fn on_request(self, req: &Request, res: Response) -> Result<Response, ErrorResponse> {
        self.set_request(req.clone());
        Ok(res)
    }
}
