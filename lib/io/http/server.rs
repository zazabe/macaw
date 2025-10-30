use crate::lib::*;
use crate::support::TokioIo;
use http::Response;
use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::{Service, service_fn};
use std::convert::Infallible;

pub(crate) struct HttpRequestEnvelope {
    pub(crate) request: HttpRequest,
    pub(crate) response_tx: oneshot::Sender<HttpResponse>,
}

#[dyn_clonable::clonable]
pub(crate) trait HttpServerSender: Clone + 'static {
    fn send(&self, envelope: HttpRequestEnvelope) -> Result<(), anyhow::Error>;
}

impl fmt::Debug for Box<dyn HttpServerSender> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "HttpServerSender")
    }
}

#[derive(Debug)]
pub(crate) struct HttpServer {
    addr: SocketAddr,
    tx: Box<dyn HttpServerSender>,
    local_addr: Option<SocketAddr>,
    task: Option<TokioTask>,
}

impl HttpServer {
    pub(crate) fn new(addr: SocketAddr, tx: Box<dyn HttpServerSender>) -> Self {
        Self {
            addr,
            tx,
            local_addr: None,
            task: None,
        }
    }

    pub(crate) async fn start<Executor>(&mut self, executor: Executor) -> Result<(), anyhow::Error>
    where
        Executor: TaskExecutor<
                Pin<Box<dyn std::future::Future<Output = Result<(), anyhow::Error>> + 'static>>,
            > + 'static,
    {
        let listener = TcpListener::bind(self.addr).await?;
        let local_addr = listener.local_addr()?;
        info!("Listening on http://{}", local_addr);
        self.local_addr = Some(local_addr);

        let task = executor.clone().execute(Box::pin({
            let exec = executor.clone();
            let sender = self.tx.clone();
            async move {
                loop {
                    let (stream, _) = listener.accept().await?;
                    let io = TokioIo::new(stream);
                    let handler = Handler {
                        sender: sender.clone(),
                    };
                    exec.execute(Box::pin(async move {
                        http1::Builder::new().serve_connection(io, handler).await?;
                        Ok(())
                    }));
                }
            }
        }));
        self.task = Some(task);
        Ok(())
    }

    pub(crate) fn stop(&mut self) -> Result<(), anyhow::Error> {
        if let Some(task) = self.task.take() {
            task.cancel();
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
struct Handler {
    sender: Box<dyn HttpServerSender>,
}

impl Service<http::Request<Incoming>> for Handler {
    type Response = HttpResponse;
    type Error = anyhow::Error;
    type Future =
        std::pin::Pin<Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>>>>;

    fn call(&self, req: http::Request<Incoming>) -> Self::Future {
        let sender = self.sender.clone();
        Box::pin(async move {
            let (parts, body) = req.into_parts();
            let bytes = body.collect().await?.to_bytes();
            let rebuilt_req = http::Request::from_parts(parts, BodyBytes::new(bytes));
            let (resp_tx, resp_rx) = tokio::sync::oneshot::channel();
            let env = HttpRequestEnvelope {
                request: rebuilt_req,
                response_tx: resp_tx,
            };
            sender
                .send(env)
                .map_err(|_| anyhow::anyhow!("Request channel closed"))?;
            let resp = resp_rx
                .await
                .map_err(|_| anyhow::anyhow!("oneshot cancelled"))?;
            Ok::<_, anyhow::Error>(resp)
        })
    }
}
