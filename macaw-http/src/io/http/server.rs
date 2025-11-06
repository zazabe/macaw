use http_body_util::BodyExt;
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::Service;

use crate::lib::*;

#[dyn_clonable::clonable]
pub(crate) trait HttpServerRequestResolver: Send + Sync + Clone + 'static {
    fn resolve_request<'a>(
        &'a self,
        envelope: HttpRequest,
    ) -> Pin<Box<dyn Future<Output = Result<HttpResponse, anyhow::Error>> + Send + 'a>>;
}

impl fmt::Debug for Box<dyn HttpServerRequestResolver> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "HttpServerRequestResolver")
    }
}

#[derive(Debug)]
pub(crate) struct HttpServer {
    addr: SocketAddr,
    sender: Box<dyn HttpServerRequestResolver>,
    local_addr: Option<SocketAddr>,
    task: Option<tokio::task::JoinHandle<Result<(), anyhow::Error>>>,
}

impl HttpServer {
    pub(crate) fn new(addr: SocketAddr, sender: Box<dyn HttpServerRequestResolver>) -> Self {
        Self {
            addr,
            sender,
            local_addr: None,
            task: None,
        }
    }

    pub(crate) async fn start(&mut self) -> Result<(), anyhow::Error> {
        let listener = TcpListener::bind(self.addr).await?;
        let local_addr = listener.local_addr()?;
        info!("Listening on http://{}", local_addr);
        self.local_addr = Some(local_addr);

        let task = tokio::spawn(Box::pin({
            let sender = self.sender.clone();
            async move {
                loop {
                    let (stream, _) = listener.accept().await?;
                    let io = TokioIo::new(stream);
                    let service = RequestHandlerService {
                        sender: sender.clone(),
                    };
                    tokio::spawn(http1::Builder::new().serve_connection(io, service));
                }
            }
        }));
        self.task = Some(task);
        Ok(())
    }

    pub(crate) fn stop(&mut self) -> Result<(), anyhow::Error> {
        if let Some(task) = self.task.take() {
            task.abort();
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
struct RequestHandlerService {
    sender: Box<dyn HttpServerRequestResolver>,
}

impl Service<http::Request<Incoming>> for RequestHandlerService {
    type Response = HttpResponse;
    type Error = anyhow::Error;
    type Future = std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>,
    >;

    fn call(&self, req: http::Request<Incoming>) -> Self::Future {
        let sender = self.sender.clone();
        Box::pin(async move {
            let (parts, body) = req.into_parts();
            let bytes = body.collect().await?.to_bytes();
            let request = http::Request::from_parts(parts, BodyBytes::new(bytes));
            debug!("Received request: {:?}", request);
            let response = sender
                .resolve_request(request)
                .await
                .map_err(|_| anyhow::anyhow!("Request channel closed"))?;
            Ok(response)
        })
    }
}
