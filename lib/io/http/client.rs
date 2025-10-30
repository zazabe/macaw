use std::convert::Infallible;

use bytes::Bytes;
use http_body_util::combinators::BoxBody;
use rustls_platform_verifier::ConfigVerifierExt;

use crate::lib::*;
use crate::support::TaskExecutor;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper_util::client::legacy::{Client, connect::HttpConnector};

#[derive(Debug)]
pub(crate) struct HttpClient {
    client: Client<hyper_rustls::HttpsConnector<HttpConnector>, BodyBytes>,
}

impl HttpClient {
    pub(crate) fn new<Executor>(executor: Executor, addr: SocketAddr) -> Result<Self, anyhow::Error>
    where
        Executor: TaskExecutor,
    {
        let tls_config = rustls::ClientConfig::with_platform_verifier()?;
        let mut http = HttpConnector::new();
        http.enforce_http(false);
        let https = hyper_rustls::HttpsConnectorBuilder::new()
            .with_tls_config(tls_config)
            .https_or_http()
            .enable_all_versions()
            .wrap_connector(http);
        let executor = TaskExecutorWrapper::new(executor);
        let client = Client::builder(executor).build(https);
        Ok(Self { client })
    }

    pub(crate) async fn request<B>(
        &self,
        request: http::Request<B>,
    ) -> Result<HttpResponse, anyhow::Error>
    where
        B: BodyExt,
        B::Error: std::error::Error + Send + Sync + 'static,
    {
        let request = {
            let (parts, body) = request.into_parts();
            let bytes = body.collect().await?.to_bytes();
            HttpRequest::from_parts(parts, BodyBytes::new(bytes))
        };
        let response = {
            let res = self.client.request(request).await?;
            let (parts, body) = res.into_parts();
            let bytes = body.collect().await?.to_bytes();
            HttpResponse::from_parts(parts, BodyBytes::new(bytes))
        };
        Ok(response)
    }
}

#[derive(Clone)]
struct TaskExecutorWrapper<Executor> {
    executor: Executor,
}

impl<Executor> TaskExecutorWrapper<Executor>
where
    Executor: TaskExecutor,
{
    pub fn new(executor: Executor) -> Self {
        Self { executor }
    }
}

impl<Executor> fmt::Debug for TaskExecutorWrapper<Executor>
where
    Executor: TaskExecutor,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TaskExecutorWrapper")
    }
}

impl<Executor, Fut> hyper::rt::Executor<Fut> for TaskExecutorWrapper<Executor>
where
    Executor: TaskExecutor + Send + Sync + Clone + 'static,
    Fut: Future<Output = ()> + 'static,
{
    fn execute(&self, fut: Fut) {
        self.executor.execute(Box::pin(async move {
            fut.await;
            Ok(())
        }));
    }
}
