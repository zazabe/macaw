use http_body_util::BodyExt;
use hyper_util::{
    client::legacy::{Client, connect::HttpConnector},
    rt::TokioExecutor,
};
use rustls_platform_verifier::ConfigVerifierExt;

use crate::lib::*;

#[derive(Debug, Clone)]
pub(crate) struct HttpClient {
    client: Client<hyper_rustls::HttpsConnector<HttpConnector>, BodyBytes>,
}

impl HttpClient {
    pub(crate) fn new(addr: SocketAddr) -> Result<Self, anyhow::Error> {
        let tls_config = rustls::ClientConfig::with_platform_verifier()?;
        let mut http = HttpConnector::new();
        http.enforce_http(false);
        let https = hyper_rustls::HttpsConnectorBuilder::new()
            .with_tls_config(tls_config)
            .https_or_http()
            .enable_all_versions()
            .wrap_connector(http);
        let client = Client::builder(TokioExecutor::new()).build(https);
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
