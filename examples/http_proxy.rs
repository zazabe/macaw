use std::convert::Infallible;
use std::net::SocketAddr;
use std::pin::Pin;
use std::str::FromStr;

use bytes::Bytes;
use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::service::Service;
use hyper::{Request, Response};

use hyper_util::client::legacy::{Client, connect::HttpConnector};
use tokio::net::TcpListener;

use macaw::support::TokioIo;

use rustls_platform_verifier::ConfigVerifierExt;

type ServerBuilder = hyper::server::conn::http1::Builder;
type HttpsClient = Client<hyper_rustls::HttpsConnector<HttpConnector>, BoxBody<Bytes, Infallible>>;

#[derive(structopt::StructOpt, Debug)]
pub struct CommandArgs {
    #[structopt(short = "p", long = "proxy-addr", default_value = "127.0.0.1:8100")]
    proxy_addr: String,
    #[structopt(
        short = "t",
        long = "target-url",
        default_value = "https://www.perdu.com/"
    )]
    target_url: String,
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    unsafe { std::env::set_var("RUST_LOG", "debug") };
    tracing_subscriber::fmt::init();

    let args: CommandArgs = structopt::StructOpt::from_args();

    let addr = SocketAddr::from_str(&args.proxy_addr)?;
    let target_url = http::Uri::from_str(&args.target_url)?;

    let listener = TcpListener::bind(addr).await?;
    println!("Listening on http://{}, Proxying to {}", addr, target_url);

    loop {
        let (stream, _) = listener.accept().await?;
        let io = TokioIo::new(stream);
        let target_url = target_url.clone();
        tokio::task::spawn(async move {
            let proxy = Proxy::new(target_url)?;
            if let Err(err) = ServerBuilder::new()
                .preserve_header_case(true)
                .title_case_headers(true)
                .serve_connection(io, proxy)
                .with_upgrades()
                .await
            {
                println!("Failed to serve connection: {:?}", err);
            }
            Ok::<(), anyhow::Error>(())
        });
    }
}

struct Proxy {
    scheme: http::uri::Scheme,
    authority: http::uri::Authority,
    client: HttpsClient,
}

impl Proxy {
    fn new(target_url: http::Uri) -> Result<Self, anyhow::Error> {
        let scheme = target_url
            .scheme()
            .cloned()
            .unwrap_or(http::uri::Scheme::HTTP);
        let authority = target_url
            .authority()
            .cloned()
            .ok_or(anyhow::anyhow!("No authority in target url"))?;
        let client = build_client()?;
        Ok(Self {
            scheme,
            authority,
            client,
        })
    }
}

impl Service<Request<Incoming>> for Proxy {
    type Response = Response<BoxBody<Bytes, Infallible>>;
    type Error = anyhow::Error;
    type Future =
        Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send + 'static>>;

    fn call(&self, req: Request<Incoming>) -> Self::Future {
        let client = self.client.clone();
        let uri = http::uri::Builder::from(req.uri().clone())
            .scheme(self.scheme.clone())
            .authority(self.authority.clone())
            .build()
            .unwrap();
        Box::pin(async move {
            let req = {
                let (mut parts, body) = req.into_parts();
                parts.uri = uri;
                let body_as_bytes = body.collect().await.unwrap().to_bytes();
                Request::from_parts(parts, Full::new(body_as_bytes).boxed())
            };
            let res = {
                let res = client.request(req).await?;
                let (parts, body) = res.into_parts();
                let body_as_bytes = Full::new(body.collect().await?.to_bytes()).boxed();
                Response::from_parts(parts, body_as_bytes)
            };
            Ok(res)
        })
    }
}

fn build_client() -> Result<HttpsClient, anyhow::Error> {
    let tls_config = rustls::ClientConfig::with_platform_verifier()?;
    let mut http = HttpConnector::new();
    http.enforce_http(false);
    let https = hyper_rustls::HttpsConnectorBuilder::new()
        .with_tls_config(tls_config)
        .https_or_http()
        .enable_all_versions()
        .wrap_connector(http);

    let client = Client::builder(hyper_util::rt::TokioExecutor::new()).build(https);
    Ok(client)
}
