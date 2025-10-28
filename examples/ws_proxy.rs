use std::{net::SocketAddr, str::FromStr};

use futures::{SinkExt, StreamExt, TryStreamExt};
use log::*;
use tokio::net::TcpListener;

#[derive(structopt::StructOpt, Debug)]
pub struct CommandArgs {
    #[structopt(short = "p", long = "proxy-addr", default_value = "127.0.0.1:8800")]
    proxy_addr: String,
    #[structopt(
        short = "t",
        long = "target-url",
        default_value = "wss://fstream.binance.com/"
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
    let authority = target_url
        .authority()
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("No authority in target url"))?;
    let scheme = target_url
        .scheme()
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("No scheme in target url"))?;

    let listener = TcpListener::bind(addr).await?;
    println!("Listening on ws://{}, Proxying to {}", addr, target_url);

    let mut connections = Box::pin(macaw::io::listen_tcp(listener).await);
    while let Some(((mut conn_sink, mut conn_stream), mut req)) = connections.try_next().await? {
        let authority = authority.clone();
        let scheme = scheme.clone();
        tokio::task::spawn(async move {
            let uri = http::uri::Builder::from(req.uri().clone())
                .authority(authority.clone())
                .scheme(scheme)
                .build()?;
            *req.uri_mut() = uri;
            req.headers_mut()
                .insert(http::header::HOST, authority.to_string().parse()?);
            req.headers_mut().remove("sec-websocket-extensions");
            debug!("Connecting to proxy, request: {:?}", req);
            let (mut proxy_sink, mut proxy_stream) = macaw::io::connect(req).await?;

            tokio::task::spawn(async move {
                while let Some(message) = conn_stream.next().await {
                    debug!("Received message from client: {:?}", message);
                    let message = message?;
                    proxy_sink.send(message).await?;
                }
                Ok::<(), anyhow::Error>(())
            });

            tokio::task::spawn(async move {
                while let Some(message) = proxy_stream.next().await {
                    debug!("Received message from proxy: {:?}", message);
                    let message = message?;
                    conn_sink.send(message).await?;
                }
                Ok::<(), anyhow::Error>(())
            });

            Ok::<(), anyhow::Error>(())
        });
    }
    Ok(())
}
