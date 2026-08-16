use std::convert::Infallible;
use std::net::SocketAddr;

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use macaw::core::*;
use macaw::http::*;
use macaw::wasm::WasmHttpPlugin;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let component_path = std::env::args()
        .nth(1)
        .ok_or_else(|| anyhow::anyhow!("usage: wasm <component.wasm>"))?;
    let plugin = WasmHttpPlugin::from_file(
        component_path,
        serde_json::json!({ "signature": "signed-by-wasm" }),
    )?;
    let (echo_address, echo_server) = start_echo_server().await?;
    let upstream = format!("http://{echo_address}");

    let mut macaw = Macaw::recorder();
    let proxy = macaw
        .add_http_proxy(
            "wasm_http",
            "127.0.0.1:0",
            &upstream,
            HttpProxyOptions {
                transform: Box::new(plugin.transform()),
                redact: Box::new(plugin.redact()),
                ..Default::default()
            },
        )
        .await?;

    println!("WASM HTTP recorder listening on http://{proxy}");
    println!("Forwarding to the built-in echo server at {upstream}");
    println!("Try: curl -d 'hello from Macaw' http://{proxy}/demo");
    println!("Press Ctrl-C to save ./data/wasm-record.json and exit");

    tokio::signal::ctrl_c().await?;
    macaw.exit_handle().exit();
    echo_server.abort();
    std::fs::create_dir_all("./data")?;
    macaw.record_when_exit("./data/wasm-record.json").await?;
    Ok(())
}

async fn start_echo_server() -> Result<(SocketAddr, JoinHandle<()>), anyhow::Error> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let handle = tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                break;
            };
            tokio::spawn(async move {
                let connection =
                    http1::Builder::new().serve_connection(TokioIo::new(stream), service_fn(echo));
                if let Err(error) = connection.await {
                    eprintln!("echo server connection failed: {error}");
                }
            });
        }
    });
    Ok((address, handle))
}

async fn echo(request: Request<Incoming>) -> Result<Response<Full<Bytes>>, Infallible> {
    let (parts, body) = request.into_parts();
    let body = match body.collect().await {
        Ok(body) => body.to_bytes(),
        Err(error) => {
            return Ok(Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body(Full::new(Bytes::from(format!(
                    "failed to read request: {error}"
                ))))
                .unwrap());
        }
    };
    let headers = parts
        .headers
        .iter()
        .map(|(name, value)| {
            (
                name.as_str(),
                value.to_str().unwrap_or("<non-UTF-8 header value>"),
            )
        })
        .collect::<std::collections::BTreeMap<_, _>>();
    let payload = serde_json::json!({
        "method": parts.method.as_str(),
        "uri": parts.uri.to_string(),
        "headers": headers,
        "body": String::from_utf8_lossy(&body),
    });
    Ok(Response::builder()
        .header("content-type", "application/json")
        .body(Full::new(Bytes::from(
            serde_json::to_vec_pretty(&payload).unwrap(),
        )))
        .unwrap())
}
