use macaw::core::*;
use macaw::http::*;
use macaw::ws::*;

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    console_subscriber::init();

    let mut macaw = Macaw::replayer("./data/record.json")?;
    macaw
        .add_http_proxy("http_demo", "127.0.0.1:8800", HttpProxyOptions::default())
        .await?;
    macaw
        .add_ws_proxy("ws_demo", "127.0.0.1:8801", WsProxyOptions::default())
        .await?;
    macaw.play()?;
    if let Err(e) = macaw.wait_until_stopped().await {
        macaw::handle_app_error(e);
    }
    Ok(())
}
