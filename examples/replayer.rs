use macaw::core::*;
use macaw::http::*;

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    tracing_subscriber::fmt::init();
    let context = AppContext::new();
    let mut macaw = Macaw::replayer(context.actor_context(), "./data/record.yaml")?;

    macaw.add_http_proxy("http_demo", "127.0.0.1:8800").await?;
    macaw.play()?;

    if let Err(e) = context.wait_until_stopped().await {
        macaw::handle_app_error(e);
    }
    drop(macaw);
    Ok(())
}
