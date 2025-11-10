use macaw::core::*;
use macaw::http::*;

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    unsafe { std::env::set_var("RUST_LOG", "debug") };
    tracing_subscriber::fmt::init();

    let mut macaw = Macaw::replayer(AppContext::new(), "./data/record.yaml")?;
    macaw.add_http_proxy("http_demo", "127.0.0.1:8800").await?;
    macaw.play().await?;

    Ok(())
}
