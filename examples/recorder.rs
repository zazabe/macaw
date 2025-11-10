use std::{path::PathBuf, time::Duration};

use macaw::core::*;
use macaw::http::*;
use tokio::time::sleep;

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    tracing_subscriber::fmt::init();
    let context = AppContext::new();
    let actor_context = context.actor_context();

    tokio::spawn({
        let actor_context = actor_context.clone();
        async move {
            sleep(Duration::from_secs(10)).await;
            actor_context.exit();
        }
    });

    let mut macaw = Macaw::recorder(actor_context);
    macaw
        .add_http_proxy("http_demo", "127.0.0.1:8800", "https://www.perdu.com/")
        .await?;

    if let Err(e) = context.wait_until_stopped().await {
        macaw::handle_app_error(e);
    }
    macaw.record(PathBuf::from("./data/record.yaml")).await?;
    Ok(())
}
