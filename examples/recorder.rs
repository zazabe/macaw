use std::{path::PathBuf, time::Duration};

use macaw::core::*;
use macaw::http::*;

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    unsafe { std::env::set_var("RUST_LOG", "debug") };
    tracing_subscriber::fmt::init();
    let mut macaw = Macaw::recorder();
    macaw
        .add_http_proxy("http_demo", "127.0.0.1:8800", "https://www.perdu.com/")
        .await?;

    tokio::time::sleep(Duration::from_secs(10)).await;
    macaw.record(PathBuf::from("./data/record.yaml")).await?;
    Ok(())
}
