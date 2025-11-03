use std::path::PathBuf;

use macaw::core::*;
use macaw::http::*;

fn main() -> Result<(), anyhow::Error> {
    unsafe { std::env::set_var("RUST_LOG", "debug") };
    tracing_subscriber::fmt::init();

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let local = tokio::task::LocalSet::new();
    local.block_on(&rt, async {
        let mut setup = MacawSetup::new(
            LocalTokioExecutor,
            Replayer::new(PathBuf::from("./data/record.yaml"))?,
        );
        setup.add_http_proxy("http_demo", "127.0.0.1:8800").await?;
        let mut macaw = setup.start();

        macaw.play().await?;

        Ok::<(), anyhow::Error>(())
    })?;
    Ok(())
}
