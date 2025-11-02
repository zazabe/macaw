use std::{path::PathBuf, time::Duration};

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
        let mut setup = MacawSetup::new(LocalTokioExecutor, Recorder::new());
        setup
            .add_http_proxy("127.0.0.1:8800".parse()?, "https://www.perdu.com/".parse()?)
            .await?;
        let mut macaw = setup.start();

        tokio::time::sleep(Duration::from_secs(10)).await;
        macaw.record(PathBuf::from("./data/record.yaml")).await?;

        Ok::<(), anyhow::Error>(())
    })?;
    Ok(())
}
