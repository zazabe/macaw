use std::time::Duration;

use macaw::prelude::*;

fn main() -> Result<(), anyhow::Error> {
    unsafe { std::env::set_var("RUST_LOG", "debug") };
    tracing_subscriber::fmt::init();

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let local = tokio::task::LocalSet::new();
    local.block_on(&rt, async {
        let mut builder = MacawBuilder::new(LocalTokioExecutor, RecordScheduler::new());
        builder
            .add_http_proxy("127.0.0.1:8800".parse()?, "https://www.perdu.com/".parse()?)
            .await?;
        let mut task = builder.run();

        tokio::time::sleep(Duration::from_secs(10)).await;
        task.stop();
        tokio::time::sleep(Duration::from_secs(2)).await;

        Ok::<(), anyhow::Error>(())
    })?;
    Ok(())
}
