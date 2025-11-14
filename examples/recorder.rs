use macaw::core::*;
use macaw::http::*;

use tracing::info;

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    tracing_subscriber::fmt::init();

    let mut macaw = Macaw::recorder();
    macaw
        .add_http_proxy("http_demo", "127.0.0.1:8800", "https://www.perdu.com/")
        .await?;

    tokio::spawn({
        let handle = macaw.exit_handle();
        async move {
            wait_for_signal().await?;
            info!("Signal received. Aborting...");
            handle.exit();
            Ok::<(), anyhow::Error>(())
        }
    });

    if let Err(e) = macaw.record_when_exit("./data/record.yaml").await {
        macaw::handle_app_error(e);
    }
    Ok(())
}

async fn wait_for_signal() -> Result<(), anyhow::Error> {
    use futures::StreamExt;
    use tokio::signal::unix::SignalKind;
    use tokio::signal::unix::signal;
    use tokio_stream::wrappers::SignalStream;

    let signal_streams = vec![
        SignalStream::new(signal(SignalKind::terminate())?),
        SignalStream::new(signal(SignalKind::interrupt())?),
        SignalStream::new(signal(SignalKind::quit())?),
    ];
    let _ = futures::stream::select_all(signal_streams)
        .into_future()
        .await;
    Ok(())
}
