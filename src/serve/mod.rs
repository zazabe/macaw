mod api;
mod model;
mod transport;

use anyhow::{Context, Result, bail};
use clap::Args;
use macaw::session::{SessionManager, SessionMode, SessionState};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;
use transport::BoundControlListener;

const DEFAULT_TCP_ADDRESS: &str = "127.0.0.1:8080";
const SERVER_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);
const SESSION_DRAIN_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Debug, Args)]
pub struct ServeArgs {
    /// TCP address for the HTTP control API
    #[arg(long, value_name = "ADDRESS", conflicts_with = "unix")]
    tcp: Option<SocketAddr>,

    /// Unix socket path for the HTTP control API
    #[arg(long, value_name = "PATH", conflicts_with = "tcp")]
    unix: Option<PathBuf>,
}

pub async fn run(args: ServeArgs) -> Result<()> {
    run_until(args, shutdown_signal()).await
}

async fn run_until(
    args: ServeArgs,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<()> {
    let listener = bind(args).await?;
    eprintln!("macaw control server listening on {}", listener.address());

    let manager = SessionManager::start();
    let app = api::router(manager.clone());
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let mut server = tokio::spawn(listener.serve(app, async move {
        let _ = shutdown_rx.await;
    }));

    shutdown.await;
    manager.begin_shutdown().await?;
    let _ = shutdown_tx.send(());
    match tokio::time::timeout(SERVER_SHUTDOWN_TIMEOUT, &mut server).await {
        Ok(result) => result.context("control server task failed")??,
        Err(_) => {
            tracing::warn!("control connections did not close before shutdown deadline");
            server.abort();
            let _ = server.await;
        }
    }

    let outcomes = tokio::time::timeout(SESSION_DRAIN_TIMEOUT, manager.stop_all())
        .await
        .context("timed out draining sessions")??;
    let flush_failed = outcomes.iter().any(|result| match result {
        Ok(snapshot) => {
            matches!(snapshot.mode, SessionMode::Record { .. })
                && snapshot.state == SessionState::Failed
        }
        Err(_) => true,
    });

    tokio::time::timeout(SESSION_DRAIN_TIMEOUT, manager.shutdown())
        .await
        .context("timed out shutting down session manager")??;

    if flush_failed {
        bail!("one or more recording sessions failed to flush");
    }
    Ok(())
}

async fn bind(args: ServeArgs) -> Result<BoundControlListener> {
    if let Some(path) = args.unix {
        #[cfg(unix)]
        {
            return BoundControlListener::unix(&path);
        }
        #[cfg(not(unix))]
        {
            let _ = path;
            bail!("Unix control sockets are not supported on this platform");
        }
    }

    let address = match args.tcp {
        Some(address) => address,
        None => DEFAULT_TCP_ADDRESS
            .parse()
            .expect("default TCP address must be valid"),
    };
    BoundControlListener::tcp(address).await
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut terminate =
            signal(SignalKind::terminate()).expect("failed to install SIGTERM handler");
        tokio::select! {
            result = tokio::signal::ctrl_c() => {
                if let Err(error) = result {
                    tracing::error!("failed to listen for Ctrl-C: {error}");
                }
            }
            _ = terminate.recv() => {}
        }
    }

    #[cfg(not(unix))]
    {
        if let Err(error) = tokio::signal::ctrl_c().await {
            tracing::error!("failed to listen for Ctrl-C: {error}");
        }
    }
}
