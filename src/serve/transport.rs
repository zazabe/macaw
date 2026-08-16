use anyhow::{Context, Result, bail};
use axum::Router;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use tokio::net::TcpListener;

#[cfg(unix)]
use std::os::unix::fs::{FileTypeExt, MetadataExt};
#[cfg(unix)]
use tokio::net::UnixListener;

#[derive(Debug)]
pub enum ControlListener {
    Tcp(TcpListener),
    #[cfg(unix)]
    Unix(UnixListener),
}

#[derive(Debug)]
pub struct BoundControlListener {
    listener: ControlListener,
    address: String,
    #[cfg(unix)]
    cleanup: Option<UnixSocketCleanup>,
}

impl BoundControlListener {
    pub async fn tcp(address: SocketAddr) -> Result<Self> {
        let listener = TcpListener::bind(address)
            .await
            .with_context(|| format!("failed to bind control server to {address}"))?;
        let address = listener.local_addr()?.to_string();
        Ok(Self {
            listener: ControlListener::Tcp(listener),
            address,
            #[cfg(unix)]
            cleanup: None,
        })
    }

    #[cfg(unix)]
    pub fn unix(path: &Path) -> Result<Self> {
        prepare_unix_path(path)?;
        let listener = UnixListener::bind(path)
            .with_context(|| format!("failed to bind Unix control socket {}", path.display()))?;
        let metadata = std::fs::symlink_metadata(path)?;
        let cleanup = UnixSocketCleanup {
            path: path.to_path_buf(),
            device: metadata.dev(),
            inode: metadata.ino(),
        };
        Ok(Self {
            listener: ControlListener::Unix(listener),
            address: path.display().to_string(),
            cleanup: Some(cleanup),
        })
    }

    pub fn address(&self) -> &str {
        &self.address
    }

    pub async fn serve(
        self,
        app: Router,
        shutdown: impl Future<Output = ()> + Send + 'static,
    ) -> Result<()> {
        let Self {
            listener,
            address: _,
            #[cfg(unix)]
            cleanup,
        } = self;
        #[cfg(unix)]
        let _cleanup = cleanup;

        match listener {
            ControlListener::Tcp(listener) => {
                axum::serve(listener, app)
                    .with_graceful_shutdown(shutdown)
                    .await?;
            }
            #[cfg(unix)]
            ControlListener::Unix(listener) => {
                axum::serve(listener, app)
                    .with_graceful_shutdown(shutdown)
                    .await?;
            }
        }
        Ok(())
    }
}

#[cfg(unix)]
fn prepare_unix_path(path: &Path) -> Result<()> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_socket() => {
            std::fs::remove_file(path).with_context(|| {
                format!("failed to remove stale Unix socket {}", path.display())
            })?;
        }
        Ok(_) => bail!(
            "refusing to replace non-socket file at Unix control path {}",
            path.display()
        ),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(error).with_context(|| {
                format!("failed to inspect Unix control path {}", path.display())
            });
        }
    }
    Ok(())
}

#[cfg(unix)]
#[derive(Debug)]
struct UnixSocketCleanup {
    path: PathBuf,
    device: u64,
    inode: u64,
}

#[cfg(unix)]
impl Drop for UnixSocketCleanup {
    fn drop(&mut self) {
        let Ok(metadata) = std::fs::symlink_metadata(&self.path) else {
            return;
        };
        if metadata.file_type().is_socket()
            && metadata.dev() == self.device
            && metadata.ino() == self.inode
        {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
    #[tokio::test]
    async fn unix_socket_cleanup_is_safe() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("control.sock");
        {
            let listener = BoundControlListener::unix(&path).unwrap();
            assert!(path.exists());
            drop(listener);
        }
        assert!(!path.exists());

        std::fs::write(&path, "keep me").unwrap();
        let error = BoundControlListener::unix(&path).unwrap_err();
        assert!(error.to_string().contains("refusing to replace non-socket"));
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "keep me");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn unix_listener_replaces_only_stale_sockets() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("stale.sock");
        drop(std::os::unix::net::UnixListener::bind(&path).unwrap());
        assert!(path.exists());

        let listener = BoundControlListener::unix(&path).unwrap();
        drop(listener);
        assert!(!path.exists());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn serves_the_same_http_router_over_unix_sockets() {
        use bytes::Bytes;
        use http_body_util::Empty;
        use hyper::Request;
        use hyper_util::rt::TokioIo;
        use macaw::session::SessionManager;

        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("api.sock");
        let listener = BoundControlListener::unix(&path).unwrap();
        let manager = SessionManager::start();
        let app = crate::serve::api::router(manager.clone());
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(listener.serve(app, async move {
            let _ = shutdown_rx.await;
        }));

        let stream = tokio::net::UnixStream::connect(&path).await.unwrap();
        let (mut sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
            .await
            .unwrap();
        tokio::spawn(connection);
        let response = sender
            .send_request(
                Request::builder()
                    .uri("/v1/health")
                    .body(Empty::<Bytes>::new())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), hyper::StatusCode::OK);

        shutdown_tx.send(()).unwrap();
        server.await.unwrap().unwrap();
        assert!(!path.exists());
        manager.shutdown().await.unwrap();
    }
}
