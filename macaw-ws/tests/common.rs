use futures::{SinkExt, StreamExt};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::{mpsc, oneshot};
use tokio_tungstenite::{connect_async, tungstenite::Message};

// ------------------------------------------------------------

/// Handle to an echo server that allows waiting for all connections to close.
#[allow(unused)]
pub struct EchoServerHandle {
    connection_count: Arc<AtomicUsize>,
}

impl EchoServerHandle {
    /// Waits until all active connections are dropped.
    #[allow(unused)]
    pub async fn wait_all_connections_disconnected(&self) {
        // Poll until connection count reaches zero
        while self.connection_count.load(Ordering::SeqCst) > 0 {
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        }
    }
}

/// Starts a simple WebSocket echo server that echoes back received messages.
/// Returns the address the server is listening on and a handle to wait for connections.
/// The server runs in the background and handles multiple connections.
#[allow(unused)]
pub async fn start_echo_server() -> Result<(std::net::SocketAddr, EchoServerHandle), anyhow::Error>
{
    use tokio::net::TcpListener;
    use tokio_tungstenite::accept_async;

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(|e| anyhow::anyhow!("Failed to bind to address: {}", e))?;
    let addr = listener
        .local_addr()
        .map_err(|e| anyhow::anyhow!("Failed to get local address: {}", e))?;

    let connection_count = Arc::new(AtomicUsize::new(0));
    let (ready_tx, ready_rx) = oneshot::channel();

    let conn_counter = connection_count.clone();

    // Spawn the server to handle connections in the background
    tokio::spawn(async move {
        // Signal that the server is ready
        let _ = ready_tx.send(());

        loop {
            match listener.accept().await {
                Ok((stream, _)) => {
                    let ws_stream = match accept_async(stream).await {
                        Ok(ws) => ws,
                        Err(e) => {
                            eprintln!("Failed to accept websocket connection: {}", e);
                            continue;
                        }
                    };
                    let (mut write, mut read) = ws_stream.split();

                    // Increment connection count
                    conn_counter.fetch_add(1, Ordering::SeqCst);
                    let conn_counter = conn_counter.clone();

                    // Handle each connection in a separate task
                    tokio::spawn(async move {
                        while let Some(msg) = read.next().await {
                            match msg {
                                Ok(Message::Text(text)) => {
                                    if write
                                        .send(Message::Text(format!("echo: {}", text).into()))
                                        .await
                                        .is_err()
                                    {
                                        break;
                                    }
                                }
                                Ok(Message::Binary(data)) => {
                                    if write.send(Message::Binary(data)).await.is_err() {
                                        break;
                                    }
                                }
                                Ok(Message::Close(_)) => {
                                    break;
                                }
                                Err(_) => break,
                                _ => {}
                            }
                        }
                        // Decrement connection count when connection closes
                        conn_counter.fetch_sub(1, Ordering::SeqCst);
                    });
                }
                Err(e) => {
                    eprintln!("Failed to accept connection: {}", e);
                    break;
                }
            }
        }
    });

    // Wait for the server to be ready using the barrier
    ready_rx
        .await
        .map_err(|e| anyhow::anyhow!("Server failed to start: {}", e))?;

    Ok((addr, EchoServerHandle { connection_count }))
}

// ------------------------------------------------------------

/// A flexible WebSocket test client that uses channels for bidirectional communication.
pub struct WsTestClient {
    send_tx: mpsc::UnboundedSender<Message>,
    recv_rx: mpsc::UnboundedReceiver<String>,
    _handle: tokio::task::JoinHandle<()>,
}

impl WsTestClient {
    /// Connects to a WebSocket server and returns a client handle.
    pub async fn connect(url: &str) -> Result<Self, anyhow::Error> {
        let (ws_stream, _) = connect_async(url).await?;
        let (mut write, mut read) = ws_stream.split();

        let (send_tx, mut send_rx) = mpsc::unbounded_channel::<Message>();
        let (recv_tx, recv_rx) = mpsc::unbounded_channel::<String>();

        // Spawn task to forward messages from channel to websocket
        let write_handle = tokio::spawn(async move {
            while let Some(msg) = send_rx.recv().await {
                if write.send(msg).await.is_err() {
                    break;
                }
            }
        });

        // Spawn task to forward messages from websocket to channel
        let read_handle = tokio::spawn(async move {
            while let Some(msg) = read.next().await {
                match msg {
                    Ok(Message::Text(text)) => {
                        if recv_tx.send(text.as_str().to_string()).is_err() {
                            break;
                        }
                    }
                    Ok(Message::Close(_)) => break,
                    Err(_) => break,
                    _ => {}
                }
            }
        });

        let _handle = tokio::spawn(async move {
            tokio::select! {
                _ = write_handle => {}
                _ = read_handle => {}
            }
        });

        Ok(Self {
            send_tx,
            recv_rx,
            _handle,
        })
    }

    /// Sends a text message to the server.
    pub async fn send(&mut self, text: &str) -> Result<(), anyhow::Error> {
        self.send_tx
            .send(Message::Text(text.to_string().into()))
            .map_err(|e| anyhow::anyhow!("Failed to send message: {}", e))?;
        Ok(())
    }

    /// Receives a text message from the server.
    pub async fn recv(&mut self) -> Result<String, anyhow::Error> {
        let message = self
            .recv_rx
            .recv()
            .await
            .ok_or_else(|| anyhow::anyhow!("Connection closed"))?;
        Ok(message)
    }

    /// Closes the WebSocket connection.
    pub async fn close(&mut self) -> Result<(), anyhow::Error> {
        self.send_tx
            .send(Message::Close(None))
            .map_err(|e| anyhow::anyhow!("Failed to close connection: {}", e))?;
        Ok(())
    }
}
