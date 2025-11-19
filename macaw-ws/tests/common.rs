use futures::{SinkExt, StreamExt};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::{Mutex, mpsc, oneshot};
use tokio_tungstenite::{connect_async, tungstenite::Message};

// ------------------------------------------------------------

/// Unique identifier for a WebSocket connection.
pub type ConnectionId = usize;

/// Handle to an echo server that allows waiting for all connections to close.
#[allow(unused)]
pub struct EchoServerHandle {
    connections: Arc<Mutex<HashMap<ConnectionId, mpsc::UnboundedSender<Message>>>>,
    next_conn_id: Arc<AtomicUsize>,
}

impl EchoServerHandle {
    /// Waits until the connection count reaches the given value.
    #[allow(unused)]
    pub async fn wait_conn_count(&self, count: usize) {
        // Poll until connection count reaches the given value
        while self.connections.lock().await.len() != count {
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        }
    }

    /// Broadcasts a message to all active connections.
    #[allow(unused)]
    pub async fn broadcast(&self, message: &str) -> Result<usize, anyhow::Error> {
        let connections = self.connections.lock().await;
        let mut sent_count = 0;
        let mut failed_conns = Vec::new();
        for (conn_id, sender) in connections.iter() {
            if sender.send(Message::text(message)).is_err() {
                failed_conns.push(*conn_id);
            } else {
                sent_count += 1;
            }
        }

        // Clean up failed connections
        if !failed_conns.is_empty() {
            drop(connections);
            let mut connections = self.connections.lock().await;
            for conn_id in failed_conns {
                connections.remove(&conn_id);
            }
        }

        Ok(sent_count)
    }

    /// Sends a message to a specific connection by ID.
    #[allow(unused)]
    pub async fn send_to(&self, conn_id: ConnectionId, message: &str) -> Result<(), anyhow::Error> {
        let mut connections = self.connections.lock().await;
        match connections.get(&conn_id) {
            Some(sender) => {
                if sender.send(Message::text(message)).is_err() {
                    // Connection closed, remove it
                    connections.remove(&conn_id);
                    Err(anyhow::anyhow!("Connection {} closed", conn_id))
                } else {
                    Ok(())
                }
            }
            None => Err(anyhow::anyhow!("Connection {} not found", conn_id)),
        }
    }

    /// Returns a list of all active connection IDs.
    #[allow(unused)]
    pub async fn list_connections(&self) -> Vec<ConnectionId> {
        let connections = self.connections.lock().await;
        connections.keys().copied().collect()
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

    let connections = Arc::new(Mutex::new(HashMap::<
        ConnectionId,
        mpsc::UnboundedSender<Message>,
    >::new()));
    let next_conn_id = Arc::new(AtomicUsize::new(0));
    let (ready_tx, ready_rx) = oneshot::channel();

    let connections_clone = connections.clone();
    let next_conn_id_clone = next_conn_id.clone();

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

                    // Assign a unique connection ID
                    let conn_id = next_conn_id_clone.fetch_add(1, Ordering::SeqCst);

                    // Create a channel for sending messages to this connection
                    let (send_tx, mut send_rx) = mpsc::unbounded_channel::<Message>();

                    // Register the connection
                    {
                        let mut conns = connections_clone.lock().await;
                        conns.insert(conn_id, send_tx);
                    }

                    let connections_clone = connections_clone.clone();

                    // Handle each connection in a separate task
                    tokio::spawn(async move {
                        loop {
                            tokio::select! {
                                // Handle incoming messages from websocket
                                msg_opt = read.next() => {
                                    match msg_opt {
                                        Some(Ok(Message::Text(text))) => {
                                            if write
                                                .send(Message::Text(format!("echo: {}", text).into()))
                                                .await
                                                .is_err()
                                            {
                                                break;
                                            }
                                        }
                                        Some(Ok(Message::Binary(data))) => {
                                            if write.send(Message::Binary(data)).await.is_err() {
                                                break;
                                            }
                                        }
                                        Some(Ok(Message::Close(_))) => {
                                            break;
                                        }
                                        Some(Err(_)) => break,
                                        None => break,
                                        _ => {}
                                    }
                                }
                                // Handle outgoing messages from channel
                                msg_opt = send_rx.recv() => {
                                    match msg_opt {
                                        Some(msg) => {
                                            if write.send(msg).await.is_err() {
                                                break;
                                            }
                                        }
                                        None => break,
                                    }
                                }
                            }
                        }
                        // Unregister the connection
                        {
                            let mut conns = connections_clone.lock().await;
                            conns.remove(&conn_id);
                        }
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

    Ok((
        addr,
        EchoServerHandle {
            connections,
            next_conn_id,
        },
    ))
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
