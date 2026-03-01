use futures::{SinkExt, StreamExt};
use std::sync::{Arc, Mutex};
use std::{collections::HashMap, time::Duration};
use tokio::sync::{mpsc, oneshot};
use tokio_tungstenite::{connect_async, tungstenite::Message};
// ------------------------------------------------------------

/// Unique identifier for a WebSocket connection.
pub type ConnectionId = usize;

/// Stores both the sender and received messages channel for a connection to keep them in sync.
struct Connection {
    client_tx: mpsc::UnboundedSender<Message>,
}

/// Manages WebSocket connections with their senders and received messages.
#[derive(Clone)]
struct Connections {
    inner: Arc<SharedConnections>,
}

impl Connections {
    fn new() -> Self {
        Self {
            inner: Arc::new(SharedConnections::new()),
        }
    }

    /// Adds a new connection and returns its ID.
    fn add_conn(
        &self,
        client_tx: mpsc::UnboundedSender<Message>,
    ) -> Result<ConnectionId, anyhow::Error> {
        let keys = self.inner.keys()?;
        let conn_id = keys.into_iter().max().map(|id| id + 1).unwrap_or(0);
        self.inner.insert(conn_id, Connection { client_tx })?;
        Ok(conn_id)
    }

    /// Sends a message to a specific connection.
    fn send_to(&self, conn_id: ConnectionId, message: &str) -> Result<(), anyhow::Error> {
        self.inner.with_connection(&conn_id, |conn| {
            conn.client_tx
                .send(Message::text(message))
                .map_err(|e| anyhow::anyhow!("Failed to send message: {}", e))
        })??;
        Ok(())
    }

    /// Sends a binary message to a specific connection.
    fn send_bytes_to(&self, conn_id: ConnectionId, data: &[u8]) -> Result<(), anyhow::Error> {
        self.inner.with_connection(&conn_id, |conn| {
            conn.client_tx
                .send(Message::Binary(data.to_vec().into()))
                .map_err(|e| anyhow::anyhow!("Failed to send message: {}", e))
        })??;
        Ok(())
    }

    /// Broadcasts a message to all connections, removing failed ones.
    fn broadcast(&self, message: &str) -> Result<usize, anyhow::Error> {
        let mut sent_count = 0;
        let mut failed_conns = Vec::new();
        let conn_ids = self.inner.keys()?;
        for conn_id in conn_ids {
            if self.send_to(conn_id, message).is_err() {
                failed_conns.push(conn_id);
            } else {
                sent_count += 1;
            }
        }
        for conn_id in failed_conns {
            self.inner.remove(&conn_id)?;
        }
        Ok(sent_count)
    }

    /// Removes a connection.
    fn remove(&self, conn_id: ConnectionId) -> Result<(), anyhow::Error> {
        self.inner.remove(&conn_id)
    }
}

#[derive(Debug)]
pub enum WsServerEvent {
    Message(WsServerMessage),
    Connected(ConnectionId),
    Disconnected(ConnectionId),
}

#[derive(Debug)]
pub struct WsServerMessage {
    pub conn_id: ConnectionId,
    pub message: Message,
}

impl WsServerMessage {
    #[allow(dead_code)]
    pub fn as_str(&self) -> Result<&str, anyhow::Error> {
        match &self.message {
            Message::Text(text) => Ok(text.as_str()),
            _ => Err(anyhow::anyhow!("Message is not a text message")),
        }
    }

    pub fn as_bytes(&self) -> Result<Vec<u8>, anyhow::Error> {
        match &self.message {
            Message::Binary(data) => Ok(data.to_vec()),
            _ => Err(anyhow::anyhow!("Message is not a binary message")),
        }
    }

    pub fn is_close(&self) -> bool {
        matches!(&self.message, Message::Close(_))
    }
}

impl PartialEq<(ConnectionId, &str)> for WsServerMessage {
    fn eq(&self, other: &(ConnectionId, &str)) -> bool {
        match &self.message {
            Message::Text(text) => self.conn_id == other.0 && text.as_str() == other.1,
            _ => false,
        }
    }
}

/// Handle to a test server that allows waiting for server events and sending messages to connections.
#[allow(unused)]
pub struct TestServerHandle {
    connections: Connections,
    server_rx: mpsc::UnboundedReceiver<WsServerEvent>,
}

impl TestServerHandle {
    /// Broadcasts a message to all active connections.
    #[allow(unused)]
    pub fn broadcast(&self, message: &str) -> Result<usize, anyhow::Error> {
        self.connections.broadcast(message)
    }

    /// Sends a message to a specific connection by ID.
    pub fn send_to(&self, conn_id: ConnectionId, message: &str) -> Result<(), anyhow::Error> {
        self.connections.send_to(conn_id, message)
    }

    /// Sends a binary message to a specific connection by ID.
    pub fn send_bytes_to(&self, conn_id: ConnectionId, data: &[u8]) -> Result<(), anyhow::Error> {
        self.connections.send_bytes_to(conn_id, data)
    }

    /// Waits for the next received text message event on the server.
    /// Returns an error if the timeout is exceeded.
    pub async fn recv_message(&mut self) -> Result<WsServerMessage, anyhow::Error> {
        let event = self.recv(Duration::from_secs(2)).await?;
        match event {
            WsServerEvent::Message(message) => Ok(message),
            _ => Err(anyhow::anyhow!(
                "Expected text message event, got {:?}",
                event
            )),
        }
    }

    /// Waits for a connection to be disconnected, either by a close message or a disconnect event.
    pub async fn assert_disconnected(
        &mut self,
        expected_conn_id: ConnectionId,
    ) -> Result<(), anyhow::Error> {
        let event = self.recv(Duration::from_secs(2)).await?;
        match event {
            WsServerEvent::Disconnected(conn_id) => {
                if conn_id == expected_conn_id {
                    Ok(())
                } else {
                    Err(anyhow::anyhow!(
                        "Expected disconnect event for connection {:?}, got connection {:?}",
                        expected_conn_id,
                        conn_id
                    ))
                }
            }
            WsServerEvent::Message(message) => {
                if message.is_close() && message.conn_id == expected_conn_id {
                    let event = self.recv(Duration::from_secs(2)).await?;
                    match event {
                        WsServerEvent::Disconnected(conn_id) => {
                            if conn_id == expected_conn_id {
                                Ok(())
                            } else {
                                Err(anyhow::anyhow!(
                                    "Expected disconnect event after close message for connection {:?}, got connection {:?}",
                                    expected_conn_id,
                                    conn_id
                                ))
                            }
                        }
                        _ => Err(anyhow::anyhow!("Expected disconnect after close event")),
                    }
                } else {
                    Err(anyhow::anyhow!("Expected close event"))
                }
            }
            _ => Err(anyhow::anyhow!(
                "Expected disconnect or close event, got {:?}",
                event
            )),
        }
    }

    /// Waits for the next connection event on the server.
    /// Returns an error if the timeout is exceeded.
    pub async fn recv_connect(&mut self) -> Result<ConnectionId, anyhow::Error> {
        let event = self.recv(Duration::from_secs(2)).await?;
        match event {
            WsServerEvent::Connected(conn_id) => Ok(conn_id),
            _ => Err(anyhow::anyhow!("Expected connected event, got {:?}", event)),
        }
    }

    /// Waits for the next received message event on the server.
    /// Returns an error if the timeout is exceeded.
    pub async fn recv(&mut self, timeout: Duration) -> Result<WsServerEvent, anyhow::Error> {
        tokio::select! {
            result = self.server_rx.recv() => {
                result.ok_or_else(|| anyhow::anyhow!("Server channel closed"))
            }
            _ = tokio::time::sleep(timeout) => {
                Err(anyhow::anyhow!("Timeout waiting for event after {:?}", timeout))
            }
        }
    }
}

/// Starts a simple WebSocket test server that tracks received messages.
/// Returns the address the server is listening on and a handle to wait for connections and check received messages.
/// The server runs in the background and handles multiple connections.
#[allow(unused)]
pub async fn start_test_server() -> Result<(std::net::SocketAddr, TestServerHandle), anyhow::Error>
{
    use tokio::net::TcpListener;
    use tokio_tungstenite::accept_async;

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(|e| anyhow::anyhow!("Failed to bind to address: {}", e))?;
    let addr = listener
        .local_addr()
        .map_err(|e| anyhow::anyhow!("Failed to get local address: {}", e))?;

    let connections = Connections::new();
    let (ready_tx, ready_rx) = oneshot::channel();
    let (server_tx, server_rx) = mpsc::unbounded_channel::<WsServerEvent>();

    // Spawn the server to handle connections in the background
    tokio::spawn({
        let connections = connections.clone();
        async move {
            // Signal that the server is ready
            let _ = ready_tx.send(());
            loop {
                match listener.accept().await {
                    Ok((stream, _)) => {
                        let ws_stream = match accept_async(stream).await {
                            Ok(ws) => ws,
                            Err(e) => {
                                panic!("Failed to accept websocket connection: {}", e);
                            }
                        };
                        let (mut write, mut read) = ws_stream.split();

                        // Create a channel for sending messages to this connection
                        let (send_tx, mut send_rx) = mpsc::unbounded_channel::<Message>();

                        // Handle each connection in a separate task
                        tokio::spawn({
                            let server_tx = server_tx.clone();
                            let conns = connections.clone();
                            async move {
                                let conn_id = conns.add_conn(send_tx).unwrap_or_else(|e| {
                                    panic!("Failed to add connection, error: {}", e)
                                });
                                server_tx.send(WsServerEvent::Connected(conn_id)).unwrap();

                                loop {
                                    tokio::select! {
                                        // Handle incoming messages from websocket
                                        msg_opt = read.next() => {
                                            match msg_opt {
                                                Some(Ok(message)) => {
                                                    let is_close_message = message.is_close();
                                                    if server_tx.send(WsServerEvent::Message(WsServerMessage { conn_id, message })).is_err() {
                                                        break;
                                                    }
                                                    if is_close_message {
                                                        break;
                                                    }
                                                }
                                                Some(Err(_)) => break,
                                                None => break,
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
                                conns.remove(conn_id).unwrap_or_else(|e| {
                                    panic!("Failed to remove connection, error: {}", e)
                                });
                                server_tx
                                    .send(WsServerEvent::Disconnected(conn_id))
                                    .unwrap();
                            }
                        });
                    }
                    Err(e) => {
                        panic!("Failed to accept connection: {}", e);
                    }
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
        TestServerHandle {
            connections,
            server_rx,
        },
    ))
}

// ------------------------------------------------------------

/// A flexible WebSocket test client that uses channels for bidirectional communication.
pub struct WsTestClient {
    send_tx: mpsc::UnboundedSender<Message>,
    recv_rx: mpsc::UnboundedReceiver<Message>,
    _handle: tokio::task::JoinHandle<()>,
}

impl WsTestClient {
    /// Connects to a WebSocket server and returns a client handle.
    pub async fn connect(url: &str) -> Result<Self, anyhow::Error> {
        let (ws_stream, _) = connect_async(url).await?;
        let (mut write, mut read) = ws_stream.split();

        let (send_tx, mut send_rx) = mpsc::unbounded_channel::<Message>();
        let (recv_tx, recv_rx) = mpsc::unbounded_channel::<Message>();
        let barrier = Arc::new(tokio::sync::Barrier::new(4));

        // Spawn task to forward messages from channel to websocket
        let write_handle = tokio::spawn({
            let barrier = barrier.clone();
            async move {
                barrier.wait().await;
                while let Some(msg) = send_rx.recv().await {
                    if write.send(msg).await.is_err() {
                        break;
                    }
                }
            }
        });

        // Spawn task to forward messages from websocket to channel
        let read_handle = tokio::spawn({
            let barrier = barrier.clone();
            async move {
                barrier.wait().await;
                while let Some(msg) = read.next().await {
                    match msg {
                        Ok(message) => match &message {
                            Message::Text(..) | Message::Binary(..) => {
                                if recv_tx.send(message).is_err() {
                                    break;
                                }
                            }
                            Message::Close(_) => break,
                            _ => {}
                        },
                        Err(_) => break,
                    }
                }
            }
        });

        let _handle = tokio::spawn({
            let barrier = barrier.clone();
            async move {
                barrier.wait().await;
                tokio::select! {
                    _ = write_handle => {}
                    _ = read_handle => {}
                }
            }
        });

        let _ = barrier.wait().await;

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

    /// Sends a binary message to the server.
    pub async fn send_bytes(&mut self, data: &[u8]) -> Result<(), anyhow::Error> {
        self.send_tx
            .send(Message::Binary(data.to_vec().into()))
            .map_err(|e| anyhow::anyhow!("Failed to send message: {}", e))?;
        Ok(())
    }

    pub async fn recv(&mut self) -> Result<Message, anyhow::Error> {
        let message = self
            .recv_rx
            .recv()
            .await
            .ok_or_else(|| anyhow::anyhow!("Connection closed"))?;
        Ok(message)
    }

    /// Receives a text message from the server.
    pub async fn recv_text(&mut self) -> Result<String, anyhow::Error> {
        match self.recv().await? {
            Message::Text(text) => Ok(text.as_str().to_string()),
            _ => Err(anyhow::anyhow!("Message is not a text message")),
        }
    }

    /// Receives a binary message from the server.
    pub async fn recv_bytes(&mut self) -> Result<Vec<u8>, anyhow::Error> {
        match self.recv().await? {
            Message::Binary(data) => Ok(data.to_vec()),
            _ => Err(anyhow::anyhow!("Message is not a binary message")),
        }
    }

    /// Closes the WebSocket connection.
    pub async fn close(&mut self) -> Result<(), anyhow::Error> {
        self.send_tx
            .send(Message::Close(None))
            .map_err(|e| anyhow::anyhow!("Failed to close connection: {}", e))?;
        Ok(())
    }
}

// ------------------------------------------------------------

struct SharedConnections {
    inner: Mutex<HashMap<ConnectionId, Connection>>,
}

impl SharedConnections {
    fn new() -> Self {
        Self {
            inner: Mutex::new(HashMap::new()),
        }
    }

    fn keys(&self) -> Result<Vec<ConnectionId>, anyhow::Error> {
        let inner = self
            .inner
            .lock()
            .map_err(|e| anyhow::anyhow!("Failed to lock slot map: {}", e))?;
        Ok(inner.keys().copied().collect())
    }

    fn insert(&self, key: ConnectionId, val: Connection) -> Result<(), anyhow::Error> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|e| anyhow::anyhow!("Failed to lock slot map: {}", e))?;
        inner.insert(key, val);
        Ok(())
    }

    fn remove(&self, key: &ConnectionId) -> Result<(), anyhow::Error> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|e| anyhow::anyhow!("Failed to lock slot map: {}", e))?;
        inner.remove(key);
        Ok(())
    }

    fn with_connection<F, R>(&self, key: &ConnectionId, f: F) -> Result<R, anyhow::Error>
    where
        F: FnOnce(&Connection) -> R,
    {
        let inner = self
            .inner
            .lock()
            .map_err(|e| anyhow::anyhow!("Failed to lock slot map: {}", e))?;
        let connection = inner
            .get(key)
            .ok_or_else(|| anyhow::anyhow!("Connection {:?} not found", key))?;
        let r = f(connection);
        Ok(r)
    }
}
