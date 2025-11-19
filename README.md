# Macaw

A record/replay tool for different protocols, built with Rust using an actor-based architecture.

## Summary

Macaw is a protocol-agnostic record/replay framework that allows you to:
- **Record** network traffic between clients and servers by proxying connections
- **Replay** recorded sessions without requiring the original upstream servers

The tool supports multiple protocols (HTTP, WebSocket, and extensible to others) and provides a clean separation between protocol-specific logic and the core recording/replay engine.

## Usage

### Recording

Create a recorder instance, add proxies for the protocols you want to record, and save the recording when done:

```rust
use macaw::core::*;
use macaw::http::*;
use macaw::ws::*;

let mut macaw = Macaw::recorder();

// Add HTTP proxy: listens on 127.0.0.1:8800, forwards to https://www.perdu.com/
macaw.add_http_proxy("http_demo", "127.0.0.1:8800", "https://www.perdu.com/")
    .await?;

// Add WebSocket proxy: listens on 127.0.0.1:8801, forwards to wss://fstream.binance.com/
macaw.add_ws_proxy("ws_demo", "127.0.0.1:8801", "wss://fstream.binance.com/")
    .await?;

// Handle exit signal (e.g., Ctrl+C)
tokio::spawn({
    let handle = macaw.exit_handle();
    async move {
        wait_for_signal().await?;
        handle.exit();
        Ok::<(), anyhow::Error>(())
    }
});

// Save recording when exit signal is received
macaw.record_when_exit("./data/record.json").await?;
```

### Replaying

Load a recording file and replay it through proxies:

```rust
use macaw::core::*;
use macaw::http::*;
use macaw::ws::*;

// Load recording from file
let mut macaw = Macaw::replayer("./data/record.json")?;

// Add proxies (no upstream URL needed in replay mode)
macaw.add_http_proxy("http_demo", "127.0.0.1:8800").await?;
macaw.add_ws_proxy("ws_demo", "127.0.0.1:8801").await?;

// Start replaying
macaw.play()?;

// Wait until replay completes
macaw.wait_until_stopped().await?;
```

## Building

```bash
cargo build --release
```

## Running Examples

```bash
# Run recorder example
cargo run --example recorder

# Run replayer example
cargo run --example replayer
```

## Architecture

### Terminology

- **Downstream**: The client side (clients connect "down" to the proxy).
  In recording mode, downstream clients connect to the proxy, and their traffic is recorded. In replay mode, downstream clients connect and receive replayed responses.

- **Upstream**: The server side (the proxy forwards "up" to the origin server).
  The proxy forwards downstream requests to upstream servers and records both the requests and responses.

### macaw-core

The `macaw-core` crate provides the generic recording/replay infrastructure:

- **Actor System**: Core actor traits, messaging, and lifecycle management
- **Recorder**: Collects `RecordedEvent` messages from proxies through channels and stores them in an `EventStore`. When requested, it writes all events to a JSON file.
- **Replayer**: Loads recorded events from a JSON file and distributes them to registered proxies via channels. It manages replay locks to ensure events are replayed in the correct order.
- **Event Model**: Generic `RecordEvent` trait that protocol-specific events implement, allowing type-erased storage and replay
- **Storage**: File I/O for reading/writing recording files in JSON format

Key components:
- `Recorder`: Actor that receives `RecordedEvent` messages and saves them
- `Replayer`: Actor that loads events and sends `RecordedEventWithLock` to proxies
- `RecordedEvent`: Wraps a protocol-specific event with a `proxy_id`
- `RecordedEventWithLock`: Includes a replay lock to coordinate replay timing

#### Actor Model

Macaw uses an actor-based architecture where:

- **Actors** are independent components that communicate through channels
- **Messages** are sent via `send()` for fire-and-forget operations
- **Requests** are sent via `request()` for operations that require a response
- Each actor runs in its own task and processes messages sequentially
- Actors can spawn child actors and manage their lifecycle

The actor system provides:
- Type-safe message passing through channels
- Request/reply patterns for synchronous operations
- Graceful shutdown and error handling
- Lifecycle management (start, stop, error handling)


### macaw-*

Protocol-specific crates (e.g., `macaw-http`, `macaw-ws`) implement:

- **Protocol Logic**: Handle protocol-specific details (parsing, framing, etc.)
- **Data Schema**: Define protocol-specific event types that implement `RecordEvent`
- **Proxy Actors**: Implement `ProxyActor` trait and handle both recording and replay modes

#### Recording Mode

```mermaid
flowchart RL
    Recorder[Recorder Actor]
    File[(Recording<br/>File)]
    Upstream[Upstream<br/>Server]
    Proxy[Proxy Actor]
    Client[Downstream<br/>Client]

    Client -->|Request| Proxy
    Proxy -->|Request| Upstream
    Upstream -->|Response| Proxy
    Proxy -->|RecordedEvent| Recorder
    Proxy -->|Response| Client

    Recorder -.->|WriteToFile<br/>command| File

    style Proxy fill:#e1f5ff
    style Recorder fill:#fff4e1
    style File fill:#e8f5e9
```


In recording mode, proxies:
1. Accept downstream connections
2. Forward requests/responses to upstream servers
3. Send `RecordedEvent` messages to the `Recorder` actor via channels
4. Record both downstream and upstream traffic

Example (HTTP):
```rust
// When downstream sends a request
self.recorder.send(RecordedEvent::new(self.proxy_id, request.clone()))?;
let response = self.send_request(request).await?; // Forward to upstream
self.recorder.send(RecordedEvent::new(self.proxy_id, response.clone()))?;
```

#### Replay Mode

```mermaid
flowchart RL
    Client[Downstream<br/>Client]
    Proxy[Proxy Actor]
    Replayer[Replayer Actor]
    File[(Recording<br/>File)]

    Replayer -->|Load events| File
    Client -->|Request| Proxy
    Proxy -.->|Release replay lock| Replayer
    Replayer -->|RecordedEventWithLock| Proxy
    Proxy -->|Response| Client

    style Proxy fill:#e1f5ff
    style Replayer fill:#fff4e1
    style File fill:#e8f5e9
```

In replay mode, proxies:
1. Accept downstream connections
2. Receive `RecordedEventWithLock` messages from the `Replayer` actor
3. Hold replay locks until downstream requests match recorded requests
4. Send recorded responses back to downstream clients

Example (HTTP):
- When a downstream request arrives, the proxy matches it with a recorded request
- The proxy holds the replay lock until the match is found
- Once matched, the proxy sends the recorded response back to the downstream client
- The replay lock ensures responses are sent in the correct order relative to other events

This pattern allows protocols like HTTP (request/response) to work correctly in replay mode, where responses need to be sent back to waiting downstream clients.


## License

[Add your license information here]
