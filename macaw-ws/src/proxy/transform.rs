use crate::lib::*;

/// Redact WebSocket events before they are recorded or compared to recorded events.
/// Use case: redact nondeterministic parts, remove sensitive data, etc...
#[dyn_clonable::clonable]
pub trait WsRedact: Clone + Send + Sync {
    fn ws_redact_event(&self, event: WsEvent) -> WsEvent {
        event
    }
}

impl fmt::Debug for Box<dyn WsRedact> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "WsRedact")?;
        Ok(())
    }
}

#[derive(Clone)]
pub struct NoopWsRedact;

impl WsRedact for NoopWsRedact {}

impl Default for Box<dyn WsRedact> {
    fn default() -> Self {
        Box::new(NoopWsRedact)
    }
}

/// Transform WebSocket events when they enter or leave Macaw in destination of a remote client/server.
/// Use case: encrypt/decrypt messages, custom compression/decompression of the message, etc...
#[dyn_clonable::clonable]
pub trait WsTransform: Send + Sync + Clone {
    fn encode_event(&self, event: WsEvent) -> Result<WsEvent, anyhow::Error> {
        Ok(event)
    }

    fn decode_event(&self, event: WsEvent) -> Result<WsEvent, anyhow::Error> {
        Ok(event)
    }
}

impl fmt::Debug for Box<dyn WsTransform> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "WsTransform")?;
        Ok(())
    }
}

#[derive(Clone)]
pub struct NoopWsTransform;

impl WsTransform for NoopWsTransform {}

impl Default for Box<dyn WsTransform> {
    fn default() -> Self {
        Box::new(NoopWsTransform)
    }
}
