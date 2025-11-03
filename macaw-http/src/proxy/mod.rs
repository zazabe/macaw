pub mod record;
pub mod replay;

pub use record::*;
pub use replay::*;

use crate::lib::*;
use macaw_core::prelude::*;

impl HttpServerRequestSender for Sender<HttpRequestEvent, HttpResponseEvent, UnexpectedEvent> {
    fn send(&self, envelope: HttpRequestEnvelope) -> Result<(), anyhow::Error> {
        debug!("Sending request: {:?}", envelope.request);
        let HttpRequestEnvelope {
            request,
            response_tx,
        } = envelope;
        let request_event = HttpRequestEvent::from_request(&request)?;
        self.tx
            .send(Message::Downstream(DownstreamMessage {
                proxy_id: self.id,
                event: request_event,
                response_tx: Some(response_tx),
            }))
            .map_err(|_| anyhow::anyhow!("Failed to send downstream message"))?;
        Ok(())
    }
}
