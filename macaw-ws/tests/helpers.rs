use macaw_ws::prelude::*;

#[derive(Clone)]
pub struct TestWsRedact;

impl WsRedact for TestWsRedact {
    fn ws_redact_event(&self, event: WsEvent) -> WsEvent {
        match event {
            WsEvent::Message(message_event) => {
                let redacted_message = match message_event.message {
                    WsMessage::Text(ref text) => {
                        // Redact any text that matches a pattern - for testing, we'll just replace "secret" with "REDACTED"
                        WsMessage::Text(text.replace("secret", "REDACTED"))
                    }
                    other => other,
                };
                WsEvent::message(redacted_message)
            }
            other => other,
        }
    }
}

pub(crate) fn encode_text(text: &str) -> String {
    base64::Engine::encode(&base64::engine::general_purpose::STANDARD, text.as_bytes())
}

pub(crate) fn decode_text(text: &str) -> String {
    String::from_utf8(
        base64::Engine::decode(&base64::engine::general_purpose::STANDARD, text.as_bytes())
            .unwrap(),
    )
    .unwrap()
}

#[derive(Clone)]
pub struct TestWsTransform;

impl WsTransform for TestWsTransform {
    fn encode_event(&self, event: WsEvent) -> Result<WsEvent, anyhow::Error> {
        match event {
            WsEvent::Message(message_event) => {
                let encoded_message = match message_event.message {
                    WsMessage::Text(text) => {
                        let encoded_text = encode_text(&text);
                        WsMessage::Text(encoded_text)
                    }
                    other => other,
                };
                Ok(WsEvent::message(encoded_message))
            }
            other => Ok(other),
        }
    }

    fn decode_event(&self, event: WsEvent) -> Result<WsEvent, anyhow::Error> {
        match event {
            WsEvent::Message(message_event) => {
                let decoded_message = match message_event.message {
                    WsMessage::Text(text) => {
                        let decoded_text = decode_text(&text);
                        WsMessage::Text(decoded_text)
                    }
                    other => other,
                };
                Ok(WsEvent::message(decoded_message))
            }
            other => Ok(other),
        }
    }
}
