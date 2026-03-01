use flate2::Compression;
use flate2::read::{GzDecoder, GzEncoder};
use macaw_ws::prelude::*;
use std::io::Read;

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

pub(crate) fn gzip_compress(data: &[u8]) -> Vec<u8> {
    let mut encoder = GzEncoder::new(data, Compression::fast());
    let mut compressed = Vec::new();
    encoder.read_to_end(&mut compressed).unwrap();
    compressed
}

pub(crate) fn gzip_decompress(data: &[u8]) -> Result<Vec<u8>, std::io::Error> {
    let mut decoder = GzDecoder::new(data);
    let mut decompressed = Vec::new();
    decoder.read_to_end(&mut decompressed)?;
    Ok(decompressed)
}

/// Asserts that decompressed `data` equals `expected` (as UTF-8 string).
#[allow(dead_code)]
pub(crate) fn assert_decompressed_eq(data: &[u8], expected: &str) {
    let decompressed = gzip_decompress(data).expect("gzip decompress failed");
    let text = String::from_utf8(decompressed).expect("invalid UTF-8");
    assert_eq!(text, expected);
}

#[derive(Clone)]
pub struct TestWsTransform;

impl WsTransform for TestWsTransform {
    fn encode_event(&self, event: WsEvent) -> Result<WsEvent, anyhow::Error> {
        match event {
            WsEvent::Message(message_event) => {
                let encoded_message = match message_event.message {
                    WsMessage::Text(text) => {
                        let compressed = gzip_compress(text.as_bytes());
                        WsMessage::Binary(compressed.into())
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
                    WsMessage::Binary(data) => {
                        let decompressed = gzip_decompress(&data)?;
                        let text = String::from_utf8(decompressed)?;
                        WsMessage::Text(text)
                    }
                    other => other,
                };
                Ok(WsEvent::message(decoded_message))
            }
            other => Ok(other),
        }
    }
}
