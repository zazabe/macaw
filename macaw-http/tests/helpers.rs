use flate2::Compression;
use flate2::read::{GzDecoder, GzEncoder};
use macaw_core::prelude::*;
use macaw_http::prelude::*;
use std::io::Read;

#[derive(Clone)]
pub struct TestHttpRedact;

impl HttpRedact for TestHttpRedact {
    fn http_redact_request(&self, mut request: HttpRequestEvent) -> HttpRequestEvent {
        request
            .headers
            .insert("x-signature".to_string(), "REDACTED".to_string());
        request
            .headers
            .insert("x-timestamp".to_string(), "TIMESTAMP".to_string());
        request
    }
}

pub fn gzip_compress(data: &[u8]) -> Vec<u8> {
    let mut encoder = GzEncoder::new(data, Compression::fast());
    let mut compressed = Vec::new();
    encoder.read_to_end(&mut compressed).unwrap();
    compressed
}

pub fn gzip_decompress(data: &[u8]) -> Result<Vec<u8>, std::io::Error> {
    let mut decoder = GzDecoder::new(data);
    let mut decompressed = Vec::new();
    decoder.read_to_end(&mut decompressed)?;
    Ok(decompressed)
}

#[derive(Clone)]
pub struct TestHttpTransform;

impl HttpTransform for TestHttpTransform {
    fn encode_request(
        &self,
        mut request: HttpRequestEvent,
    ) -> Result<HttpRequestEvent, anyhow::Error> {
        let body = request.body.to_bytes();
        if body.is_empty() {
            return Ok(request);
        }
        let compressed = gzip_compress(&body);
        request
            .headers
            .insert("content-length".to_string(), compressed.len().to_string());
        request.body = Content::from_bytes(&compressed);
        Ok(request)
    }

    fn decode_request(
        &self,
        mut request: HttpRequestEvent,
    ) -> Result<HttpRequestEvent, anyhow::Error> {
        let body = request.body.to_bytes();
        if body.is_empty() {
            return Ok(request);
        }
        let decompressed_bytes = gzip_decompress(&body)?;
        let decompressed = String::from_utf8_lossy(&decompressed_bytes).to_string();
        request.body = Content::Text(PlainText::new(decompressed));
        Ok(request)
    }

    fn encode_response(
        &self,
        mut response: HttpResponseEvent,
    ) -> Result<HttpResponseEvent, anyhow::Error> {
        let body = response.body.to_bytes();
        if body.is_empty() {
            return Ok(response);
        }
        let compressed = gzip_compress(&body);
        response
            .headers
            .insert("content-length".to_string(), compressed.len().to_string());
        response.body = Content::from_bytes(&compressed);
        Ok(response)
    }

    fn decode_response(
        &self,
        mut response: HttpResponseEvent,
    ) -> Result<HttpResponseEvent, anyhow::Error> {
        let body = response.body.to_bytes();
        if body.is_empty() {
            return Ok(response);
        }
        let decompressed_bytes = gzip_decompress(&body)?;
        let decompressed = String::from_utf8_lossy(&decompressed_bytes).to_string();
        response.body = Content::Text(PlainText::new(decompressed.to_string()));
        Ok(response)
    }
}
