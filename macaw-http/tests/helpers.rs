use flate2::Compression;
use flate2::read::{GzDecoder, GzEncoder};
use macaw_core::prelude::*;
use macaw_http::prelude::*;
use std::io::Read;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

#[derive(Clone)]
pub struct TestHttpRedact;

impl HttpRedact for TestHttpRedact {
    fn http_redact_request(
        &self,
        mut request: HttpRequestEvent,
    ) -> Result<HttpRequestEvent, anyhow::Error> {
        request
            .headers
            .insert("x-signature".to_string(), "REDACTED".to_string());
        request
            .headers
            .insert("x-timestamp".to_string(), "TIMESTAMP".to_string());
        Ok(request)
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

/// Spawns a TCP server that accepts connections and immediately drops them without sending any data.
/// Returns the server address and a join handle to keep the server running.
#[allow(dead_code)]
pub async fn spawn_tcp_disconnect_server() -> (SocketAddr, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = tokio::spawn(async move {
        loop {
            if let Ok((stream, _)) = listener.accept().await {
                drop(stream);
            }
        }
    });
    (addr, handle)
}

/// Spawns a TCP server that sends HTTP response headers then closes the connection before sending the body.
/// Returns the server address and a join handle to keep the server running.
#[allow(dead_code)]
pub async fn spawn_tcp_partial_response_server() -> (SocketAddr, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = tokio::spawn(async move {
        loop {
            if let Ok((mut stream, _)) = listener.accept().await {
                let mut buf = [0u8; 4096];
                let _ = tokio::io::AsyncReadExt::read(&mut stream, &mut buf).await;
                let headers = b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\n";
                let _ = tokio::io::AsyncWriteExt::write_all(&mut stream, headers).await;
                drop(stream);
            }
        }
    });
    (addr, handle)
}

/// Spawns a TLS server with a self-signed certificate.
/// The platform verifier will reject it when connecting via https://127.0.0.1.
/// Returns the server address and a join handle to keep the server running.
#[cfg(test)]
#[allow(dead_code)]
pub async fn spawn_tls_server_with_self_signed_cert() -> (SocketAddr, JoinHandle<()>) {
    use rcgen::generate_simple_self_signed;
    use rustls::pki_types::PrivateKeyDer;
    use tokio_rustls::TlsAcceptor;

    let subject_alt_names = vec!["localhost".to_string(), "127.0.0.1".to_string()];
    let certified_key = generate_simple_self_signed(subject_alt_names).unwrap();

    let cert_chain = vec![certified_key.cert.der().clone()];
    let key_der = PrivateKeyDer::Pkcs8(certified_key.signing_key.serialize_der().into());

    let config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(cert_chain, key_der)
        .unwrap();
    let acceptor = TlsAcceptor::from(Arc::new(config));

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = tokio::spawn(async move {
        loop {
            if let Ok((stream, _)) = listener.accept().await {
                let acceptor = acceptor.clone();
                tokio::spawn(async move {
                    if let Ok(mut tls_stream) = acceptor.accept(stream).await {
                        let mut buf = [0u8; 4096];
                        let _ = tokio::io::AsyncReadExt::read(&mut tls_stream, &mut buf).await;
                        let response = b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok";
                        let _ =
                            tokio::io::AsyncWriteExt::write_all(&mut tls_stream, response).await;
                    }
                });
            }
        }
    });
    (addr, handle)
}
