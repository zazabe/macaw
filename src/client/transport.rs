use super::model::ErrorResponse;
use anyhow::{Context, Result, bail};
use reqwest::{Method, Response};
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::path::Path;
use std::time::Duration;

#[derive(Clone)]
pub struct ControlClient {
    client: reqwest::Client,
    stream_client: reqwest::Client,
    base_url: String,
}

impl ControlClient {
    pub fn new(url: &str, unix: Option<&Path>, timeout: Duration) -> Result<Self> {
        let mut builder = reqwest::Client::builder().timeout(timeout);
        let mut stream_builder = reqwest::Client::builder();
        #[cfg(unix)]
        if let Some(path) = unix {
            builder = builder.unix_socket(path);
            stream_builder = stream_builder.unix_socket(path);
        }
        #[cfg(not(unix))]
        if unix.is_some() {
            bail!("Unix control sockets are not supported on this platform");
        }
        let base_url = if unix.is_some() {
            "http://localhost".to_owned()
        } else {
            url.trim_end_matches('/').to_owned()
        };
        Ok(Self {
            client: builder.build().context("failed to create HTTP client")?,
            stream_client: stream_builder
                .build()
                .context("failed to create streaming HTTP client")?,
            base_url,
        })
    }

    pub async fn get<T: DeserializeOwned>(&self, path: &str) -> Result<T> {
        self.send_json::<(), T>(Method::GET, path, None).await
    }

    pub async fn post<B: Serialize, T: DeserializeOwned>(
        &self,
        path: &str,
        body: Option<&B>,
    ) -> Result<T> {
        self.send_json(Method::POST, path, body).await
    }

    pub async fn delete(&self, path: &str) -> Result<()> {
        let response = self.request(Method::DELETE, path).send().await?;
        checked(response).await?;
        Ok(())
    }

    pub async fn stream(&self, path: &str) -> Result<Response> {
        checked(
            self.stream_client
                .get(format!("{}{}", self.base_url, path))
                .send()
                .await?,
        )
        .await
    }

    async fn send_json<B: Serialize, T: DeserializeOwned>(
        &self,
        method: Method,
        path: &str,
        body: Option<&B>,
    ) -> Result<T> {
        let mut request = self.request(method, path);
        if let Some(body) = body {
            request = request.json(body);
        }
        checked(request.send().await?)
            .await?
            .json()
            .await
            .context("control server returned invalid JSON")
    }

    fn request(&self, method: Method, path: &str) -> reqwest::RequestBuilder {
        self.client
            .request(method, format!("{}{}", self.base_url, path))
    }
}

async fn checked(response: Response) -> Result<Response> {
    if response.status().is_success() {
        return Ok(response);
    }
    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    if let Ok(error) = serde_json::from_str::<ErrorResponse>(&body) {
        bail!(
            "control server returned {status}: {:?}: {}",
            error.error.code,
            error.error.message
        );
    }
    bail!("control server returned {status}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{Json, Router, routing::get};
    use serde_json::{Value, json};

    fn app() -> Router {
        Router::new().route("/v1/health", get(|| async { Json(json!({"ready": true})) }))
    }

    #[tokio::test]
    async fn connects_over_tcp() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app()).await });
        let client =
            ControlClient::new(&format!("http://{address}"), None, Duration::from_secs(2)).unwrap();

        let response = client.get::<Value>("/v1/health").await.unwrap();
        assert_eq!(response["ready"], true);
        server.abort();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn connects_over_a_unix_socket() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("control.sock");
        let listener = tokio::net::UnixListener::bind(&path).unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app()).await });
        let client = ControlClient::new("", Some(&path), Duration::from_secs(2)).unwrap();

        let response = client.get::<Value>("/v1/health").await.unwrap();
        assert_eq!(response["ready"], true);
        server.abort();
    }
}
