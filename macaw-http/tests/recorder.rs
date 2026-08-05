use bytes::Bytes;
use http::Request;
use insta::assert_json_snapshot;
use macaw_core::prelude::*;
use macaw_http::prelude::*;

mod helpers;
use helpers::*;

#[tokio::test]
async fn test_recorder_multiple_http_proxies() {
    let temp_file = tempfile::NamedTempFile::new().expect("Failed to create temp file");
    let test_file = temp_file.path().to_path_buf();

    let server = httpmock::MockServer::start_async().await;

    // Set up mock endpoints
    let _mock1 = server.mock(|when, then| {
        when.method(httpmock::Method::GET).path("/test1");
        then.status(200).body("response1");
    });

    let _mock2 = server.mock(|when, then| {
        when.method(httpmock::Method::GET).path("/test2");
        then.status(200).body("response2");
    });

    let server_url = server.base_url();
    println!("server_url: {}", server_url);
    // Create recorder
    let mut macaw = Macaw::<Recorder>::recorder();

    // Add first HTTP proxy
    let proxy1_addr = macaw
        .add_http_proxy(
            "http_proxy1",
            "127.0.0.1:0",
            &server_url,
            HttpProxyOptions::default(),
        )
        .await
        .unwrap();

    // Add second HTTP proxy
    let proxy2_addr = macaw
        .add_http_proxy(
            "http_proxy2",
            "127.0.0.1:0",
            &server_url,
            HttpProxyOptions::default(),
        )
        .await
        .unwrap();

    // Make HTTP requests through the proxies using reqwest
    let client = reqwest::Client::new();

    // Make request through proxy 1
    let proxy1_url = format!("http://{}/test1", proxy1_addr);
    let _res1 = client.get(&proxy1_url).send().await.unwrap();

    // Make request through proxy 2
    let proxy2_url = format!("http://{}/test2", proxy2_addr);
    let _res2 = client.get(&proxy2_url).send().await.unwrap();

    // Exit and save
    macaw.exit_handle().exit();
    macaw.record_when_exit(&test_file).await.unwrap();

    let file_content: RecordFile =
        serde_json::from_str(std::fs::read_to_string(test_file).unwrap().as_str()).unwrap();
    insta::assert_json_snapshot!(file_content, {
        r#".header.record_id"# => "[record_id]",
        r#".header.record_seed"# => "[record_seed]",
        r#".**.timestamp"# => "[timestamp]",
        r#".**.request_id"# => "[request_id]",
        r#".**.headers.host"# => "[host]",
        r#".**.headers.date"# => "[date]",
    }, @r#"
    {
      "header": {
        "record_id": "[record_id]",
        "record_seed": "[record_seed]",
        "timestamp": "[timestamp]"
      },
      "events": [
        {
          "proxy": "http_proxy1",
          "timestamp": "[timestamp]",
          "HttpRequest": {
            "request_id": "[request_id]",
            "method": "GET",
            "uri": "/test1",
            "version": "HTTP/1.1",
            "headers": {
              "accept": "*/*",
              "host": "[host]"
            },
            "body": null
          }
        },
        {
          "proxy": "http_proxy1",
          "timestamp": "[timestamp]",
          "HttpResponse": {
            "request_id": "[request_id]",
            "status": 200,
            "version": "HTTP/1.1",
            "headers": {
              "content-length": "9",
              "date": "[date]"
            },
            "body": "response1"
          }
        },
        {
          "proxy": "http_proxy2",
          "timestamp": "[timestamp]",
          "HttpRequest": {
            "request_id": "[request_id]",
            "method": "GET",
            "uri": "/test2",
            "version": "HTTP/1.1",
            "headers": {
              "accept": "*/*",
              "host": "[host]"
            },
            "body": null
          }
        },
        {
          "proxy": "http_proxy2",
          "timestamp": "[timestamp]",
          "HttpResponse": {
            "request_id": "[request_id]",
            "status": 200,
            "version": "HTTP/1.1",
            "headers": {
              "content-length": "9",
              "date": "[date]"
            },
            "body": "response2"
          }
        }
      ]
    }
    "#);
}

#[tokio::test]
async fn test_recorder_http_transform() {
    let temp_file = tempfile::NamedTempFile::new().expect("Failed to create temp file");
    let test_file = temp_file.path().to_path_buf();

    let server = httpmock::MockServer::start_async().await;

    // Set up mock endpoints - use custom matcher to decompress gzip body and verify
    let mock = server.mock(|when, then| {
        when.method(httpmock::Method::POST)
            .path("/test")
            .header("x-signature", "sd20#Rfkm320QQ")
            .header("x-timestamp", "1765613418")
            .is_true(|req: &httpmock::HttpMockRequest| {
                let http_req = Request::<Bytes>::from(req);
                gzip_decompress(http_req.body())
                    .map(|b| b == b"request1")
                    .unwrap_or(false)
            });
        then.status(200).body(gzip_compress(b"response1"));
    });

    let server_url = server.base_url();

    // Create recorder
    let mut macaw = Macaw::<Recorder>::recorder();

    let options = HttpProxyOptions {
        redact: Box::new(TestHttpRedact),
        transform: Box::new(TestHttpTransform),
        overrides: Default::default(),
    };

    // Add HTTP proxy
    let proxy_addr = macaw
        .add_http_proxy("http_proxy", "127.0.0.1:0", &server_url, options)
        .await
        .unwrap();

    // Make HTTP request through the proxy using reqwest (send gzip-compressed body)
    let client = reqwest::Client::new();
    let proxy_url = format!("http://{}/test", proxy_addr);
    let res = client
        .post(&proxy_url)
        .header("x-signature", "sd20#Rfkm320QQ")
        .header("x-timestamp", "1765613418")
        .body(gzip_compress(b"request1"))
        .send()
        .await
        .unwrap();

    assert_eq!(res.status(), 200);
    let body = res.bytes().await.unwrap();
    assert_eq!(
        gzip_decompress(&body).unwrap(),
        b"response1",
        "response body should decompress to response1"
    );
    mock.assert_calls(1);

    // Exit and save
    macaw.exit_handle().exit();
    macaw.record_when_exit(&test_file).await.unwrap();

    let file_content: RecordFile =
        serde_json::from_str(std::fs::read_to_string(test_file).unwrap().as_str()).unwrap();
    insta::assert_json_snapshot!(file_content, {
      r#".header.record_id"# => "[record_id]",
      r#".header.record_seed"# => "[record_seed]",
      r#".**.timestamp"# => "[timestamp]",
      r#".**.request_id"# => "[request_id]",
      r#".**.uri"# => "[uri]",
      r#".**.headers.host"# => "[host]",
      r#".**.headers.date"# => "[date]",
  }, @r#"
    {
      "header": {
        "record_id": "[record_id]",
        "record_seed": "[record_seed]",
        "timestamp": "[timestamp]"
      },
      "events": [
        {
          "proxy": "http_proxy",
          "timestamp": "[timestamp]",
          "HttpRequest": {
            "request_id": "[request_id]",
            "method": "POST",
            "uri": "[uri]",
            "version": "HTTP/1.1",
            "headers": {
              "accept": "*/*",
              "content-length": "28",
              "host": "[host]",
              "x-signature": "REDACTED",
              "x-timestamp": "TIMESTAMP"
            },
            "body": "request1"
          }
        },
        {
          "proxy": "http_proxy",
          "timestamp": "[timestamp]",
          "HttpResponse": {
            "request_id": "[request_id]",
            "status": 200,
            "version": "HTTP/1.1",
            "headers": {
              "content-length": "29",
              "date": "[date]"
            },
            "body": "response1"
          }
        }
      ]
    }
    "#);
}

#[tokio::test]
async fn test_recorder_upstream_disconnect_before_response() {
    let temp_file = tempfile::NamedTempFile::new().expect("Failed to create temp file");
    let test_file = temp_file.path().to_path_buf();

    let (server_addr, _handle) = spawn_tcp_disconnect_server().await;
    let server_url = format!("http://{}/", server_addr);

    let mut macaw = Macaw::<Recorder>::recorder();
    let proxy_addr = macaw
        .add_http_proxy(
            "http_proxy",
            "127.0.0.1:0",
            &server_url,
            HttpProxyOptions::default(),
        )
        .await
        .unwrap();

    let client = reqwest::Client::new();
    let proxy_url = format!("http://{}/", proxy_addr);
    let response = client.get(&proxy_url).send().await.unwrap();

    assert_eq!(response.status(), 500);
    let body = response.bytes().await.unwrap();
    let json = serde_json::from_slice::<serde_json::Value>(&body).unwrap();
    assert_json_snapshot!(json, {
        r#".request_id"# => "[request_id]",
    }, @r#"
    {
      "error": "Transport error: client error (SendRequest): connection closed before message completed",
      "request_id": "[request_id]"
    }
    "#);

    macaw.exit_handle().exit();
    macaw.record_when_exit(&test_file).await.unwrap();
}

#[tokio::test]
async fn test_recorder_upstream_partial_response() {
    let temp_file = tempfile::NamedTempFile::new().expect("Failed to create temp file");
    let test_file = temp_file.path().to_path_buf();

    let (server_addr, _handle) = spawn_tcp_partial_response_server().await;
    let server_url = format!("http://{}/", server_addr);

    let mut macaw = Macaw::<Recorder>::recorder();
    let proxy_addr = macaw
        .add_http_proxy(
            "http_proxy",
            "127.0.0.1:0",
            &server_url,
            HttpProxyOptions::default(),
        )
        .await
        .unwrap();

    let client = reqwest::Client::new();
    let proxy_url = format!("http://{}/test", proxy_addr);
    let response = client.get(&proxy_url).send().await.unwrap();

    assert_eq!(response.status(), 500);
    let body = response.bytes().await.unwrap();
    let json = serde_json::from_slice::<serde_json::Value>(&body).unwrap();
    assert_json_snapshot!(json, {
        r#".request_id"# => "[request_id]",
    }, @r#"
    {
      "error": "Transport error: error reading a body from connection: end of file before message length reached",
      "request_id": "[request_id]"
    }
    "#);

    macaw.exit_handle().exit();
    macaw.record_when_exit(&test_file).await.unwrap();
}

#[tokio::test]
async fn test_recorder_client_disconnect_before_complete() {
    let temp_file = tempfile::NamedTempFile::new().expect("Failed to create temp file");
    let test_file = temp_file.path().to_path_buf();

    let server = httpmock::MockServer::start_async().await;
    let _mock = server.mock(|when, then| {
        when.method(httpmock::Method::GET).path("/test");
        then.status(200).body("ok");
    });
    let server_url = server.base_url();

    let mut macaw = Macaw::<Recorder>::recorder();
    let proxy_addr = macaw
        .add_http_proxy(
            "http_proxy",
            "127.0.0.1:0",
            &server_url,
            HttpProxyOptions::default(),
        )
        .await
        .unwrap();

    let stream = tokio::net::TcpStream::connect(proxy_addr).await.unwrap();
    drop(stream);

    macaw.exit_handle().exit();
    macaw.record_when_exit(&test_file).await.unwrap();

    let json: serde_json::Value =
        serde_json::from_str(std::fs::read_to_string(test_file).unwrap().as_str()).unwrap();
    assert_json_snapshot!(json, {
        r#".header.record_id"# => "[record_id]",
        r#".header.record_seed"# => "[record_seed]",
        r#".header.timestamp"# => "[timestamp]",
    }, @r#"
    {
      "events": [],
      "header": {
        "record_id": "[record_id]",
        "record_seed": "[record_seed]",
        "timestamp": "[timestamp]"
      }
    }
    "#);
}

#[tokio::test]
async fn test_recorder_upstream_not_reachable() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);

    let unreachable_url = format!("http://127.0.0.1:{}/", port);
    let mut macaw = Macaw::<Recorder>::recorder();
    let result = macaw
        .add_http_proxy(
            "http_proxy",
            "127.0.0.1:0",
            &unreachable_url,
            HttpProxyOptions::default(),
        )
        .await;

    let err = result.unwrap_err();
    assert!(
        err.to_string().contains("tcp connect error"),
        "result should contain 'tcp connect error', got: {}",
        err
    );
}

#[tokio::test]
async fn test_recorder_upstream_schema_not_supported_error() {
    let (addr, _handle) = spawn_tls_server_with_self_signed_cert().await;
    let server_url = format!("ws://127.0.0.1:{}/", addr.port());

    let mut macaw = Macaw::<Recorder>::recorder();
    let result = macaw
        .add_http_proxy(
            "http_proxy",
            "127.0.0.1:0",
            &server_url,
            HttpProxyOptions::default(),
        )
        .await;

    let err = result.unwrap_err();
    assert!(
        err.to_string().contains("unsupported scheme ws"),
        "result should contain 'unsupported scheme ws', got: {}",
        err
    );
}

#[tokio::test]
async fn test_recorder_upstream_tls_error() {
    let (addr, _handle) = spawn_tls_server_with_self_signed_cert().await;
    let server_url = format!("https://127.0.0.1:{}/", addr.port());

    let mut macaw = Macaw::<Recorder>::recorder();
    let result = macaw
        .add_http_proxy(
            "http_proxy",
            "127.0.0.1:0",
            &server_url,
            HttpProxyOptions::default(),
        )
        .await;

    let err = result.unwrap_err();
    assert!(
        err.to_string()
            .contains("invalid peer certificate: UnknownIssuer"),
        "result should contain 'invalid peer certificate: UnknownIssuer', got: {}",
        err
    );
}
