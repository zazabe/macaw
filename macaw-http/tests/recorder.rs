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

    // Set up mock endpoints
    let mock = server.mock(|when, then| {
        when.method(httpmock::Method::POST)
            .path("/test")
            .header("x-signature", "sd20#Rfkm320QQ")
            .header("x-timestamp", "1765613418")
            .body("TX(request1)");
        then.status(200).body("RX(response1)");
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

    // Make HTTP request through the proxy using reqwest
    let client = reqwest::Client::new();
    let proxy_url = format!("http://{}/test", proxy_addr);
    let res = client
        .post(&proxy_url)
        .header("x-signature", "sd20#Rfkm320QQ")
        .header("x-timestamp", "1765613418")
        .body("TX(request1)")
        .send()
        .await
        .unwrap();

    assert_eq!(res.status(), 200);
    assert_eq!(res.text().await.unwrap(), "RX(response1)");
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
              "content-length": "12",
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
              "content-length": "13",
              "date": "[date]"
            },
            "body": "response1"
          }
        }
      ]
    }
    "#);
}
