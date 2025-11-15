use macaw_core::prelude::*;
use macaw_http::prelude::*;
use std::net::SocketAddr;
use tokio::time::{Duration, sleep};

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

    // Create recorder
    let mut macaw = Macaw::<Recorder>::recorder();

    // Add first HTTP proxy
    let proxy1_addr = macaw
        .add_http_proxy("http_proxy1", "127.0.0.1:0", &server_url)
        .await
        .unwrap();

    // Add second HTTP proxy
    let proxy2_addr = macaw
        .add_http_proxy("http_proxy2", "127.0.0.1:0", &server_url)
        .await
        .unwrap();

    // Give proxies time to start
    tokio::task::yield_now().await;

    // Make HTTP requests through the proxies using reqwest
    let client = reqwest::Client::new();

    // Make request through proxy 1
    let proxy1_url = format!("http://{}/test1", proxy1_addr);
    let _res1 = client.get(&proxy1_url).send().await.unwrap();

    // Make request through proxy 2
    let proxy2_url = format!("http://{}/test2", proxy2_addr);
    let _res2 = client.get(&proxy2_url).send().await.unwrap();

    // Give time for events to be recorded
    tokio::task::yield_now().await;

    // Exit and save
    macaw.exit_handle().exit();
    macaw.record_when_exit(&test_file).await.unwrap();

    let file_content: serde_yaml::Value =
        serde_yaml::from_str(std::fs::read_to_string(test_file).unwrap().as_str()).unwrap();
    insta::assert_yaml_snapshot!(file_content, {
        r#"["header"]["record_id"]"# => "[record_id]",
        r#"["header"]["record_seed"]"# => "[record_seed]",
        r#".**["timestamp"]"# => "[timestamp]",
        r#".**["request_id"]"# => "[request_id]",
        r#".**["uri"]"# => "[uri]",
        r#".**["host"]"# => "[host]",
        r#".**["headers"]["date"]"# => "[date]",
    }, @r#"
    header:
      record_id: "[record_id]"
      record_seed: "[record_seed]"
      timestamp: "[timestamp]"
    events:
      - proxy: http_proxy1
        timestamp: "[timestamp]"
        HttpRequest:
          request_id: "[request_id]"
          method: GET
          uri: "[uri]"
          version: HTTP/1.1
          headers:
            accept: "*/*"
            host: "[host]"
          body:
            Empty: ~
      - proxy: http_proxy1
        timestamp: "[timestamp]"
        HttpResponse:
          request_id: "[request_id]"
          status: 200
          version: HTTP/1.1
          headers:
            content-length: "9"
            date: "[date]"
          body:
            PlainText: response1
      - proxy: http_proxy2
        timestamp: "[timestamp]"
        HttpRequest:
          request_id: "[request_id]"
          method: GET
          uri: "[uri]"
          version: HTTP/1.1
          headers:
            accept: "*/*"
            host: "[host]"
          body:
            Empty: ~
      - proxy: http_proxy2
        timestamp: "[timestamp]"
        HttpResponse:
          request_id: "[request_id]"
          status: 200
          version: HTTP/1.1
          headers:
            content-length: "9"
            date: "[date]"
          body:
            PlainText: response2
    "#);
}
