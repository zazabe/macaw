use std::time::Duration;

use macaw_core::prelude::*;
use macaw_core::test_path;
use macaw_http::prelude::*;

mod helpers;
use helpers::*;

#[tokio::test]
async fn test_replayer_multiple_http_proxies() {
    let recording_path =
        test_path!().join("./data/replayer-test_replayer_multiple_http_proxies.json");
    let mut macaw = Macaw::<Replayer>::replayer(recording_path).unwrap();

    let proxy1_addr = macaw
        .add_http_proxy("http_proxy1", "127.0.0.1:0", HttpProxyOptions::default())
        .await
        .unwrap();
    let proxy2_addr = macaw
        .add_http_proxy("http_proxy2", "127.0.0.1:0", HttpProxyOptions::default())
        .await
        .unwrap();

    macaw.play().unwrap();

    let client = reqwest::Client::new();

    // Make request through proxy 1 - this should match a recorded request
    let proxy1_url = format!("http://{}/test1", proxy1_addr);
    let res1 = client.get(&proxy1_url).send().await.unwrap();
    assert_eq!(res1.status(), 200);
    let body_str = res1.text().await.unwrap();
    assert_eq!(body_str, "response1");

    // Make request through proxy 2 - this should match a recorded request
    let proxy2_url = format!("http://{}/test2", proxy2_addr);
    let res2 = client.get(&proxy2_url).send().await.unwrap();
    assert_eq!(res2.status(), 200);
    let body_str = res2.text().await.unwrap();
    assert_eq!(body_str, "response2");
}

#[tokio::test]
async fn test_replayer_http_transform() {
    let recording_path = test_path!().join("./data/replayer-test_replayer_http_transform.json");
    let mut macaw = Macaw::<Replayer>::replayer(recording_path).unwrap();

    let options = HttpProxyOptions {
        redact: Box::new(TestHttpRedact),
        transform: Box::new(TestHttpTransform),
        overrides: Default::default(),
    };
    let proxy_addr = macaw
        .add_http_proxy("http_proxy", "127.0.0.1:0", options)
        .await
        .unwrap();

    macaw.play().unwrap();

    let client = reqwest::Client::new();

    // Make request through proxy - send gzip-compressed body, receive gzip-compressed response
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
}

#[tokio::test]
async fn test_replayer_disconnect_before_response() {
    let recording_path =
        test_path!().join("./data/replayer-test_replayer_disconnect_before_response.json");
    let mut macaw = Macaw::<Replayer>::replayer(recording_path).unwrap();

    let proxy_addr = macaw
        .add_http_proxy("http_proxy1", "127.0.0.1:0", HttpProxyOptions::default())
        .await
        .unwrap();

    macaw.play().unwrap();

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .unwrap();

    let proxy_url = format!("http://{}/test1", proxy_addr);
    let res = client.get(&proxy_url).send().await;

    assert!(res.is_err() || res.unwrap().status().is_server_error());
}

#[tokio::test]
async fn test_replayer_no_matching_request() {
    let recording_path =
        test_path!().join("./data/replayer-test_replayer_multiple_http_proxies.json");
    let mut macaw = Macaw::<Replayer>::replayer(recording_path).unwrap();

    let proxy_addr = macaw
        .add_http_proxy("http_proxy1", "127.0.0.1:0", HttpProxyOptions::default())
        .await
        .unwrap();

    macaw.play().unwrap();

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .unwrap();

    let proxy_url = format!("http://{}/other", proxy_addr);
    let res = client.get(&proxy_url).send().await;

    assert!(res.is_err() || res.unwrap().status().is_server_error());
}
