use macaw_core::prelude::*;
use macaw_http::prelude::*;
use tokio::time::{Duration, sleep};

#[tokio::test]
async fn test_replayer_multiple_http_proxies() {
    let recording_path =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/recordings.yaml");
    let mut macaw = Macaw::<Replayer>::replayer(recording_path).unwrap();

    let proxy1_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy1_addr = proxy1_listener.local_addr().unwrap();
    drop(proxy1_listener);
    let proxy2_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy2_addr = proxy2_listener.local_addr().unwrap();
    drop(proxy2_listener);

    macaw
        .add_http_proxy("http_proxy1", &proxy1_addr.to_string())
        .await
        .unwrap();
    macaw
        .add_http_proxy("http_proxy2", &proxy2_addr.to_string())
        .await
        .unwrap();

    macaw.play().unwrap();

    tokio::task::yield_now().await;

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
