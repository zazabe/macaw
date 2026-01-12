#[allow(unused)]
mod common;

use macaw_core::prelude::*;
use macaw_core::test_path;
use macaw_ws::prelude::*;

use common::WsTestClient;
mod helpers;
use helpers::*;

#[tokio::test]
async fn test_replayer_multiple_ws_proxies() -> Result<(), anyhow::Error> {
    let recording_path = test_path!().join("data/replayer-test_replayer_multiple_ws_proxies.json");

    let mut macaw = Macaw::<Replayer>::replayer(recording_path)?;
    let proxy1_addr = macaw
        .add_ws_proxy("ws_proxy1", "127.0.0.1:0", WsProxyOptions::default())
        .await?;
    let proxy2_addr = macaw
        .add_ws_proxy("ws_proxy2", "127.0.0.1:0", WsProxyOptions::default())
        .await?;

    macaw.play()?;

    // Connect to proxy 1 and send a message
    let proxy1_url = format!("ws://{}/", proxy1_addr);
    let mut client1 = WsTestClient::connect(&proxy1_url).await?;
    client1.send("hello from proxy1").await?;
    assert_eq!(client1.recv().await?, "echo: hello from proxy1");
    client1.close().await?;

    // Connect to proxy 2 and send a message
    let proxy2_url = format!("ws://{}/", proxy2_addr);
    let mut client2 = WsTestClient::connect(&proxy2_url).await?;
    client2.send("hello from proxy2").await?;
    assert_eq!(client2.recv().await?, "echo: hello from proxy2");
    client2.close().await?;
    Ok(())
}

#[tokio::test]
async fn test_replayer_multiple_ws_conns() -> Result<(), anyhow::Error> {
    let recording_path = test_path!().join("data/replayer-test_replayer_multiple_ws_conns.json");

    let mut macaw = Macaw::<Replayer>::replayer(recording_path)?;
    let proxy_addr = macaw
        .add_ws_proxy("ws_proxy1", "127.0.0.1:0", WsProxyOptions::default())
        .await?;

    macaw.play()?;

    let proxy_url = format!("ws://{}/", proxy_addr);
    let mut client1 = WsTestClient::connect(&proxy_url).await?;
    let mut client2 = WsTestClient::connect(&proxy_url).await?;

    assert_eq!(client1.recv().await?, "msg1 for conn0");
    assert_eq!(client2.recv().await?, "msg1 for conn1");

    client1.send("hello from client1").await?;

    assert_eq!(client1.recv().await?, "msg2 for conn0");
    assert_eq!(client2.recv().await?, "msg2 for conn1");

    client2.send("hello from client2").await?;

    assert_eq!(client1.recv().await?, "echo: hello from client1");
    assert_eq!(client2.recv().await?, "echo: hello from client2");

    client1.close().await?;
    client2.close().await?;
    Ok(())
}

#[tokio::test]
async fn test_replayer_ws_transform() -> Result<(), anyhow::Error> {
    let recording_path = test_path!().join("data/replayer-test_replayer_ws_transform.json");
    let mut macaw = Macaw::<Replayer>::replayer(recording_path)?;

    let options = WsProxyOptions {
        redact: Box::new(TestWsRedact),
        transform: Box::new(TestWsTransform),
    };
    let proxy_addr = macaw
        .add_ws_proxy("ws_proxy", "127.0.0.1:0", options)
        .await?;

    macaw.play()?;

    // Connect to proxy and send a transformed message - this should match a recorded request
    let proxy_url = format!("ws://{}/", proxy_addr);
    let mut client = WsTestClient::connect(&proxy_url).await?;

    // Send a message with transform encoding - the transform will encode it in base64,
    // but the recorded event has "request1", so after decode it should match
    client.send(&encode_text("request1")).await?;

    // Receive response - the recorded event has "response1", but transform will encode it in base64,
    let response = client.recv().await?;
    assert_eq!(response, encode_text("response1"));

    client.close().await?;
    Ok(())
}
