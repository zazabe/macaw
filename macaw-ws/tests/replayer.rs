mod common;

use macaw_core::prelude::*;
use macaw_core::test_path;
use macaw_ws::prelude::*;

use common::WsTestClient;

#[tokio::test]
async fn test_replayer_multiple_ws_proxies() -> Result<(), anyhow::Error> {
    let recording_path = test_path!().join("data/replayer-test_replayer_multiple_ws_proxies.json");

    let mut macaw = Macaw::<Replayer>::replayer(recording_path)?;
    let proxy1_addr = macaw.add_ws_proxy("ws_proxy1", "127.0.0.1:0").await?;
    let proxy2_addr = macaw.add_ws_proxy("ws_proxy2", "127.0.0.1:0").await?;

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
