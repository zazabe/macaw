use macaw_core::prelude::{Macaw, Recorder};
use macaw_http::prelude::{HttpProxyOptions, MacawHttpRecorderSetup};
use macaw_wasm::prelude::WasmHttpPlugin;

const COMPONENT: &[u8] = include_bytes!("fixtures/http-auth-plugin.wasm");

#[tokio::test]
async fn wasm_plugin_signs_upstream_and_redacts_recording() {
    let recording = tempfile::NamedTempFile::new().unwrap();
    let upstream = httpmock::MockServer::start_async().await;
    let signed_request = upstream.mock(|when, then| {
        when.method(httpmock::Method::GET)
            .path("/signed")
            .header("x-wasm-signature", "integration-signature");
        then.status(200);
    });
    let config = serde_json::json!({ "signature": "integration-signature" });
    let plugin = match std::env::var_os("MACAW_WASM_COMPONENT") {
        Some(path) => WasmHttpPlugin::from_file(path, config),
        None => WasmHttpPlugin::from_bytes(COMPONENT, config),
    }
    .unwrap();

    let mut macaw = Macaw::<Recorder>::recorder();
    let proxy = macaw
        .add_http_proxy(
            "http_proxy",
            "127.0.0.1:0",
            &upstream.base_url(),
            HttpProxyOptions {
                transform: Box::new(plugin.transform()),
                redact: Box::new(plugin.redact()),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let response = reqwest::get(format!("http://{proxy}/signed"))
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    signed_request.assert_calls(1);

    macaw.exit_handle().exit();
    macaw.record_when_exit(recording.path()).await.unwrap();

    let recording_bytes = std::fs::read(recording.path()).unwrap();
    let recorded: serde_json::Value = serde_json::from_slice(&recording_bytes).unwrap();
    assert_eq!(
        recorded["events"][0]["event"]["HttpRequest"]["headers"]["x-wasm-signature"],
        "<redacted>"
    );
    assert!(
        !String::from_utf8(recording_bytes)
            .unwrap()
            .contains("integration-signature")
    );
}
