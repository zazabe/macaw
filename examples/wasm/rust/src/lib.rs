mod bindings {
    wit_bindgen::generate!({
        path: "../../../macaw-wasm/wit",
        world: "plugin",
    });

    use super::HttpAuthPlugin;
    export!(HttpAuthPlugin);
}

use std::sync::OnceLock;

use bindings::exports::macaw::http::{http_redact, http_transform, lifecycle};
use bindings::macaw::http::types::{Header, Request, Response};
use serde::Deserialize;

#[derive(Deserialize)]
struct Config {
    signature: String,
}

static CONFIG: OnceLock<Config> = OnceLock::new();

struct HttpAuthPlugin;

impl lifecycle::Guest for HttpAuthPlugin {
    fn initialize(config_json: String) -> Result<(), String> {
        let config = serde_json::from_str(&config_json)
            .map_err(|error| format!("invalid plugin configuration: {error}"))?;
        CONFIG
            .set(config)
            .map_err(|_| "plugin is already initialized".to_string())
    }
}

impl http_transform::Guest for HttpAuthPlugin {
    fn decode_request(value: Request) -> Result<Request, String> {
        Ok(value)
    }

    fn encode_request(mut value: Request) -> Result<Request, String> {
        let config = CONFIG
            .get()
            .ok_or_else(|| "plugin is not initialized".to_string())?;
        set_header(&mut value.headers, "x-wasm-signature", &config.signature);
        Ok(value)
    }

    fn decode_response(value: Response) -> Result<Response, String> {
        Ok(value)
    }

    fn encode_response(value: Response) -> Result<Response, String> {
        Ok(value)
    }
}

impl http_redact::Guest for HttpAuthPlugin {
    fn redact_request(mut value: Request) -> Result<Request, String> {
        set_header(&mut value.headers, "x-wasm-signature", "<redacted>");
        Ok(value)
    }
}

fn set_header(headers: &mut Vec<Header>, name: &str, value: &str) {
    if let Some(header) = headers.iter_mut().find(|header| header.name == name) {
        header.value = value.to_string();
    } else {
        headers.push(Header {
            name: name.to_string(),
            value: value.to_string(),
        });
    }
}
