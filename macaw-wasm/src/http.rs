mod bindings {
    wasmtime::component::bindgen!({
        path: "wit",
        world: "plugin",
    });
}

use std::collections::BTreeMap;
use std::fmt;
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard};

use anyhow::{Context, anyhow, bail};
use macaw_core::prelude::Content;
use macaw_http::prelude::{HttpRedact, HttpRequestEvent, HttpResponseEvent, HttpTransform};
use serde_json::Value;
use wasmtime::component::{Component, Linker, ResourceTable};
use wasmtime::{Config, Engine, Store, StoreLimits, StoreLimitsBuilder};
use wasmtime_wasi::{WasiCtx, WasiCtxView, WasiView};

use bindings::macaw::http::types::{Header, HttpVersion, Request, Response};

// Starting CPython and importing a componentized guest costs hundreds of
// millions of Wasmtime fuel units before the first exported hook runs.
const DEFAULT_FUEL_PER_CALL: u64 = 500_000_000;
const DEFAULT_MAX_MEMORY: usize = 64 * 1024 * 1024;

struct HostState {
    limits: StoreLimits,
    wasi: WasiCtx,
    resources: ResourceTable,
}

impl WasiView for HostState {
    fn ctx(&mut self) -> WasiCtxView<'_> {
        WasiCtxView {
            ctx: &mut self.wasi,
            table: &mut self.resources,
        }
    }
}

struct PluginRuntime {
    store: Store<HostState>,
    bindings: bindings::Plugin,
    failed: bool,
}

#[derive(Clone)]
pub struct WasmHttpPlugin {
    runtime: Arc<Mutex<PluginRuntime>>,
}

impl fmt::Debug for WasmHttpPlugin {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WasmHttpPlugin")
            .finish_non_exhaustive()
    }
}

impl WasmHttpPlugin {
    /// Compile and initialize an HTTP plugin from a WebAssembly Component file.
    pub fn from_file(path: impl AsRef<Path>, config: Value) -> Result<Self, anyhow::Error> {
        let mut engine_config = Config::new();
        engine_config.wasm_component_model(true);
        engine_config.consume_fuel(true);
        let engine = map_wasmtime(Engine::new(&engine_config), "Failed to create WASM engine")?;
        let component = map_wasmtime(
            Component::from_file(&engine, path.as_ref()),
            &format!(
                "Failed to compile WASM component: {}",
                path.as_ref().display()
            ),
        )?;
        Self::instantiate(&engine, &component, config)
    }

    /// Compile and initialize an HTTP plugin from WebAssembly Component bytes.
    pub fn from_bytes(bytes: impl AsRef<[u8]>, config: Value) -> Result<Self, anyhow::Error> {
        let mut engine_config = Config::new();
        engine_config.wasm_component_model(true);
        engine_config.consume_fuel(true);
        let engine = map_wasmtime(Engine::new(&engine_config), "Failed to create WASM engine")?;
        let component = map_wasmtime(
            Component::new(&engine, bytes),
            "Failed to compile WASM component",
        )?;
        Self::instantiate(&engine, &component, config)
    }

    fn instantiate(
        engine: &Engine,
        component: &Component,
        config: Value,
    ) -> Result<Self, anyhow::Error> {
        let mut linker = Linker::new(engine);
        map_wasmtime(
            wasmtime_wasi::p2::add_to_linker_sync(&mut linker),
            "Failed to link WASI imports",
        )?;
        let limits = StoreLimitsBuilder::new()
            .memory_size(DEFAULT_MAX_MEMORY)
            .build();
        let mut store = Store::new(
            engine,
            HostState {
                limits,
                wasi: WasiCtx::builder().build(),
                resources: ResourceTable::new(),
            },
        );
        store.limiter(|state| &mut state.limits);
        map_wasmtime(
            store.set_fuel(DEFAULT_FUEL_PER_CALL),
            "Failed to configure WASM fuel",
        )?;

        let bindings = map_wasmtime(
            bindings::Plugin::instantiate(&mut store, component, &linker),
            "Failed to instantiate WASM HTTP plugin",
        )?;
        let config_json =
            serde_json::to_string(&config).context("Failed to serialize WASM plugin config")?;
        map_wasmtime(
            bindings
                .macaw_http_lifecycle()
                .call_initialize(&mut store, &config_json),
            "WASM plugin initialization trapped",
        )?
        .map_err(|message| anyhow!("WASM plugin initialization failed: {message}"))?;

        Ok(Self {
            runtime: Arc::new(Mutex::new(PluginRuntime {
                store,
                bindings,
                failed: false,
            })),
        })
    }

    /// Return a transform backed by this plugin's shared component instance.
    pub fn transform(&self) -> WasmHttpTransform {
        WasmHttpTransform {
            plugin: self.clone(),
        }
    }

    /// Return a redactor backed by this plugin's shared component instance.
    pub fn redact(&self) -> WasmHttpRedact {
        WasmHttpRedact {
            plugin: self.clone(),
        }
    }

    fn lock(&self) -> Result<MutexGuard<'_, PluginRuntime>, anyhow::Error> {
        let runtime = self
            .runtime
            .lock()
            .map_err(|_| anyhow!("WASM HTTP plugin lock is poisoned"))?;
        if runtime.failed {
            bail!("WASM HTTP plugin is unavailable after a previous trap");
        }
        Ok(runtime)
    }
}

#[derive(Clone)]
pub struct WasmHttpTransform {
    plugin: WasmHttpPlugin,
}

impl fmt::Debug for WasmHttpTransform {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WasmHttpTransform")
            .finish_non_exhaustive()
    }
}

impl HttpTransform for WasmHttpTransform {
    fn decode_request(&self, request: HttpRequestEvent) -> Result<HttpRequestEvent, anyhow::Error> {
        self.call_request(request, RequestHook::Decode)
    }

    fn encode_request(&self, request: HttpRequestEvent) -> Result<HttpRequestEvent, anyhow::Error> {
        self.call_request(request, RequestHook::Encode)
    }

    fn decode_response(
        &self,
        response: HttpResponseEvent,
    ) -> Result<HttpResponseEvent, anyhow::Error> {
        self.call_response(response, ResponseHook::Decode)
    }

    fn encode_response(
        &self,
        response: HttpResponseEvent,
    ) -> Result<HttpResponseEvent, anyhow::Error> {
        self.call_response(response, ResponseHook::Encode)
    }
}

impl WasmHttpTransform {
    fn call_request(
        &self,
        request: HttpRequestEvent,
        hook: RequestHook,
    ) -> Result<HttpRequestEvent, anyhow::Error> {
        let request_id = request.request_id;
        let value = request_to_wit(request);
        let mut runtime = self.plugin.lock()?;
        map_wasmtime(
            runtime.store.set_fuel(DEFAULT_FUEL_PER_CALL),
            "Failed to reset WASM fuel",
        )?;
        let PluginRuntime {
            store, bindings, ..
        } = &mut *runtime;
        let result = match hook {
            RequestHook::Decode => bindings
                .macaw_http_http_transform()
                .call_decode_request(store, &value),
            RequestHook::Encode => bindings
                .macaw_http_http_transform()
                .call_encode_request(store, &value),
        };
        let value = handle_guest_result(&mut runtime.failed, result, hook.name())?;
        request_from_wit(value, request_id)
    }

    fn call_response(
        &self,
        response: HttpResponseEvent,
        hook: ResponseHook,
    ) -> Result<HttpResponseEvent, anyhow::Error> {
        let request_id = response.request_id;
        let value = response_to_wit(response);
        let mut runtime = self.plugin.lock()?;
        map_wasmtime(
            runtime.store.set_fuel(DEFAULT_FUEL_PER_CALL),
            "Failed to reset WASM fuel",
        )?;
        let PluginRuntime {
            store, bindings, ..
        } = &mut *runtime;
        let result = match hook {
            ResponseHook::Decode => bindings
                .macaw_http_http_transform()
                .call_decode_response(store, &value),
            ResponseHook::Encode => bindings
                .macaw_http_http_transform()
                .call_encode_response(store, &value),
        };
        let value = handle_guest_result(&mut runtime.failed, result, hook.name())?;
        response_from_wit(value, request_id)
    }
}

#[derive(Clone)]
pub struct WasmHttpRedact {
    plugin: WasmHttpPlugin,
}

impl fmt::Debug for WasmHttpRedact {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WasmHttpRedact")
            .finish_non_exhaustive()
    }
}

impl HttpRedact for WasmHttpRedact {
    fn http_redact_request(
        &self,
        request: HttpRequestEvent,
    ) -> Result<HttpRequestEvent, anyhow::Error> {
        let request_id = request.request_id;
        let value = request_to_wit(request);
        let mut runtime = self.plugin.lock()?;
        map_wasmtime(
            runtime.store.set_fuel(DEFAULT_FUEL_PER_CALL),
            "Failed to reset WASM fuel",
        )?;
        let PluginRuntime {
            store, bindings, ..
        } = &mut *runtime;
        let result = bindings
            .macaw_http_http_redact()
            .call_redact_request(store, &value);
        let value = handle_guest_result(&mut runtime.failed, result, "redact-request")?;
        request_from_wit(value, request_id)
    }
}

enum RequestHook {
    Decode,
    Encode,
}

impl RequestHook {
    fn name(&self) -> &'static str {
        match self {
            Self::Decode => "decode-request",
            Self::Encode => "encode-request",
        }
    }
}

enum ResponseHook {
    Decode,
    Encode,
}

impl ResponseHook {
    fn name(&self) -> &'static str {
        match self {
            Self::Decode => "decode-response",
            Self::Encode => "encode-response",
        }
    }
}

fn handle_guest_result<T>(
    failed: &mut bool,
    result: Result<Result<T, String>, wasmtime::Error>,
    hook: &str,
) -> Result<T, anyhow::Error> {
    match result {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(message)) => Err(anyhow!("WASM HTTP {hook} failed: {message}")),
        Err(error) => {
            *failed = true;
            Err(anyhow!("WASM HTTP {hook} trapped: {error}"))
        }
    }
}

fn map_wasmtime<T>(result: Result<T, wasmtime::Error>, context: &str) -> Result<T, anyhow::Error> {
    result.map_err(|error| anyhow!("{context}: {error}"))
}

fn request_to_wit(request: HttpRequestEvent) -> Request {
    Request {
        request_id: request.request_id.to_string(),
        method: request.method.to_string(),
        uri: request.uri.to_string(),
        version: version_to_wit(request.version),
        headers: headers_to_wit(request.headers),
        body: request.body.to_bytes().to_vec(),
    }
}

fn request_from_wit(
    request: Request,
    expected_request_id: uuid::Uuid,
) -> Result<HttpRequestEvent, anyhow::Error> {
    ensure_request_id(&request.request_id, expected_request_id)?;
    Ok(HttpRequestEvent {
        request_id: expected_request_id,
        method: request
            .method
            .parse()
            .context("WASM plugin returned an invalid HTTP method")?,
        uri: request
            .uri
            .parse()
            .context("WASM plugin returned an invalid HTTP URI")?,
        version: version_from_wit(request.version),
        headers: headers_from_wit(request.headers)?,
        body: Content::from_bytes(&request.body),
    })
}

fn response_to_wit(response: HttpResponseEvent) -> Response {
    Response {
        request_id: response.request_id.to_string(),
        status: response.status.as_u16(),
        version: version_to_wit(response.version),
        headers: headers_to_wit(response.headers),
        body: response.body.to_bytes().to_vec(),
    }
}

fn response_from_wit(
    response: Response,
    expected_request_id: uuid::Uuid,
) -> Result<HttpResponseEvent, anyhow::Error> {
    ensure_request_id(&response.request_id, expected_request_id)?;
    Ok(HttpResponseEvent {
        request_id: expected_request_id,
        status: http::StatusCode::from_u16(response.status)
            .context("WASM plugin returned an invalid HTTP status")?,
        version: version_from_wit(response.version),
        headers: headers_from_wit(response.headers)?,
        body: Content::from_bytes(&response.body),
    })
}

fn ensure_request_id(actual: &str, expected: uuid::Uuid) -> Result<(), anyhow::Error> {
    if actual != expected.to_string() {
        bail!("WASM plugin changed the request ID");
    }
    Ok(())
}

fn headers_to_wit(headers: BTreeMap<String, String>) -> Vec<Header> {
    headers
        .into_iter()
        .map(|(name, value)| Header { name, value })
        .collect()
}

fn headers_from_wit(headers: Vec<Header>) -> Result<BTreeMap<String, String>, anyhow::Error> {
    let mut result = BTreeMap::new();
    for Header { name, value } in headers {
        let name = http::HeaderName::try_from(name)
            .context("WASM plugin returned an invalid HTTP header name")?;
        http::HeaderValue::try_from(&value)
            .context("WASM plugin returned an invalid HTTP header value")?;
        if result.insert(name.to_string(), value).is_some() {
            bail!("WASM plugin returned a duplicate HTTP header");
        }
    }
    Ok(result)
}

fn version_to_wit(version: http::Version) -> HttpVersion {
    match version {
        http::Version::HTTP_09 => HttpVersion::Http09,
        http::Version::HTTP_10 => HttpVersion::Http10,
        http::Version::HTTP_11 => HttpVersion::Http11,
        http::Version::HTTP_2 => HttpVersion::Http2,
        http::Version::HTTP_3 => HttpVersion::Http3,
        _ => unreachable!("unsupported HTTP version"),
    }
}

fn version_from_wit(version: HttpVersion) -> http::Version {
    match version {
        HttpVersion::Http09 => http::Version::HTTP_09,
        HttpVersion::Http10 => http::Version::HTTP_10,
        HttpVersion::Http11 => http::Version::HTTP_11,
        HttpVersion::Http2 => http::Version::HTTP_2,
        HttpVersion::Http3 => http::Version::HTTP_3,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use macaw_core::prelude::PlainText;
    use uuid::Uuid;

    fn request() -> HttpRequestEvent {
        HttpRequestEvent {
            request_id: Uuid::new_v4(),
            method: http::Method::POST,
            uri: "https://example.test/items?limit=1".parse().unwrap(),
            version: http::Version::HTTP_2,
            headers: [
                ("content-type".to_string(), "application/json".to_string()),
                ("x-test".to_string(), "value".to_string()),
            ]
            .into_iter()
            .collect(),
            body: Content::Text(PlainText::new(r#"{"ok":true}"#)),
        }
    }

    fn response(request_id: Uuid) -> HttpResponseEvent {
        HttpResponseEvent {
            request_id,
            status: http::StatusCode::CREATED,
            version: http::Version::HTTP_11,
            headers: [("content-type".to_string(), "application/json".to_string())]
                .into_iter()
                .collect(),
            body: Content::from_bytes(&[0, 1, 2, 255]),
        }
    }

    #[test]
    fn request_round_trips_through_wit_model() {
        let original = request();
        let result =
            request_from_wit(request_to_wit(original.clone()), original.request_id).unwrap();

        assert_eq!(result.request_id, original.request_id);
        assert_eq!(result.method, original.method);
        assert_eq!(result.uri, original.uri);
        assert_eq!(result.version, original.version);
        assert_eq!(result.headers, original.headers);
        assert_eq!(result.body.to_bytes(), original.body.to_bytes());
    }

    #[test]
    fn response_round_trips_through_wit_model() {
        let original = response(Uuid::new_v4());
        let result =
            response_from_wit(response_to_wit(original.clone()), original.request_id).unwrap();

        assert_eq!(result.request_id, original.request_id);
        assert_eq!(result.status, original.status);
        assert_eq!(result.version, original.version);
        assert_eq!(result.headers, original.headers);
        assert_eq!(result.body.to_bytes(), original.body.to_bytes());
    }

    #[test]
    fn rejects_changed_request_id() {
        let original = request();
        let mut value = request_to_wit(original.clone());
        value.request_id = Uuid::new_v4().to_string();

        let error = request_from_wit(value, original.request_id).unwrap_err();
        assert!(error.to_string().contains("changed the request ID"));
    }

    #[test]
    fn rejects_duplicate_headers() {
        let original = request();
        let mut value = request_to_wit(original.clone());
        value.headers.push(Header {
            name: "x-test".to_string(),
            value: "other".to_string(),
        });

        let error = request_from_wit(value, original.request_id).unwrap_err();
        assert!(error.to_string().contains("duplicate HTTP header"));
    }

    #[test]
    fn rejects_non_component_bytes() {
        let error = WasmHttpPlugin::from_bytes(b"not a component", Value::Null).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("Failed to compile WASM component")
        );
    }

    #[test]
    fn guest_error_does_not_disable_plugin() {
        let mut failed = false;
        let result =
            handle_guest_result::<()>(&mut failed, Ok(Err("invalid request".into())), "encode");

        assert!(result.unwrap_err().to_string().contains("invalid request"));
        assert!(!failed);
    }

    #[test]
    fn trap_disables_plugin() {
        let mut failed = false;
        let result = handle_guest_result::<()>(
            &mut failed,
            Err(wasmtime::Error::msg("unreachable")),
            "encode",
        );

        assert!(result.unwrap_err().to_string().contains("trapped"));
        assert!(failed);
    }
}
