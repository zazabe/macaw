use std::future::Future;
use std::net::SocketAddr;
use std::path::Path;
use std::pin::Pin;

use macaw_core::prelude::{Macaw, ProxyConfig, Recorder, Replayer};
use macaw_http::prelude::{
    HttpOverride, HttpOverrideRules, HttpProxyOptions, MacawHttpRecorderSetup,
    MacawHttpReplayerSetup,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::http::WasmHttpPlugin;

/// HTTP proxy configuration backed by an isolated WASM component instance.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WasmHttpProxyConfig {
    #[serde(default = "default_bind")]
    bind: String,
    #[serde(default)]
    target: Option<String>,
    #[serde(default)]
    overrides: Option<String>,
    component: String,
    #[serde(default)]
    config: Value,
}

impl WasmHttpProxyConfig {
    pub fn new(
        bind: impl Into<String>,
        target: impl Into<String>,
        component: impl Into<String>,
        config: Value,
    ) -> Self {
        Self {
            bind: bind.into(),
            target: Some(target.into()),
            overrides: None,
            component: component.into(),
            config,
        }
    }

    pub fn replay(bind: impl Into<String>, component: impl Into<String>, config: Value) -> Self {
        Self {
            bind: bind.into(),
            target: None,
            overrides: None,
            component: component.into(),
            config,
        }
    }

    pub fn with_overrides(mut self, path: impl Into<String>) -> Self {
        self.overrides = Some(path.into());
        self
    }

    fn options(&self) -> Result<HttpProxyOptions, anyhow::Error> {
        let plugin = WasmHttpPlugin::from_file(&self.component, self.config.clone())?;
        Ok(HttpProxyOptions {
            redact: Box::new(plugin.redact()),
            transform: Box::new(plugin.transform()),
            overrides: load_overrides(self.overrides.as_deref())?,
        })
    }
}

fn default_bind() -> String {
    "127.0.0.1:0".to_string()
}

#[typetag::serde(name = "wasm_http")]
impl ProxyConfig for WasmHttpProxyConfig {
    fn protocol(&self) -> &'static str {
        "http"
    }

    fn validate(&self, recording: bool) -> Result<(), String> {
        let Some(target) = self.target.as_deref().filter(|target| !target.is_empty()) else {
            return if recording {
                Err("recording proxy requires a target".to_owned())
            } else {
                Ok(())
            };
        };
        let target = url::Url::parse(target).map_err(|_| "invalid HTTP proxy target".to_owned())?;
        if !matches!(target.scheme(), "http" | "https") || target.host_str().is_none() {
            return Err("invalid HTTP proxy target".to_owned());
        }
        Ok(())
    }

    fn bind(&self) -> &str {
        &self.bind
    }

    fn target(&self) -> Option<&str> {
        self.target.as_deref()
    }

    fn overrides(&self) -> Option<&str> {
        self.overrides.as_deref()
    }

    fn set_root_path(&mut self, path: &Path) {
        if let Some(overrides) = &self.overrides {
            self.overrides = Some(path.join(overrides).to_string_lossy().into_owned());
        }
        self.component = path.join(&self.component).to_string_lossy().into_owned();
    }

    fn bind_to_recorder<'b, 'a: 'b>(
        &'a self,
        proxy_id: &'b str,
        macaw: &'b mut Macaw<Recorder>,
    ) -> Pin<Box<dyn Future<Output = Result<SocketAddr, anyhow::Error>> + Send + 'b>> {
        let options = self.options();
        Box::pin(async move {
            macaw
                .add_http_proxy(
                    proxy_id,
                    &self.bind,
                    self.target.as_deref().unwrap_or_default(),
                    options?,
                )
                .await
        })
    }

    fn bind_to_replayer<'b, 'a: 'b>(
        &'a self,
        proxy_id: &'b str,
        macaw: &'b mut Macaw<Replayer>,
    ) -> Pin<Box<dyn Future<Output = Result<SocketAddr, anyhow::Error>> + Send + 'b>> {
        let options = self.options();
        Box::pin(async move { macaw.add_http_proxy(proxy_id, &self.bind, options?).await })
    }
}

fn load_overrides(path: Option<&str>) -> Result<Box<dyn HttpOverride>, anyhow::Error> {
    match path {
        Some(path) => Ok(Box::new(HttpOverrideRules::from_file(Path::new(path))?)),
        None => Ok(Default::default()),
    }
}
