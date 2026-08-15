use std::pin::Pin;

use crate::lib::*;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HttpProxyConfig {
    #[serde(default = "default_bind")]
    bind: String,
    #[serde(default)]
    target: Option<String>,
    #[serde(default)]
    overrides: Option<String>,
}

impl HttpProxyConfig {
    pub fn new(bind: impl Into<String>, target: impl Into<String>) -> Self {
        Self {
            bind: bind.into(),
            target: Some(target.into()),
            overrides: None,
        }
    }

    pub fn replay(bind: impl Into<String>) -> Self {
        Self {
            bind: bind.into(),
            target: None,
            overrides: None,
        }
    }

    pub fn with_overrides(mut self, path: impl Into<String>) -> Self {
        self.overrides = Some(path.into());
        self
    }
}

fn default_bind() -> String {
    "127.0.0.1:0".to_string()
}

#[typetag::serde(name = "http")]
impl ProxyConfig for HttpProxyConfig {
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
        if let Some(overrides_path) = &self.overrides {
            self.overrides = Some(path.join(overrides_path).to_string_lossy().to_string());
        }
    }

    fn bind_to_recorder<'b, 'a: 'b>(
        &'a self,
        proxy_id: &'b str,
        macaw: &'b mut Macaw<Recorder>,
    ) -> Pin<Box<dyn Future<Output = Result<SocketAddr, anyhow::Error>> + Send + 'b>> {
        let overrides = load_overrides(self.overrides.as_deref());
        Box::pin(async move {
            let options = HttpProxyOptions {
                redact: Default::default(),
                transform: Default::default(),
                overrides: overrides?,
            };
            let addr = macaw
                .add_http_proxy(
                    proxy_id,
                    &self.bind,
                    self.target.as_deref().unwrap_or_default(),
                    options,
                )
                .await?;
            Ok(addr)
        })
    }

    fn bind_to_replayer<'b, 'a: 'b>(
        &'a self,
        proxy_id: &'b str,
        macaw: &'b mut Macaw<Replayer>,
    ) -> Pin<Box<dyn Future<Output = Result<SocketAddr, anyhow::Error>> + Send + 'b>> {
        let overrides = load_overrides(self.overrides.as_deref());
        Box::pin(async move {
            let options = HttpProxyOptions {
                redact: Default::default(),
                transform: Default::default(),
                overrides: overrides?,
            };
            let addr = macaw.add_http_proxy(proxy_id, &self.bind, options).await?;
            Ok(addr)
        })
    }
}

fn load_overrides(path: Option<&str>) -> Result<Box<dyn HttpOverride>, anyhow::Error> {
    match path {
        Some(p) => {
            if std::path::Path::new(p).exists() {
                Ok(Box::new(HttpOverrideRules::from_file(
                    std::path::Path::new(p),
                )?))
            } else {
                Err(anyhow::anyhow!("Override file not found: {}", p))
            }
        }
        _ => Ok(Default::default()),
    }
}
