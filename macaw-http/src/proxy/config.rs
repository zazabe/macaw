use std::pin::Pin;

use crate::lib::*;

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct HttpProxyConfig {
    bind: String,
    target: String,
    overrides: Option<String>,
}

#[typetag::serde(name = "http")]
impl ProxyConfig for HttpProxyConfig {
    fn bind(&self) -> &str {
        &self.bind
    }
    fn target(&self) -> &str {
        &self.target
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
    ) -> Pin<Box<dyn Future<Output = Result<SocketAddr, anyhow::Error>> + 'b>> {
        let overrides = load_overrides(self.overrides.as_deref());
        Box::pin(async move {
            let options = HttpProxyOptions {
                redact: Default::default(),
                transform: Default::default(),
                overrides: overrides?,
            };
            let addr = macaw
                .add_http_proxy(proxy_id, &self.bind, &self.target, options)
                .await?;
            Ok(addr)
        })
    }

    fn bind_to_replayer<'b, 'a: 'b>(
        &'a self,
        proxy_id: &'b str,
        macaw: &'b mut Macaw<Replayer>,
    ) -> Pin<Box<dyn Future<Output = Result<SocketAddr, anyhow::Error>> + 'b>> {
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
