use std::net::SocketAddr;

use crate::lib::*;

#[dyn_clonable::clonable]
#[typetag::serde(tag = "type")]
pub trait ProxyConfig: Clone + Send + Sync {
    /// Protocol exposed by this proxy (for example `http` or `ws`).
    fn protocol(&self) -> &'static str;

    fn validate(&self, recording: bool) -> Result<(), String> {
        if recording && self.target().is_none_or(str::is_empty) {
            return Err("recording proxy requires a target".to_owned());
        }
        Ok(())
    }

    fn bind(&self) -> &str;

    fn target(&self) -> Option<&str>;

    fn overrides(&self) -> Option<&str>;

    fn set_root_path(&mut self, path: &Path);

    fn bind_to_recorder<'b, 'a: 'b>(
        &'a self,
        proxy_id: &'b str,
        macaw: &'b mut Macaw<Recorder>,
    ) -> Pin<Box<dyn Future<Output = Result<SocketAddr, anyhow::Error>> + Send + 'b>>;

    fn bind_to_replayer<'b, 'a: 'b>(
        &'a self,
        proxy_id: &'b str,
        macaw: &'b mut Macaw<Replayer>,
    ) -> Pin<Box<dyn Future<Output = Result<SocketAddr, anyhow::Error>> + Send + 'b>>;
}

impl fmt::Debug for Box<dyn ProxyConfig> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Box<dyn ProxyConfig>")
    }
}
