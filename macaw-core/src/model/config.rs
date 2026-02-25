use std::net::SocketAddr;

use crate::lib::*;

#[typetag::serde(tag = "type")]
pub trait ProxyConfig {
    fn bind(&self) -> &str;

    fn target(&self) -> &str;

    fn overrides(&self) -> Option<&str>;

    fn set_root_path(&mut self, path: &Path);

    fn bind_to_recorder<'b, 'a: 'b>(
        &'a self,
        proxy_id: &'b str,
        macaw: &'b mut Macaw<Recorder>,
    ) -> Pin<Box<dyn Future<Output = Result<SocketAddr, anyhow::Error>> + 'b>>;

    fn bind_to_replayer<'b, 'a: 'b>(
        &'a self,
        proxy_id: &'b str,
        macaw: &'b mut Macaw<Replayer>,
    ) -> Pin<Box<dyn Future<Output = Result<SocketAddr, anyhow::Error>> + 'b>>;
}

impl fmt::Debug for Box<dyn ProxyConfig> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Box<dyn ProxyConfig>")
    }
}
