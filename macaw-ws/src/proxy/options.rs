use crate::lib::*;

#[derive(Debug, Default)]
pub struct WsProxyOptions {
    pub redact: Box<dyn WsRedact>,
    pub transform: Box<dyn WsTransform>,
    pub overrides: Box<dyn WsOverride>,
}
