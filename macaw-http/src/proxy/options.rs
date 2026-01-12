use crate::lib::*;

#[derive(Debug, Default)]
pub struct HttpProxyOptions {
    pub redact: Box<dyn HttpRedact>,
    pub transform: Box<dyn HttpTransform>,
}
