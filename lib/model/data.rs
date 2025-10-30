use crate::lib::*;

pub(crate) enum DownstreamData {
    Http(ProxyId, HttpRequestEnvelope),
}
