use crate::lib::*;

#[derive(Debug, Eq, PartialEq, Hash, Clone, Copy, Serialize, Deserialize)]
pub(crate) enum ProxyId {
    Uuid(Uuid),
}

impl ProxyId {
    pub(crate) fn uuid() -> Self {
        Self::Uuid(Uuid::new_v4())
    }
}

#[derive(Debug, Clone)]
pub(crate) struct TargetUrl {
    scheme: http::uri::Scheme,
    authority: http::uri::Authority,
}

impl TargetUrl {
    pub(crate) fn apply(&self, other: &http::Uri) -> Result<http::Uri, anyhow::Error> {
        Ok(http::uri::Builder::from(other.clone())
            .scheme(self.scheme.clone())
            .authority(self.authority.clone())
            .build()?)
    }
}

impl TryFrom<http::Uri> for TargetUrl {
    type Error = anyhow::Error;
    fn try_from(url: http::Uri) -> Result<Self, Self::Error> {
        Ok(Self {
            scheme: url
                .scheme()
                .cloned()
                .ok_or(anyhow::anyhow!("No scheme in target url"))?,
            authority: url
                .authority()
                .cloned()
                .ok_or(anyhow::anyhow!("No authority in target url"))?,
        })
    }
}

pub(crate) trait Proxy {
    fn id(&self) -> ProxyId;
}

#[derive(Debug)]
pub(crate) struct Proxies<HttpProxy, WsProxy>
where
    HttpProxy: Proxy,
    WsProxy: Proxy,
{
    http: HashMap<ProxyId, HttpProxy>,
    ws: HashMap<ProxyId, WsProxy>,
}

impl<HttpProxy, WsProxy> Proxies<HttpProxy, WsProxy>
where
    HttpProxy: Proxy,
    WsProxy: Proxy,
{
    pub(crate) fn default() -> Self {
        Self {
            http: HashMap::new(),
            ws: HashMap::new(),
        }
    }

    pub(crate) fn insert_http_proxy(&mut self, proxy: HttpProxy) {
        self.http.insert(proxy.id(), proxy);
    }

    pub(crate) fn insert_ws_proxy(&mut self, proxy: WsProxy) {
        self.ws.insert(proxy.id(), proxy);
    }

    pub(crate) fn get_http_proxy(&self, proxy_id: &ProxyId) -> Result<&HttpProxy, anyhow::Error> {
        self.http
            .get(proxy_id)
            .ok_or(anyhow::anyhow!("Proxy not found"))
    }

    pub(crate) fn get_ws_proxy(&self, proxy_id: &ProxyId) -> Result<&WsProxy, anyhow::Error> {
        self.ws
            .get(proxy_id)
            .ok_or(anyhow::anyhow!("Proxy not found"))
    }
}

#[derive(Debug, Clone)]
pub(crate) struct Sender {
    id: ProxyId,
    tx: mpsc::UnboundedSender<DownstreamData>,
}

impl Sender {
    pub(crate) fn new(id: ProxyId, tx: mpsc::UnboundedSender<DownstreamData>) -> Self {
        Self { id, tx }
    }
}

impl HttpServerSender for Sender {
    fn send(&self, envelope: HttpRequestEnvelope) -> Result<(), anyhow::Error> {
        self.tx.send(DownstreamData::Http(self.id, envelope))?;
        Ok(())
    }
}
