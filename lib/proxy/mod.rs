pub(crate) mod http_proxy;

use std::str::FromStr;

pub(crate) use http_proxy::*;

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

#[async_trait::async_trait(?Send)]
pub(crate) trait Proxy: fmt::Debug + 'static {
    fn id(&self) -> ProxyId;

    async fn redact_downstream_message(
        &self,
        message: Box<dyn RecordEvent>,
    ) -> Result<Box<dyn RecordEvent>, anyhow::Error> {
        Ok(message)
    }

    async fn process_downstream_message(
        &self,
        message: Box<dyn RecordEvent>,
    ) -> Result<Option<Box<dyn RecordEvent>>, anyhow::Error>;

    async fn redact_upstream_message(
        &self,
        message: Box<dyn RecordEvent>,
    ) -> Result<Box<dyn RecordEvent>, anyhow::Error> {
        Ok(message)
    }

    async fn process_upstream_message(
        &self,
        message: Box<dyn RecordEvent>,
    ) -> Result<(), anyhow::Error>;
}

#[derive(Debug)]
pub(crate) struct Proxies {
    proxies: HashMap<ProxyId, Box<dyn Proxy>>,
}

impl Proxies {
    pub(crate) fn default() -> Self {
        Self {
            proxies: HashMap::new(),
        }
    }

    pub(crate) fn insert_proxy<P: Proxy + 'static>(&mut self, proxy: P) {
        self.proxies.insert(proxy.id(), Box::new(proxy));
    }

    pub(crate) fn get_proxy(&self, proxy_id: &ProxyId) -> Result<&dyn Proxy, anyhow::Error> {
        self.proxies
            .get(proxy_id)
            .map(|proxy| proxy.as_ref())
            .ok_or(anyhow::anyhow!("Proxy not found"))
    }
}

#[derive(Debug, Clone)]
pub struct TargetUrl {
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

impl FromStr for TargetUrl {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let uri = http::Uri::from_str(s)?;
        Self::try_from(uri)
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

#[derive(Debug, Clone)]
pub(crate) struct Sender {
    id: ProxyId,
    tx: mpsc::UnboundedSender<Message>,
}

impl Sender {
    pub(crate) fn new(id: ProxyId, tx: mpsc::UnboundedSender<Message>) -> Self {
        Self { id, tx }
    }
}
