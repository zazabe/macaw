use http::Request;
use http_body_util::{BodyExt, Full};

use crate::lib::*;

#[derive(Debug)]
pub struct RecordScheduler {
    events: Vec<RecordEvent>,
    tx: mpsc::UnboundedSender<DownstreamData>,
    rx: mpsc::UnboundedReceiver<DownstreamData>,
    proxies: Proxies<HttpRecordProxy, WsRecordProxy>,
}

impl RecordScheduler {
    pub fn new() -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        Self {
            tx,
            rx,
            proxies: Proxies::<HttpRecordProxy, WsRecordProxy>::default(),
            events: Vec::new(),
        }
    }
}

impl Default for RecordScheduler {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait(?Send)]
impl Scheduler for RecordScheduler {
    async fn add_http_proxy<Exec>(
        &mut self,
        executor: Exec,
        addr: SocketAddr,
        target_url: http::Uri,
    ) -> Result<(), anyhow::Error>
    where
        Exec: TaskExecutor,
    {
        let proxy = HttpRecordProxy::new(executor, &self.tx, addr, target_url).await?;
        self.proxies.insert_http_proxy(proxy);
        Ok(())
    }

    async fn start(&mut self) -> Result<(), anyhow::Error> {
        while let Some(event) = self.rx.recv().await {
            match event {
                DownstreamData::Http(id, envelope) => {
                    let res = self.downstream_http_request(id, envelope.request).await?;
                    envelope
                        .response_tx
                        .send(res)
                        .map_err(|_| anyhow::anyhow!("Failed to send response"))?;
                }
            }
        }
        Ok(())
    }
}

impl RecordScheduler {
    async fn downstream_http_request(
        &mut self,
        id: ProxyId,
        req: HttpRequest,
    ) -> Result<HttpResponse, anyhow::Error> {
        let proxy = self.proxies.get_http_proxy(&id)?;
        let uri = proxy.target_url.apply(req.uri())?;
        let req = {
            let (mut parts, body) = req.into_parts();
            parts.uri = uri;
            HttpRequest::from_parts(parts, body)
        };
        let res = proxy.upstream.request(req).await?;
        Ok(res)
    }
}

#[derive(Debug)]
pub(crate) struct HttpRecordProxy {
    id: ProxyId,
    target_url: TargetUrl,
    upstream: HttpClient,
    downstream: HttpServer,
}

impl HttpRecordProxy {
    pub(crate) async fn new<Executor>(
        executor: Executor,
        tx: &mpsc::UnboundedSender<DownstreamData>,
        addr: SocketAddr,
        target_url: http::Uri,
    ) -> Result<Self, anyhow::Error>
    where
        Executor: TaskExecutor,
    {
        let id = ProxyId::uuid();
        let target_url = TargetUrl::try_from(target_url)?;
        let upstream = HttpClient::new(executor.clone(), addr)?;
        let mut downstream = HttpServer::new(addr, Box::new(Sender::new(id, tx.clone())));
        downstream.start(executor.clone()).await?;

        Ok(Self {
            id,
            upstream,
            downstream,
            target_url,
        })
    }
}

impl Proxy for HttpRecordProxy {
    fn id(&self) -> ProxyId {
        self.id
    }
}

#[derive(Debug)]
pub(crate) struct WsRecordProxy {
    id: ProxyId,
    upstream: WsClient,
    downstream: WsServer,
}

impl Proxy for WsRecordProxy {
    fn id(&self) -> ProxyId {
        self.id
    }
}
