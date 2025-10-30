use crate::lib::*;

#[derive(Debug)]
pub(crate) struct HttpReplayProxy {
    id: ProxyId,
    target_url: TargetUrl,
    downstream: HttpServer,
}

impl HttpReplayProxy {
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
        let mut downstream = HttpServer::new(addr, Box::new(Sender::new(id, tx.clone())));
        downstream.start(executor.clone()).await?;

        Ok(Self {
            id,
            downstream,
            target_url,
        })
    }
}

impl Proxy for HttpReplayProxy {
    fn id(&self) -> ProxyId {
        self.id
    }
}

#[derive(Debug)]
pub(crate) struct WsReplayProxy {
    id: ProxyId,
    upstream: WsClient,
    downstream: WsServer,
}

impl Proxy for WsReplayProxy {
    fn id(&self) -> ProxyId {
        self.id
    }
}
