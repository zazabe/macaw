use crate::lib::*;

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
        tx: &mpsc::UnboundedSender<Message>,
        addr: SocketAddr,
        target_url: TargetUrl,
    ) -> Result<Self, anyhow::Error>
    where
        Executor: TaskExecutor + Clone,
    {
        let id = ProxyId::uuid();
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

impl HttpServerRequestSender for Sender {
    fn send(&self, envelope: HttpRequestEnvelope) -> Result<(), anyhow::Error> {
        let HttpRequestEnvelope {
            request,
            response_tx,
        } = envelope;
        let request_event = HttpRequestEvent::from_request(&request)?;
        self.tx
            .send(Message::Downstream(DownstreamMessage {
                proxy_id: self.id,
                event: Box::new(request_event),
                response_tx: Some(response_tx),
            }))
            .map_err(|_| anyhow::anyhow!("Failed to send downstream message"))?;
        Ok(())
    }
}

#[async_trait::async_trait(?Send)]
impl Proxy for HttpRecordProxy {
    fn id(&self) -> ProxyId {
        self.id
    }

    async fn process_downstream_message(
        &self,
        message: Box<dyn RecordEvent>,
    ) -> Result<Option<Box<dyn RecordEvent>>, anyhow::Error> {
        let mut request: Box<HttpRequestEvent> = message
            .downcast::<HttpRequestEvent>()
            .map_err(|_| anyhow::anyhow!("Failed to downcast message to HttpRequestEvent"))?;

        request.uri = self.target_url.apply(&request.uri)?;
        let req = request.to_request()?;
        let res = self.upstream.request(req).await?;
        let response = HttpResponseEvent::from_response(&res)?;
        Ok(Some(Box::new(response)))
    }

    async fn process_upstream_message(
        &self,
        message: Box<dyn RecordEvent>,
    ) -> Result<(), anyhow::Error> {
        Ok(())
    }
}

trait HttpMacawInterface {
    async fn add_http_proxy<Exec>(
        &mut self,
        executor: Exec,
        addr: SocketAddr,
        target_url: TargetUrl,
    ) -> Result<(), anyhow::Error>
    where
        Exec: TaskExecutor;
}

impl HttpMacawInterface for Recorder {
    async fn add_http_proxy<Exec>(
        &mut self,
        executor: Exec,
        addr: SocketAddr,
        target_url: TargetUrl,
    ) -> Result<(), anyhow::Error>
    where
        Exec: TaskExecutor,
    {
        let proxy = HttpRecordProxy::new(executor, &self.tx, addr, target_url).await?;
        self.proxies.insert_proxy(proxy);
        Ok(())
    }
}

impl<Exec> MacawSetup<Exec, Recorder>
where
    Exec: TaskExecutor,
{
    pub async fn add_http_proxy(
        &mut self,
        addr: SocketAddr,
        target_url: TargetUrl,
    ) -> Result<(), anyhow::Error> {
        self.processor
            .add_http_proxy(self.executor.clone(), addr, target_url)
            .await
    }
}
