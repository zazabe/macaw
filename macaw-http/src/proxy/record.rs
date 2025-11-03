use crate::lib::*;

type HttpMessage = Message<HttpRequestEvent, HttpResponseEvent, UnexpectedEvent>;

#[derive(Debug, Clone)]
pub(crate) struct HttpRecordProxy {
    id: ProxyId,
    target_url: TargetUrl,
    upstream: Rc<HttpClient>,
    downstream: Rc<HttpServer>,
}

impl HttpRecordProxy {
    pub(crate) async fn new<Executor>(
        proxy_id: ProxyId,
        executor: Executor,
        addr: SocketAddr,
        target_url: TargetUrl,
    ) -> Result<(mpsc::UnboundedReceiver<HttpMessage>, Self), anyhow::Error>
    where
        Executor: TaskExecutor + Clone,
    {
        let upstream = HttpClient::new(executor.clone(), addr)?;
        let (tx, rx) = mpsc::unbounded_channel();
        let mut downstream = HttpServer::new(addr, Box::new(Sender::new(proxy_id, tx.clone())));
        downstream.start(executor.clone()).await?;

        Ok((
            rx,
            Self {
                id: proxy_id,
                target_url,
                upstream: Rc::new(upstream),
                downstream: Rc::new(downstream),
            },
        ))
    }
}

impl Proxy for HttpRecordProxy {
    fn id(&self) -> ProxyId {
        self.id
    }
}

#[async_trait::async_trait(?Send)]
impl ProxyDownstream for HttpRecordProxy {
    type IncomingMessage = HttpRequestEvent;
    type OutgoingMessage = HttpResponseEvent;

    async fn downstream_incoming_process(
        &self,
        request: HttpRequestEvent,
    ) -> Result<Option<HttpResponseEvent>, anyhow::Error> {
        let mut request = request;
        request.uri = self.target_url.apply(&request.uri)?;
        let request_id = request.request_id;
        let req = request.to_request()?;
        let res = self.upstream.request(req).await?;
        let response = HttpResponseEvent::from_response(&res, request_id)?;
        Ok(Some(response))
    }
}

#[async_trait::async_trait(?Send)]
impl ProxyUpstream for HttpRecordProxy {
    type IncomingMessage = UnexpectedEvent;
}

#[async_trait::async_trait(?Send)]
impl ProxyHandler for HttpRecordProxy {
    type Message = UnexpectedEvent;
}

#[async_trait::async_trait(?Send)]
pub trait HttpMacawRecordSetup {
    async fn add_http_proxy(
        &mut self,
        proxy_id: &str,
        addr: &str,
        target_url: &str,
    ) -> Result<(), anyhow::Error>;
}

#[async_trait::async_trait(?Send)]
impl<Exec> HttpMacawRecordSetup for MacawSetup<Exec, Recorder>
where
    Exec: TaskExecutor,
{
    async fn add_http_proxy(
        &mut self,
        proxy_id: &str,
        addr: &str,
        target_url: &str,
    ) -> Result<(), anyhow::Error> {
        let executor = self.executor();
        let (rx, proxy) = HttpRecordProxy::new(
            proxy_id.parse()?,
            executor,
            addr.parse()?,
            target_url.parse()?,
        )
        .await?;
        self.processor().add_proxy(rx, proxy);
        Ok(())
    }
}
