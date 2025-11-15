use crate::lib::*;

#[derive(Debug)]
pub(crate) struct HttpRecorderProxy {
    id: ProxyId,
    target_url: TargetUrl,
    upstream: HttpClient,
    downstream: HttpServer,
}

impl ProxyRecorder for HttpRecorderProxy {
    type DownstreamIncomingMessage = HttpRequestEvent;
    type DownstreamOutgoingMessage = HttpResponseEvent;
    type UpstreamIncomingMessage = UnexpectedEvent;

    fn id(&self) -> ProxyId {
        self.id
    }

    async fn downstream_incoming_process(
        &mut self,
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

impl HttpRecorderProxy {
    pub(crate) async fn new(
        proxy_id: ProxyId,
        addr: SocketAddr,
        target_url: TargetUrl,
        sender: Box<dyn HttpServerRequestResolver>,
    ) -> Result<Self, anyhow::Error> {
        let upstream = HttpClient::new(addr)?;
        let mut downstream = HttpServer::new(addr, sender);
        downstream.start().await?;

        Ok(Self {
            id: proxy_id,
            target_url,
            upstream,
            downstream,
        })
    }
}

// ------------------------------------------------------------

pub trait HttpMacawRecordSetup {
    fn add_http_proxy(
        &mut self,
        proxy_id: &str,
        addr: &str,
        target_url: &str,
    ) -> impl Future<Output = Result<(), anyhow::Error>>;
}

impl HttpMacawRecordSetup for Macaw<Recorder> {
    async fn add_http_proxy(
        &mut self,
        proxy_id: &str,
        addr: &str,
        target_url: &str,
    ) -> Result<(), anyhow::Error> {
        let (tx, rx) = actor_channel::<ProxyRecorderActor<HttpRecorderProxy>>();
        let proxy = HttpRecorderProxy::new(
            proxy_id.parse()?,
            addr.parse()?,
            target_url.parse()?,
            Box::new(tx.clone()),
        )
        .await?;
        self.add_proxy(move |recorder, actor_context| {
            let proxy_id = proxy.id();
            let actor = ProxyRecorderActor::new(proxy, recorder.clone());
            Ok((proxy_id, actor.run_with_channel(&actor_context, tx, rx)))
        })
    }
}
