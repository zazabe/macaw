use crate::lib::*;

#[derive(Debug)]
pub(crate) struct HttpProxyRecorderActor {
    context: ActorContext,
    proxy_id: ProxyId,
    target_url: TargetUrl,
    recorder: ActorHandle<Recorder>,
    upstream: HttpClient,
    downstream: HttpServer,
    options: HttpProxyOptions,
}

impl Actor for HttpProxyRecorderActor {
    fn context(&self) -> &ActorContext {
        &self.context
    }
}

impl ProxyActor for HttpProxyRecorderActor {
    fn proxy_id(&self) -> ProxyId {
        self.proxy_id
    }
}

impl HttpProxyRecorderActor {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        context: ActorContext,
        proxy_id: ProxyId,
        addr: SocketAddr,
        target_url: TargetUrl,
        recorder: ActorHandle<Recorder>,
        http_client: HttpClient,
        sender: Box<dyn HttpServerRequestResolver>,
        options: HttpProxyOptions,
    ) -> Result<Self, anyhow::Error> {
        let downstream = HttpServer::new(addr, sender);

        Ok(Self {
            context,
            proxy_id,
            target_url,
            recorder,
            upstream: http_client,
            downstream,
            options,
        })
    }

    pub(crate) async fn start(&self) -> Result<SocketAddr, anyhow::Error> {
        self.downstream.start(self.context()).await
    }

    async fn send_request(&self, request: HttpRequestEvent) -> HttpResponseEvent {
        let request_id = request.request_id;
        self.send_request_with_error(request)
            .await
            .unwrap_or_else(|error| HttpResponseEvent::from_transport_error(error, request_id))
    }

    async fn send_request_with_error(
        &self,
        request: HttpRequestEvent,
    ) -> Result<HttpResponseEvent, anyhow::Error> {
        let request_id = request.request_id;
        let req = request.to_request()?;
        let res = self.upstream.request(req).await?;
        HttpResponseEvent::from_response(&res, request_id)
    }

    fn prepare_upstream_request(
        &self,
        mut request: HttpRequestEvent,
    ) -> Result<HttpRequestEvent, anyhow::Error> {
        request.uri = self.target_url.apply(&request.uri)?;
        request.headers.insert(
            http::header::HOST.to_string(),
            self.target_url.authority.to_string(),
        );
        Ok(request)
    }

    fn record<E: RecordEvent>(&self, event: E) -> Result<(), anyhow::Error> {
        self.recorder.send(RecordedEvent::new(self.proxy_id, event))
    }

    async fn process_request(
        &self,
        request: HttpRequestEvent,
    ) -> Result<HttpResponseEvent, anyhow::Error> {
        let request_decoded = self
            .options
            .transform
            .decode_request(request.clone())
            .context("Failed to decode request")?;
        let request_overridden = self
            .options
            .overrides
            .http_override_request(request_decoded);
        let upstream_request = self
            .prepare_upstream_request(request_overridden.clone())
            .context("Failed to prepare upstream request")?;
        let request_encoded = self
            .options
            .transform
            .encode_request(upstream_request)
            .context("Failed to encode request")?;
        let response_future = self.send_request(request_encoded);

        let request_redacted = self
            .options
            .redact
            .http_redact_request(request_overridden.clone())
            .context("Failed to redact request")?;

        self.record(request_redacted)
            .context("Failed to record request")?;

        let response = response_future.await;

        let response_decoded = self
            .options
            .transform
            .decode_response(response)
            .context("Failed to decode response")?;
        let response_overridden = self
            .options
            .overrides
            .http_override_response(response_decoded, request_overridden);

        self.record(response_overridden.clone())
            .context("Failed to record response")?;

        let encoded_response = self
            .options
            .transform
            .encode_response(response_overridden)
            .context("Failed to encode response")?;
        Ok(encoded_response)
    }
}

impl ActorHandler<HttpRequestEvent> for HttpProxyRecorderActor {
    type Reply = HttpResponseEvent;

    async fn handle(&mut self, request: HttpRequestEvent) -> HttpResponseEvent {
        let request_id = request.request_id;
        self.process_request(request).await.unwrap_or_else(|error| {
            HttpResponseEvent::from_macaw_processing_error(error, request_id)
        })
    }
}

// ------------------------------------------------------------

#[derive(Debug)]
struct StartProxyEvent;

impl ActorHandler<StartProxyEvent> for HttpProxyRecorderActor {
    type Reply = Result<SocketAddr, anyhow::Error>;
    async fn handle(&mut self, _event: StartProxyEvent) -> Result<SocketAddr, anyhow::Error> {
        self.start().await
    }
}

pub trait MacawHttpRecorderSetup {
    fn add_http_proxy(
        &mut self,
        proxy_id: &str,
        addr: &str,
        target_url: &str,
        options: HttpProxyOptions,
    ) -> impl Future<Output = Result<SocketAddr, anyhow::Error>>;
}

impl MacawHttpRecorderSetup for Macaw<Recorder> {
    async fn add_http_proxy(
        &mut self,
        proxy_id: &str,
        addr: &str,
        target_url: &str,
        options: HttpProxyOptions,
    ) -> Result<SocketAddr, anyhow::Error> {
        let target_url: TargetUrl = target_url.parse()?;
        let http_client = HttpClient::new(target_url.uri()).await?;
        let (_proxy_id, handle) = self.add_proxy(move |recorder, actor_context| {
            let proxy_id: ProxyId = proxy_id.parse()?;
            let addr: SocketAddr = addr.parse()?;
            let (tx, rx) = actor_channel::<HttpProxyRecorderActor>();
            let sender = Box::new(tx.clone());
            let context = actor_context.create_child(&proxy_id.to_string());
            let actor = HttpProxyRecorderActor::new(
                context,
                proxy_id,
                addr,
                target_url,
                recorder.clone(),
                http_client,
                sender,
                options,
            )?;
            Ok((proxy_id, actor.run_with_channel(tx, rx)))
        })?;
        let listen_addr = handle.request(StartProxyEvent).await??;
        Ok(listen_addr)
    }
}
