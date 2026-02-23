use crate::lib::*;

#[derive(Debug)]
pub(crate) struct HttpProxyReplayerActor {
    context: ActorContext,
    proxy_id: ProxyId,
    downstream: HttpServer,
    pending_requests: PendingRequests,
    options: HttpProxyOptions,
}

impl Actor for HttpProxyReplayerActor {
    fn context(&self) -> &ActorContext {
        &self.context
    }
}

impl ProxyActor for HttpProxyReplayerActor {
    fn proxy_id(&self) -> ProxyId {
        self.proxy_id
    }
}

impl HttpProxyReplayerActor {
    pub(crate) fn new(
        context: ActorContext,
        proxy_id: ProxyId,
        addr: SocketAddr,
        sender: Box<dyn HttpServerRequestResolver>,
        options: HttpProxyOptions,
    ) -> Result<Self, anyhow::Error> {
        let downstream = HttpServer::new(addr, sender);

        Ok(Self {
            context,
            proxy_id,
            downstream,
            pending_requests: PendingRequests::new(),
            options,
        })
    }

    pub(crate) async fn start(&self) -> Result<SocketAddr, anyhow::Error> {
        self.downstream.start(self.context()).await
    }

    async fn handle_event(
        &mut self,
        recorded_event: RecordedEventWithLock,
    ) -> Result<(), anyhow::Error> {
        let RecordedEventWithLock {
            event, replay_lock, ..
        } = recorded_event;

        match HttpEvent::downcast(event)? {
            HttpEvent::HttpRequest(request) => {
                self.pending_requests
                    .add_replay_request(request, replay_lock)?;
            }
            HttpEvent::HttpResponse(response) => {
                self.pending_requests.resolve_pending_request(response)?;
            }
        }
        Ok(())
    }

    async fn handle_downstream_incoming(
        &mut self,
        request: HttpRequestEvent,
        response_sender: ResponseSender<HttpResponseEvent>,
    ) -> Result<(), anyhow::Error> {
        let request_decoded = self.options.transform.decode_request(request)?;
        let request_overridden = self
            .options
            .overrides
            .http_override_request(request_decoded);
        let request_redacted = self.options.redact.http_redact_request(request_overridden);

        let response_sender =
            HttpResponseSender::new(response_sender, self.options.transform.clone());
        self.pending_requests
            .add_downstream_request(request_redacted, response_sender)
    }
}

impl ActorHandler<RecordedEventWithLock> for HttpProxyReplayerActor {
    type Reply = ();

    async fn handle(&mut self, recorded_event: RecordedEventWithLock) {
        if let Err(e) = self.handle_event(recorded_event).await {
            error!("Failed to handle recorded event: {:?}", e);
        }
    }
}

impl ActorHandler<(HttpRequestEvent, ResponseSender<HttpResponseEvent>)>
    for HttpProxyReplayerActor
{
    type Reply = ();

    async fn handle(
        &mut self,
        (request, response_sender): (HttpRequestEvent, ResponseSender<HttpResponseEvent>),
    ) {
        if let Err(e) = self
            .handle_downstream_incoming(request, response_sender)
            .await
        {
            error!("Failed to handle downstream incoming message: {:?}", e);
        }
    }
}

// ------------------------------------------------------------

#[derive(Debug, Default)]
struct PendingRequests {
    requests: Vec<ReceivedRequest>,
}

impl PendingRequests {
    fn new() -> Self {
        Self {
            requests: Vec::new(),
        }
    }

    fn add_replay_request(
        &mut self,
        replay_request: HttpRequestEvent,
        replay_lock: ReplayLockHolder,
    ) -> Result<(), anyhow::Error> {
        let pending = self
            .requests
            .iter_mut()
            .position(|pending| pending.matches_downstream(&replay_request))
            .map(|index| self.requests.remove(index));

        match pending {
            Some(pending) => match pending {
                ReceivedRequest::Downstream(downstream) => {
                    self.requests.push(ReceivedRequest::Matched(MatchedRequest {
                        replay: replay_request,
                        downstream: downstream.request,
                        response_sender: downstream.response_sender,
                    }));
                    replay_lock.release();
                }
                _request => {
                    return Err(anyhow::anyhow!("Invalid request state"));
                }
            },
            None => {
                self.requests.push(ReceivedRequest::Replay(ReplayRequest {
                    request: replay_request,
                    replay_lock,
                }));
            }
        }
        Ok(())
    }

    fn add_downstream_request(
        &mut self,
        downstream_request: HttpRequestEvent,
        response_sender: HttpResponseSender,
    ) -> Result<(), anyhow::Error> {
        let pending = self
            .requests
            .iter_mut()
            .position(|pending| pending.matches_replay(&downstream_request))
            .map(|index| self.requests.remove(index));

        match pending {
            Some(pending) => match pending {
                ReceivedRequest::Replay(replay) => {
                    self.requests.push(ReceivedRequest::Matched(MatchedRequest {
                        downstream: downstream_request,
                        replay: replay.request,
                        response_sender,
                    }));
                    replay.replay_lock.release();
                }
                request => {
                    return Err(anyhow::anyhow!("Invalid request state: {:?}", request));
                }
            },
            None => {
                self.requests
                    .push(ReceivedRequest::Downstream(DownstreamRequest {
                        request: downstream_request,
                        response_sender,
                    }));
            }
        };
        Ok(())
    }

    fn resolve_pending_request(
        &mut self,
        response: HttpResponseEvent,
    ) -> Result<(), anyhow::Error> {
        let pending = self
            .requests
            .iter_mut()
            .position(|pending| pending.matches_response(&response))
            .map(|index| self.requests.remove(index));

        match pending {
            Some(pending) => match pending {
                ReceivedRequest::Matched(matched) => {
                    matched.response_sender.send(response)?;
                    Ok(())
                }
                request => Err(anyhow::anyhow!("Invalid request state: {:?}", request)),
            },
            None => Err(anyhow::anyhow!(
                "Response not matching with previous replay request: {:?}",
                response
            )),
        }
    }
}

#[derive(Debug)]
enum ReceivedRequest {
    Replay(ReplayRequest),
    Downstream(DownstreamRequest),
    Matched(MatchedRequest),
}

impl ReceivedRequest {
    fn matches_replay(&self, downstream_request: &HttpRequestEvent) -> bool {
        match &self {
            Self::Replay(replay) => replay.request.matches(downstream_request),
            _ => false,
        }
    }

    fn matches_downstream(&self, replay_request: &HttpRequestEvent) -> bool {
        match &self {
            Self::Downstream(downstream) => downstream.request.matches(replay_request),
            _ => false,
        }
    }

    fn matches_response(&self, response: &HttpResponseEvent) -> bool {
        match &self {
            Self::Replay(replay) => replay.request.request_id == response.request_id,
            Self::Matched(matched) => matched.replay.request_id == response.request_id,
            _ => false,
        }
    }
}

#[derive(Debug)]
struct ReplayRequest {
    request: HttpRequestEvent,
    replay_lock: ReplayLockHolder,
}

#[derive(Debug)]
struct DownstreamRequest {
    request: HttpRequestEvent,
    response_sender: HttpResponseSender,
}

#[derive(Debug)]
struct MatchedRequest {
    #[allow(unused)]
    downstream: HttpRequestEvent,
    replay: HttpRequestEvent,
    response_sender: HttpResponseSender,
}
// ------------------------------------------------------------

#[derive(Debug)]
struct HttpResponseSender {
    sender: ResponseSender<HttpResponseEvent>,
    transform: Box<dyn HttpTransform>,
}

impl HttpResponseSender {
    fn new(sender: ResponseSender<HttpResponseEvent>, transform: Box<dyn HttpTransform>) -> Self {
        Self { sender, transform }
    }

    fn send(self, response: HttpResponseEvent) -> Result<(), anyhow::Error> {
        let encoded_response = self.transform.encode_response(response)?;
        self.sender.send(encoded_response)
    }
}

// ------------------------------------------------------------

#[derive(Debug)]
struct StartProxyEvent;

impl ActorHandler<StartProxyEvent> for HttpProxyReplayerActor {
    type Reply = Result<SocketAddr, anyhow::Error>;
    async fn handle(&mut self, _event: StartProxyEvent) -> Result<SocketAddr, anyhow::Error> {
        self.start().await
    }
}

pub trait MacawHttpReplayerSetup {
    fn add_http_proxy(
        &mut self,
        proxy_id: &str,
        addr: &str,
        options: HttpProxyOptions,
    ) -> impl Future<Output = Result<SocketAddr, anyhow::Error>>;
}

impl MacawHttpReplayerSetup for Macaw<Replayer> {
    async fn add_http_proxy(
        &mut self,
        proxy_id: &str,
        addr: &str,
        options: HttpProxyOptions,
    ) -> Result<SocketAddr, anyhow::Error> {
        let (tx, rx) = actor_channel::<HttpProxyReplayerActor>();
        let (_proxy_id, handle) = self
            .add_proxy(move |_replayer, actor_context| {
                let proxy_id: ProxyId = proxy_id.parse()?;
                let context = actor_context.create_child(&proxy_id.to_string());
                let sender = Box::new(tx.clone());
                let actor =
                    HttpProxyReplayerActor::new(context, proxy_id, addr.parse()?, sender, options)?;
                Ok((proxy_id, actor.run_with_channel(tx, rx)))
            })
            .await?;
        let listen_addr = handle.request(StartProxyEvent).await??;
        Ok(listen_addr)
    }
}
