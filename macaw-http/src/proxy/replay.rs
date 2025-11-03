use crate::lib::*;

type HttpMessage = Message<HttpRequestEvent, HttpResponseEvent, UnexpectedEvent>;

#[derive(Debug, Clone)]
pub(crate) struct HttpReplayProxy {
    id: ProxyId,
    downstream: Rc<HttpServer>,
    pending_requests: PendingRequests,
}

impl HttpReplayProxy {
    pub(crate) async fn new<Executor>(
        proxy_id: ProxyId,
        executor: Executor,
        addr: SocketAddr,
    ) -> Result<(mpsc::UnboundedReceiver<HttpMessage>, Self), anyhow::Error>
    where
        Executor: TaskExecutor + Clone,
    {
        let (replayer_tx, replayer_rx) = mpsc::unbounded_channel();
        let mut downstream =
            HttpServer::new(addr, Box::new(Sender::new(proxy_id, replayer_tx.clone())));
        downstream.start(executor).await?;

        Ok((
            replayer_rx,
            Self {
                id: proxy_id,
                downstream: Rc::new(downstream),
                pending_requests: PendingRequests::new(),
            },
        ))
    }
}

impl Proxy for HttpReplayProxy {
    fn id(&self) -> ProxyId {
        self.id
    }
}

#[async_trait::async_trait(?Send)]
impl ProxyDownstream for HttpReplayProxy {
    type IncomingMessage = HttpRequestEvent;
    type OutgoingMessage = HttpResponseEvent;

    async fn downstream_incoming_process(
        &self,
        request: HttpRequestEvent,
    ) -> Result<Option<HttpResponseEvent>, anyhow::Error> {
        debug!("Downstream incoming process: {:?}", request);
        let response = self.pending_requests.wait_for_response(request).await?;
        Ok(Some(response))
    }
}

#[async_trait::async_trait(?Send)]
impl ProxyUpstream for HttpReplayProxy {
    type IncomingMessage = UnexpectedEvent;
}

#[async_trait::async_trait(?Send)]
impl ProxyHandler for HttpReplayProxy {
    type Message = HttpEvent;

    async fn handle_message(&self, event: HttpEvent) -> Result<(), anyhow::Error> {
        match event {
            HttpEvent::HttpRequest(request) => {
                self.pending_requests.add_replay_request(request)?;
            }
            HttpEvent::HttpResponse(response) => {
                self.pending_requests.wait_for_request(response).await?;
            }
        }
        debug!("Handled message: {:?}", self.pending_requests);
        Ok(())
    }
}

#[derive(Debug, Clone, Default)]
struct PendingRequests(Rc<RefCell<PendingRequestsInner>>);

impl PendingRequests {
    fn new() -> Self {
        Self(Default::default())
    }

    fn add_replay_request(&self, request: HttpRequestEvent) -> Result<(), anyhow::Error> {
        self.0.borrow_mut().add_replay_request(request)
    }

    async fn wait_for_request(&self, response: HttpResponseEvent) -> Result<(), anyhow::Error> {
        let mut rx = self.0.borrow_mut().add_response(response)?;
        if rx.borrow_and_update().is_some() {
            return Ok(());
        }
        let changed_value = rx.wait_for(|v| v.is_some()).await?;
        Ok(())
    }

    async fn wait_for_response(
        &self,
        request: HttpRequestEvent,
    ) -> Result<HttpResponseEvent, anyhow::Error> {
        let mut rx = self.0.borrow_mut().add_downstream_request(request)?;
        if let Some(response) = rx.borrow_and_update().clone() {
            return Ok(response);
        }
        let changed_value = rx.wait_for(|v| v.is_some()).await?;
        let response = changed_value.clone().unwrap_or_else(|| unreachable!());
        Ok(response)
    }
}

#[derive(Debug, Default)]
struct PendingRequestsInner {
    pending_responses: Vec<PendingResponse>,
}

impl PendingRequestsInner {
    fn new() -> Self {
        Self {
            pending_responses: Vec::new(),
        }
    }

    fn add_replay_request(&mut self, request: HttpRequestEvent) -> Result<(), anyhow::Error> {
        use self::PendingResponseState::*;
        let replay_pending = {
            let downstream_index = self
                .pending_responses
                .iter()
                .position(|pending| pending.match_replay_request(&request));
            downstream_index.map(|index| self.pending_responses.remove(index))
        };

        match replay_pending {
            Some(mut pending) => match pending.state {
                DownstreamReceived(downstream) => {
                    pending.state = PendingResponseState::RequestMatched {
                        downstream,
                        replay: request,
                    };
                    self.pending_responses.push(pending);
                    Ok(())
                }
                _ => {
                    self.pending_responses.push(pending);
                    Err(anyhow::anyhow!(
                        "Replay request already received, request id: {}",
                        request.request_id
                    ))
                }
            },
            None => {
                self.pending_responses
                    .push(PendingResponse::replay_received(request));
                Ok(())
            }
        }
    }

    fn add_downstream_request(
        &mut self,
        request: HttpRequestEvent,
    ) -> Result<watch::Receiver<Option<HttpResponseEvent>>, anyhow::Error> {
        use self::PendingResponseState::*;

        let downstream_pending = {
            let downstream_index = self
                .pending_responses
                .iter()
                .position(|pending| pending.match_downstream_request(&request));
            downstream_index.map(|index| self.pending_responses.remove(index))
        };

        match downstream_pending {
            Some(mut pending) => match pending.state {
                ReplayReceived(replay) => {
                    pending.state = PendingResponseState::RequestMatched {
                        downstream: request,
                        replay,
                    };
                    let rx = pending.proxy_response.subscribe();
                    self.pending_responses.push(pending);
                    Ok(rx)
                }
                ResponsePending { response, replay } => {
                    pending
                        .proxy_response
                        .send(Some(response))
                        .map_err(|_| anyhow::anyhow!("Failed to send response"))?;
                    let rx = pending.proxy_response.subscribe();
                    Ok(rx)
                }
                _ => {
                    self.pending_responses.push(pending);
                    Err(anyhow::anyhow!(
                        "Replay request already received, request id: {}",
                        request.request_id
                    ))
                }
            },
            None => {
                let pending = PendingResponse::downstream_received(request);
                let rx = pending.proxy_response.subscribe();
                self.pending_responses.push(pending);
                Ok(rx)
            }
        }
    }

    fn add_response(
        &mut self,
        response: HttpResponseEvent,
    ) -> Result<watch::Receiver<Option<HttpResponseEvent>>, anyhow::Error> {
        use self::PendingResponseState::*;

        let pending = {
            let downstream_index = self
                .pending_responses
                .iter()
                .position(|pending| pending.match_response(&response));
            downstream_index.map(|index| self.pending_responses.remove(index))
        };

        match pending {
            Some(pending) => match pending.state {
                ReplayReceived(replay) => {
                    let proxy_response = pending.proxy_response.clone();
                    let rx = proxy_response.subscribe();
                    self.pending_responses.push(PendingResponse::new(
                        PendingResponseState::ResponsePending { response, replay },
                        proxy_response,
                    ));
                    Ok(rx)
                }
                RequestMatched { downstream, replay } => {
                    pending
                        .proxy_response
                        .send(Some(response))
                        .map_err(|_| anyhow::anyhow!("Failed to send response"))?;
                    let rx = pending.proxy_response.subscribe();
                    Ok(rx)
                }
                _ => Err(anyhow::anyhow!(
                    "Response already received, response id: {}",
                    response.request_id
                )),
            },
            None => Err(anyhow::anyhow!(
                "At least a replay request should have been received already for response id: {}",
                response.request_id
            )),
        }
    }
}

#[derive(Debug)]
struct PendingResponse {
    state: PendingResponseState,
    proxy_response: watch::Sender<Option<HttpResponseEvent>>,
}

impl PendingResponse {
    fn new(
        state: PendingResponseState,
        proxy_response: watch::Sender<Option<HttpResponseEvent>>,
    ) -> Self {
        Self {
            state,
            proxy_response,
        }
    }

    fn replay_received(request: HttpRequestEvent) -> Self {
        let (tx, _rx) = watch::channel(None);
        Self::new(PendingResponseState::ReplayReceived(request), tx)
    }

    fn downstream_received(request: HttpRequestEvent) -> Self {
        let (tx, _rx) = watch::channel(None);
        Self::new(PendingResponseState::DownstreamReceived(request), tx)
    }

    fn match_downstream_request(&self, request: &HttpRequestEvent) -> bool {
        use self::PendingResponseState::*;
        match &self.state {
            DownstreamReceived(downstream) => downstream.request_id == request.request_id,
            ReplayReceived(replay) => replay.matches(request),
            ResponsePending { response, replay } => replay.matches(request),
            RequestMatched { downstream, replay } => {
                downstream.request_id == request.request_id && replay.matches(request)
            }
        }
    }

    fn match_replay_request(&self, request: &HttpRequestEvent) -> bool {
        use self::PendingResponseState::*;
        match &self.state {
            DownstreamReceived(downstream) => downstream.matches(request),
            ReplayReceived(replay) => replay.request_id == request.request_id,
            ResponsePending { response, replay } => replay.request_id == request.request_id,
            RequestMatched { downstream, replay } => {
                replay.request_id == request.request_id && downstream.matches(request)
            }
        }
    }

    fn match_response(&self, response: &HttpResponseEvent) -> bool {
        use self::PendingResponseState::*;
        match &self.state {
            ReplayReceived(replay) => replay.request_id == response.request_id,
            ResponsePending {
                response: pending_response,
                replay,
            } => pending_response.request_id == response.request_id,
            RequestMatched { downstream, replay } => replay.request_id == response.request_id,
            DownstreamReceived(..) => false,
        }
    }
}

#[derive(Debug)]
enum PendingResponseState {
    ReplayReceived(HttpRequestEvent),
    DownstreamReceived(HttpRequestEvent),
    RequestMatched {
        downstream: HttpRequestEvent,
        replay: HttpRequestEvent,
    },
    ResponsePending {
        response: HttpResponseEvent,
        replay: HttpRequestEvent,
    },
}

#[async_trait::async_trait(?Send)]
pub trait HttpMacawReplaySetup {
    async fn add_http_proxy(&mut self, proxy_id: &str, addr: &str) -> Result<(), anyhow::Error>;
}

#[async_trait::async_trait(?Send)]
impl<Exec> HttpMacawReplaySetup for MacawSetup<Exec, Replayer>
where
    Exec: TaskExecutor,
{
    async fn add_http_proxy(&mut self, proxy_id: &str, addr: &str) -> Result<(), anyhow::Error> {
        let executor = self.executor();
        let (rx, proxy) = HttpReplayProxy::new(proxy_id.parse()?, executor, addr.parse()?).await?;
        self.processor().add_proxy(rx, proxy);
        Ok(())
    }
}
