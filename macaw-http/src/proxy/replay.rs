use crate::lib::*;

#[derive(Debug)]
pub(crate) struct HttpReplayProxy {
    id: ProxyId,
    downstream: HttpServer,
    pending_requests: PendingRequests,
}

impl HttpReplayProxy {
    pub(crate) async fn new(
        proxy_id: ProxyId,
        addr: SocketAddr,
        sender: Box<dyn HttpServerRequestResolver>,
    ) -> Result<Self, anyhow::Error> {
        let mut downstream = HttpServer::new(addr, sender);
        downstream.start().await?;

        Ok(Self {
            id: proxy_id,
            downstream,
            pending_requests: PendingRequests::new(),
        })
    }
}

impl ProxyReplayer for HttpReplayProxy {
    type DownstreamIncomingMessage = HttpRequestEvent;
    type DownstreamOutgoingMessage = HttpResponseEvent;
    type RecordedMessage = HttpEvent;

    fn id(&self) -> ProxyId {
        self.id
    }

    async fn downstream_incoming_process(
        &mut self,
        request: HttpRequestEvent,
        response_sender: ResponseSender<HttpResponseEvent>,
    ) -> Result<(), anyhow::Error> {
        debug!(
            "HttpReplayProxy - Downstream incoming process: {:?}",
            request
        );
        self.pending_requests
            .add_downstream_request(request, response_sender)
    }

    async fn handle_recorded_message(
        &mut self,
        event: HttpEvent,
        replay_lock: ReplayLockHolder,
    ) -> Result<(), anyhow::Error> {
        match event {
            HttpEvent::HttpRequest(request) => {
                self.pending_requests
                    .add_replay_request(request, replay_lock)?;
            }
            HttpEvent::HttpResponse(response) => {
                self.pending_requests.resolve_pending_request(response)?;
            }
        }
        debug!(
            "HttpReplayProxy - Handling recorded message, pending requests: {:?}",
            self.pending_requests
        );

        Ok(())
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
                    replay_lock.unlock();
                }
                request => {
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
        response_sender: ResponseSender<HttpResponseEvent>,
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
                    replay.replay_lock.unlock();
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
    response_sender: ResponseSender<HttpResponseEvent>,
}

#[derive(Debug)]
struct MatchedRequest {
    downstream: HttpRequestEvent,
    replay: HttpRequestEvent,
    response_sender: ResponseSender<HttpResponseEvent>,
}

// ------------------------------------------------------------

pub trait HttpMacawReplaySetup {
    fn add_http_proxy(
        &mut self,
        proxy_id: &str,
        addr: &str,
    ) -> impl Future<Output = Result<(), anyhow::Error>>;
}

impl HttpMacawReplaySetup for Macaw<Replayer> {
    async fn add_http_proxy(&mut self, proxy_id: &str, addr: &str) -> Result<(), anyhow::Error> {
        let (tx, rx) = actor_channel::<ProxyReplayerActor<HttpReplayProxy>>();
        let proxy =
            HttpReplayProxy::new(proxy_id.parse()?, addr.parse()?, Box::new(tx.clone())).await?;
        let actor = ProxyReplayerActor::new(proxy);
        self.add_proxy_with_channel(actor, rx, tx).await?;
        Ok(())
    }
}
