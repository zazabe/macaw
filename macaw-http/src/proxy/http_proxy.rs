use std::net::SocketAddr;

use tokio::sync::mpsc;

use macaw_core::prelude::*;

use crate::io::http::server::{HttpRequestEnvelope, HttpServerRequestSender};
use crate::io::http::{HttpClient, HttpServer};
use crate::model::{HttpRequestEvent, HttpResponseEvent};

type HttpMessage = Message<HttpRequestEvent, HttpResponseEvent, UnexpectedEvent>;

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
        addr: SocketAddr,
        target_url: TargetUrl,
    ) -> Result<(mpsc::UnboundedReceiver<HttpMessage>, Self), anyhow::Error>
    where
        Executor: TaskExecutor + Clone,
    {
        let id = ProxyId::uuid();
        let upstream = HttpClient::new(executor.clone(), addr)?;
        let (tx, rx) = mpsc::unbounded_channel();
        let mut downstream = HttpServer::new(addr, Box::new(Sender::new(id, tx.clone())));
        downstream.start(executor.clone()).await?;

        Ok((
            rx,
            Self {
                id,
                upstream,
                downstream,
                target_url,
            },
        ))
    }
}

impl HttpServerRequestSender for Sender<HttpRequestEvent, HttpResponseEvent, UnexpectedEvent> {
    fn send(&self, envelope: HttpRequestEnvelope) -> Result<(), anyhow::Error> {
        let HttpRequestEnvelope {
            request,
            response_tx,
        } = envelope;
        let request_event = HttpRequestEvent::from_request(&request)?;
        self.tx
            .send(Message::Downstream(DownstreamMessage {
                proxy_id: self.id,
                event: request_event,
                response_tx: Some(response_tx),
            }))
            .map_err(|_| anyhow::anyhow!("Failed to send downstream message"))?;
        Ok(())
    }
}

#[async_trait::async_trait(?Send)]
impl Proxy for HttpRecordProxy {
    type DownstreamInputMessage = HttpRequestEvent;
    type DownstreamOutputMessage = HttpResponseEvent;
    type UpstreamInputMessage = UnexpectedEvent;

    fn id(&self) -> ProxyId {
        self.id
    }

    async fn process_downstream_message(
        &self,
        mut request: HttpRequestEvent,
    ) -> Result<Option<HttpResponseEvent>, anyhow::Error> {
        request.uri = self.target_url.apply(&request.uri)?;
        let req = request.to_request()?;
        let res = self.upstream.request(req).await?;
        let response = HttpResponseEvent::from_response(&res)?;
        Ok(Some(response))
    }
}

#[async_trait::async_trait(?Send)]
pub trait HttpMacawSetup {
    async fn add_http_proxy(
        &mut self,
        addr: SocketAddr,
        target_url: TargetUrl,
    ) -> Result<(), anyhow::Error>;
}

#[async_trait::async_trait(?Send)]
impl<Exec> HttpMacawSetup for MacawSetup<Exec, Recorder>
where
    Exec: TaskExecutor,
{
    async fn add_http_proxy(
        &mut self,
        addr: SocketAddr,
        target_url: TargetUrl,
    ) -> Result<(), anyhow::Error> {
        let executor = self.executor();
        let (rx, proxy) = HttpRecordProxy::new(executor, addr, target_url).await?;
        self.processor().add_proxy(rx, proxy);
        Ok(())
    }
}
