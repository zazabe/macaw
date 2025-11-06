use crate::lib::*;

impl<P> HttpServerRequestResolver for ActorChannelSender<ProxyReplayerActor<P>>
where
    P: ProxyReplayer<
            DownstreamIncomingMessage = HttpRequestEvent,
            DownstreamOutgoingMessage = HttpResponseEvent,
        >,
{
    fn resolve_request<'a>(
        &'a self,
        request: HttpRequest,
    ) -> Pin<Box<dyn Future<Output = Result<HttpResponse, anyhow::Error>> + Send + 'a>> {
        Box::pin(async move {
            let (response_sender, response_receiver) = response_channel::<HttpResponseEvent>();
            let request_event = HttpRequestEvent::from_request(&request)?;
            debug!(
                "HttpServerRequestResolver - Sent request: {:?}",
                request_event
            );
            self.send(DownstreamMessageWithResponseSender::new(
                request_event,
                response_sender,
            ))?;
            let response_event = response_receiver.recv().await?;
            debug!(
                "HttpServerRequestResolver - Received response: {:?}",
                response_event
            );
            response_event.to_response()
        })
    }
}

impl<P> HttpServerRequestResolver for ActorChannelSender<ProxyRecorderActor<P>>
where
    P: ProxyRecorder<
            DownstreamIncomingMessage = HttpRequestEvent,
            DownstreamOutgoingMessage = HttpResponseEvent,
        >,
{
    fn resolve_request<'a>(
        &'a self,
        request: HttpRequest,
    ) -> Pin<Box<dyn Future<Output = Result<HttpResponse, anyhow::Error>> + Send + 'a>> {
        Box::pin(async move {
            let request_event = HttpRequestEvent::from_request(&request)?;
            let response_event = self.request(DownstreamMessage::new(request_event)).await?;
            let response = match response_event {
                Some(response_event) => response_event.to_response()?,
                None => http::Response::builder()
                    .status(StatusCode::NOT_FOUND)
                    .body(BodyBytes::empty())?,
            };
            Ok(response)
        })
    }
}
