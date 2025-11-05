use crate::lib::*;

impl<P> HttpServerRequestSender for ActorChannelSender<ProxyReplayerActor<P>>
where
    P: Proxy,
    P: ProxyDownstream<IncomingMessage = HttpRequestEvent, OutgoingMessage = HttpResponseEvent>,
{
    fn send<'a>(
        &'a self,
        request: HttpRequest,
    ) -> Pin<Box<dyn Future<Output = Result<HttpResponse, anyhow::Error>> + Send + 'a>> {
        Box::pin(async move {
            debug!("Sending request: {:?}", request);
            let request_event = HttpRequestEvent::from_request(&request)?;
            let response_event = self.request(DownstreamMessage::from(request_event)).await?;
            debug!("Received response: {:?}", response_event);
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

impl<P> HttpServerRequestSender for ActorChannelSender<ProxyRecorderActor<P>>
where
    P: Proxy,
    P: ProxyDownstream<IncomingMessage = HttpRequestEvent, OutgoingMessage = HttpResponseEvent>,
{
    fn send<'a>(
        &'a self,
        request: HttpRequest,
    ) -> Pin<Box<dyn Future<Output = Result<HttpResponse, anyhow::Error>> + Send + 'a>> {
        Box::pin(async move {
            debug!("Sending request: {:?}", request);
            let request_event = HttpRequestEvent::from_request(&request)?;
            let response_event = self.request(DownstreamMessage::from(request_event)).await?;
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
