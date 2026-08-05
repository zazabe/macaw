use crate::lib::*;

impl HttpServerRequestResolver for ActorChannelSender<HttpProxyReplayerActor> {
    fn resolve_request<'a>(
        &'a self,
        request: HttpRequest,
    ) -> Pin<Box<dyn Future<Output = Result<HttpResponse, anyhow::Error>> + Send + 'a>> {
        Box::pin(async move {
            let (response_sender, response_receiver) = response_channel::<HttpResponseEvent>();
            let request_event = HttpRequestEvent::from_request(&request)?;
            self.send((request_event, response_sender))?;
            let response_event = response_receiver.recv().await?;
            response_event.to_response()
        })
    }
}

impl HttpServerRequestResolver for ActorChannelSender<HttpProxyRecorderActor> {
    fn resolve_request<'a>(
        &'a self,
        request: HttpRequest,
    ) -> Pin<Box<dyn Future<Output = Result<HttpResponse, anyhow::Error>> + Send + 'a>> {
        Box::pin(async move {
            let request_event = HttpRequestEvent::from_request(&request)?;
            let request_id = request_event.request_id;
            let response_event = self
                .request(request_event)
                .await
                .unwrap_or_else(|error| HttpResponseEvent::from_internal_error(error, request_id));
            let response = response_event.to_response()?;
            Ok(response)
        })
    }
}
