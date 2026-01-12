use macaw_core::prelude::*;
use macaw_http::prelude::*;

#[derive(Clone)]
pub struct TestHttpRedact;

impl HttpRedact for TestHttpRedact {
    fn http_redact_request(&self, mut request: HttpRequestEvent) -> HttpRequestEvent {
        request
            .headers
            .insert("x-signature".to_string(), "REDACTED".to_string());
        request
            .headers
            .insert("x-timestamp".to_string(), "TIMESTAMP".to_string());
        request
    }
}

fn text_wire_encode(prefix: &str, text: &str) -> String {
    format!("{}({})", prefix, text)
}

fn text_wire_decode(prefix: &str, text: &str) -> String {
    regex::Regex::new(&format!("^{}\\((.+)\\)$", prefix))
        .unwrap()
        .replace(text, |caps: &regex::Captures| caps[1].to_string())
        .to_string()
}

#[derive(Clone)]
pub struct TestHttpTransform;

impl HttpTransform for TestHttpTransform {
    fn encode_request(
        &self,
        mut request: HttpRequestEvent,
    ) -> Result<HttpRequestEvent, anyhow::Error> {
        let body = request.body.downcast::<PlainText>().unwrap();
        let encoded_body = text_wire_encode("TX", body.as_str());
        request.body = Box::new(PlainText::new(encoded_body));
        Ok(request)
    }

    fn decode_request(
        &self,
        mut request: HttpRequestEvent,
    ) -> Result<HttpRequestEvent, anyhow::Error> {
        let body = request.body.downcast::<PlainText>().unwrap();
        let decoded_body = text_wire_decode("TX", body.as_str());
        request.body = Box::new(PlainText::new(decoded_body));
        Ok(request)
    }

    fn encode_response(
        &self,
        mut response: HttpResponseEvent,
    ) -> Result<HttpResponseEvent, anyhow::Error> {
        let body = response.body.downcast::<PlainText>().unwrap();
        let encoded_body = text_wire_encode("RX", body.as_str());
        response.body = Box::new(PlainText::new(encoded_body));
        Ok(response)
    }

    fn decode_response(
        &self,
        mut response: HttpResponseEvent,
    ) -> Result<HttpResponseEvent, anyhow::Error> {
        let body = response.body.downcast::<PlainText>().unwrap();
        let decoded_body = text_wire_decode("RX", body.as_str());
        response.body = Box::new(PlainText::new(decoded_body));
        Ok(response)
    }
}
