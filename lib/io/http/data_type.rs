use std::{
    convert::Infallible,
    pin::Pin,
    task::{Context, Poll},
};

use bytes::Bytes;
use hyper::body::{Frame, SizeHint};

pub(crate) type HttpRequest = http::Request<BodyBytes>;
pub(crate) type HttpResponse = http::Response<BodyBytes>;

#[derive(Debug, Clone)]
pub(crate) struct BodyBytes(Bytes);

impl BodyBytes {
    pub(crate) fn new(bytes: Bytes) -> Self {
        Self(bytes)
    }

    pub(crate) fn to_bytes(&self) -> &Bytes {
        &self.0
    }
}

impl From<String> for BodyBytes {
    fn from(s: String) -> Self {
        BodyBytes::new(Bytes::from(s))
    }
}

impl From<&str> for BodyBytes {
    fn from(s: &str) -> Self {
        BodyBytes::new(Bytes::from(s.as_bytes().to_vec()))
    }
}

impl From<Vec<u8>> for BodyBytes {
    fn from(v: Vec<u8>) -> Self {
        BodyBytes::new(Bytes::from(v))
    }
}

impl hyper::body::Body for BodyBytes {
    type Data = Bytes;
    type Error = Infallible;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        if !self.0.is_empty() {
            let s = std::mem::take(&mut self.0);
            Poll::Ready(Some(Ok(Frame::data(s))))
        } else {
            Poll::Ready(None)
        }
    }

    fn is_end_stream(&self) -> bool {
        self.0.is_empty()
    }

    fn size_hint(&self) -> SizeHint {
        SizeHint::with_exact(self.0.len() as u64)
    }
}
