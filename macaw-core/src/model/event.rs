use std::any::Any;
use std::fmt;

use base64::{Engine, prelude::BASE64_STANDARD};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::processor::proxy::ProxyId;

#[dyn_clonable::clonable]
#[typetag::serde(tag = "type")]
pub trait RecordEvent: Any + fmt::Debug + Clone + 'static {}

impl dyn RecordEvent {
    pub fn downcast<T: RecordEvent + 'static>(self: Box<Self>) -> Result<Box<T>, Box<Self>> {
        if (*self).as_any().is::<T>() {
            // It is sound to convert; the trait object is actually T
            Ok(self.downcast_unchecked())
        } else {
            Err(self)
        }
    }

    // Helper for unchecked downcast (only call if is::<T>() successful)
    fn downcast_unchecked<T: RecordEvent + 'static>(self: Box<Self>) -> Box<T> {
        unsafe { Box::from_raw(Box::into_raw(self) as *mut T) }
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct UnexpectedEvent;

#[typetag::serde]
impl RecordEvent for UnexpectedEvent {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct RecordHeader {
    pub(crate) record_id: String,
    pub(crate) record_seed: String,
    pub(crate) timestamp: DateTime<Utc>,
}

impl RecordHeader {
    pub(crate) fn new() -> Self {
        Self {
            record_id: Uuid::new_v4().to_string(),
            record_seed: Uuid::new_v4().to_string(),
            timestamp: Utc::now(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Event<D> {
    proxy_id: ProxyId,
    timestamp: DateTime<Utc>,
    data: D,
}

impl<D> Event<D> {
    pub(crate) fn new(proxy_id: ProxyId, data: D) -> Self {
        Self {
            proxy_id,
            timestamp: Utc::now(),
            data,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Content {
    encoding: ContentEncoding,
    data: String,
}

impl Content {
    pub fn from_bytes(bytes: &[u8]) -> Self {
        match String::from_utf8(bytes.to_vec()) {
            Ok(s) => Self {
                encoding: ContentEncoding::Plain,
                data: s,
            },
            Err(e) => Self {
                encoding: ContentEncoding::Base64,
                data: BASE64_STANDARD.encode(e.as_bytes()),
            },
        }
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, anyhow::Error> {
        match self.encoding {
            ContentEncoding::Plain => Ok(self.data.as_bytes().to_vec()),
            ContentEncoding::Base64 => Ok(BASE64_STANDARD.decode(&self.data)?),
        }
    }

    pub fn from_string(s: String) -> Self {
        Self {
            encoding: ContentEncoding::Plain,
            data: s,
        }
    }

    pub fn to_string(&self) -> Result<String, anyhow::Error> {
        match self.encoding {
            ContentEncoding::Plain => Ok(self.data.clone()),
            ContentEncoding::Base64 => {
                let bytes = BASE64_STANDARD.decode(&self.data)?;
                Ok(String::from_utf8(bytes)?)
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum ContentEncoding {
    Plain,
    Base64,
}
