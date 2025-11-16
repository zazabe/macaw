use crate::lib::*;
use base64::{Engine, prelude::BASE64_STANDARD};
use std::any::Any;

/// Message wrapping a `RecordEvent` associated with a specific proxy.
#[derive(Debug)]
pub struct RecordedEvent {
    pub proxy_id: ProxyId,
    pub event: Box<dyn RecordEvent>,
}

impl RecordedEvent {
    pub fn new<E: RecordEvent>(proxy_id: ProxyId, event: E) -> Self {
        Self {
            proxy_id,
            event: Box::new(event),
        }
    }
}

#[derive(Debug)]
pub struct RecordedEventWithLock {
    pub proxy_id: ProxyId,
    pub event: Box<dyn RecordEvent>,
    pub replay_lock: ReplayLockHolder,
}

impl RecordedEventWithLock {
    pub fn new<E: RecordEvent>(proxy_id: ProxyId, event: E, replay_lock: ReplayLockHolder) -> Self {
        Self {
            proxy_id,
            event: Box::new(event),
            replay_lock,
        }
    }
}

/// Trait to support ser/de for generic RecordEvent, allowing to record and replay generic events.
#[dyn_clonable::clonable]
#[typetag::serde]
pub trait RecordEvent: Send + Sync + Any + fmt::Debug + Clone + 'static {}

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
    #[serde(rename = "proxy")]
    pub(crate) proxy_id: ProxyId,
    pub(crate) timestamp: DateTime<Utc>,
    #[serde(flatten)]
    pub(crate) data: D,
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

impl Event<Box<dyn RecordEvent>> {
    pub(crate) fn downcast_data<T: RecordEvent + 'static>(
        self,
    ) -> Result<Box<T>, Box<dyn RecordEvent>> {
        self.data.downcast::<T>()
    }
}

// ----------------------------------------

#[dyn_clonable::clonable]
#[typetag::serde]
pub trait Content: Send + Sync + Any + fmt::Debug + Clone + 'static {
    fn to_bytes(&self) -> Result<Bytes, anyhow::Error>;
}

impl dyn Content {
    pub fn from_bytes(bytes: &[u8]) -> Box<dyn Content> {
        if bytes.is_empty() {
            Box::new(Empty)
        } else {
            match String::from_utf8(bytes.to_vec()) {
                Ok(s) => Box::new(PlainText::new(s)),
                Err(_) => Box::new(Base64::from_bytes(bytes)),
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlainText(String);

impl PlainText {
    pub fn new(data: String) -> Self {
        Self(data)
    }
}

#[typetag::serde]
impl Content for PlainText {
    fn to_bytes(&self) -> Result<Bytes, anyhow::Error> {
        Ok(Bytes::from(self.0.as_bytes().to_vec()))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Base64(String);

impl Base64 {
    pub fn from_bytes(bytes: &[u8]) -> Self {
        Self(BASE64_STANDARD.encode(bytes))
    }
}

#[typetag::serde]
impl Content for Base64 {
    fn to_bytes(&self) -> Result<Bytes, anyhow::Error> {
        Ok(Bytes::from(BASE64_STANDARD.decode(self.0.as_bytes())?))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Empty;

#[typetag::serde]
impl Content for Empty {
    fn to_bytes(&self) -> Result<Bytes, anyhow::Error> {
        Ok(Bytes::new())
    }
}
