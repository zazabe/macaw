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

/// Direction of traffic for debug output.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DebugDirection {
    /// Downstream to upstream (client → server)
    DownstreamToUpstream,
    /// Upstream to downstream (server → client)
    UpstreamToDownstream,
}

impl DebugDirection {
    pub fn arrow(&self) -> &'static str {
        match self {
            Self::DownstreamToUpstream => "→",
            Self::UpstreamToDownstream => "←",
        }
    }
}

#[derive(Debug, Clone)]
pub enum RecordPart {
    // e.g. HTTP, WS, etc...
    StreamType(String),
    // e.g. Body, WS message, etc...
    Content(String),
    // Message ID if any
    Id(String),
    // Metadata like HTTP Method, Status, etc...
    Meta(String),
}

impl RecordPart {
    pub fn len(&self) -> usize {
        match self {
            Self::StreamType(s) => s.len(),
            Self::Content(s) => s.len(),
            Self::Id(s) => s.len(),
            Self::Meta(s) => s.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            Self::StreamType(s) => s.is_empty(),
            Self::Content(s) => s.is_empty(),
            Self::Id(s) => s.is_empty(),
            Self::Meta(s) => s.is_empty(),
        }
    }

    pub fn replace(&self, text: &str) -> Self {
        match self {
            Self::StreamType(..) => Self::StreamType(text.to_string()),
            Self::Content(..) => Self::Content(text.to_string()),
            Self::Id(..) => Self::Id(text.to_string()),
            Self::Meta(..) => Self::Meta(text.to_string()),
        }
    }
}

impl fmt::Display for RecordPart {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::StreamType(s) => write!(f, "{}", s),
            Self::Content(s) => write!(f, "{}", s),
            Self::Id(s) => write!(f, "{}", s),
            Self::Meta(s) => write!(f, "{}", s),
        }
    }
}

#[derive(Debug)]
pub struct RecordFormatter {
    direction: DebugDirection,
    parts: Vec<RecordPart>,
}

impl RecordFormatter {
    pub fn new(direction: DebugDirection, parts: Vec<RecordPart>) -> Self {
        Self { direction, parts }
    }

    pub fn direction(&self) -> DebugDirection {
        self.direction
    }

    pub fn parts(&self) -> &[RecordPart] {
        &self.parts
    }
}

/// Trait to support ser/de for generic RecordEvent, allowing to record and replay generic events.
#[dyn_clonable::clonable]
#[typetag::serde]
pub trait RecordEvent: Send + Sync + Any + fmt::Debug + Clone + 'static {
    /// Format this event for human-readable debug output. Returns (direction, formatted string).
    fn format_debug(&self) -> RecordFormatter {
        RecordFormatter::new(
            DebugDirection::DownstreamToUpstream,
            vec![RecordPart::Content(format!("{:?}", self))],
        )
    }
}

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

// ----------------------------------------

#[derive(Debug, Clone)]
pub enum Content {
    Text(PlainText),
    Bytes(Base64),
    Empty,
}

impl Content {
    pub fn from_bytes(bytes: &[u8]) -> Self {
        if bytes.is_empty() {
            Self::Empty
        } else {
            match String::from_utf8(bytes.to_vec()) {
                Ok(s) => Self::Text(PlainText::new(s)),
                Err(_) => Self::Bytes(Base64::from_bytes(bytes)),
            }
        }
    }

    pub fn to_bytes(&self) -> Bytes {
        match self {
            Self::Text(text) => Bytes::from(text.as_str().as_bytes().to_vec()),
            Self::Bytes(base64) => base64.0.clone(),
            Self::Empty => Bytes::new(),
        }
    }

    pub fn to_text(&self) -> Result<String, anyhow::Error> {
        match self {
            Self::Text(text) => Ok(text.as_str().to_string()),
            _ => Err(anyhow::anyhow!("Content is not a text")),
        }
    }

    pub fn is_empty(&self) -> bool {
        matches!(self, Self::Empty)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlainText(String);

impl PlainText {
    pub fn new<S: ToString>(data: S) -> Self {
        Self(data.to_string())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone)]
pub struct Base64(Bytes);

impl Base64 {
    pub fn from_bytes(bytes: &[u8]) -> Self {
        Self(Bytes::from(bytes.to_vec()))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Empty;

// ----------------------------------------

impl From<String> for Content {
    fn from(s: String) -> Self {
        Content::Text(PlainText::new(s))
    }
}

impl From<&str> for Content {
    fn from(s: &str) -> Self {
        Content::Text(PlainText::new(s.to_string()))
    }
}

impl From<Bytes> for Content {
    fn from(bytes: Bytes) -> Self {
        Content::Bytes(Base64::from_bytes(&bytes))
    }
}

impl From<Vec<u8>> for Content {
    fn from(bytes: Vec<u8>) -> Self {
        Content::Bytes(Base64::from_bytes(&bytes))
    }
}

impl From<()> for Content {
    fn from(_: ()) -> Self {
        Content::Empty
    }
}

impl<T: Into<Content>> From<Option<T>> for Content {
    fn from(option: Option<T>) -> Self {
        match option {
            Some(value) => value.into(),
            None => Content::Empty,
        }
    }
}

impl Serialize for Content {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            Content::Text(text) => serializer.serialize_str(text.as_str()),
            Content::Bytes(base64) => {
                use serde::ser::SerializeMap;
                let base64_str = BASE64_STANDARD.encode(&base64.0);
                let mut map = serializer.serialize_map(Some(1))?;
                map.serialize_entry("bytes", &base64_str)?;
                map.end()
            }
            Content::Empty => serializer.serialize_none(),
        }
    }
}

impl<'de> Deserialize<'de> for Content {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::{self, Visitor};
        use std::fmt;

        struct ContentVisitor;

        impl<'de> Visitor<'de> for ContentVisitor {
            type Value = Content;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a string, an object with 'bytes' key, or null")
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(Content::Text(PlainText::new(value.to_string())))
            }

            fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(Content::Text(PlainText::new(value)))
            }

            fn visit_none<E>(self) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(Content::Empty)
            }

            fn visit_unit<E>(self) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(Content::Empty)
            }

            fn visit_map<V>(self, mut map: V) -> Result<Self::Value, V::Error>
            where
                V: de::MapAccess<'de>,
            {
                let mut bytes_value: Option<String> = None;
                while let Some(key) = map.next_key::<String>()? {
                    if key == "bytes" {
                        if bytes_value.is_some() {
                            return Err(de::Error::duplicate_field("bytes"));
                        }
                        bytes_value = Some(map.next_value()?);
                    } else {
                        let _: de::IgnoredAny = map.next_value()?;
                    }
                }
                match bytes_value {
                    Some(base64_str) => {
                        let decoded = BASE64_STANDARD
                            .decode(base64_str.as_bytes())
                            .map_err(|e| de::Error::custom(format!("Invalid base64: {}", e)))?;
                        Ok(Content::Bytes(Base64(Bytes::from(decoded))))
                    }
                    None => Err(de::Error::missing_field("bytes")),
                }
            }
        }

        deserializer.deserialize_any(ContentVisitor)
    }
}
