use crate::lib::*;

#[derive(Serialize, Deserialize, PartialEq, Default, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct WsMessageMatcher {
    message: RegexMatcher,
}

impl WsMessageMatcher {
    pub(crate) fn is_match(&self, message: &WsMessage) -> bool {
        match message {
            WsMessage::Text(text) => self.message.is_match(text),
            WsMessage::Binary(_) => false,
            WsMessage::Ping(_) => false,
            WsMessage::Pong(_) => false,
            WsMessage::Close(_) => false,
        }
    }
}
