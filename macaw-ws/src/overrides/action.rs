use crate::lib::*;

#[derive(Serialize, Deserialize, PartialEq, Default, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct WsMessageAction {
    message: FieldAction,
}

impl TransformAction for WsMessageAction {
    type Message = WsMessage;

    fn apply(&self, message: Self::Message) -> Self::Message {
        if let WsMessage::Text(text) = message {
            return WsMessage::Text(self.message.transform(text).unwrap_or_default());
        }
        message
    }
}
