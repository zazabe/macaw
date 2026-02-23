use crate::lib::*;

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct WsMessageRule {
    #[serde(default, rename = "match", skip_serializing_if = "is_default")]
    matcher: WsMessageMatcher,
    #[serde(default, skip_serializing_if = "is_default")]
    action: TransformOrIgnoreAction<WsMessageAction>,
    #[serde(default)]
    optional: bool,
}

impl Rule for WsMessageRule {
    type Message = WsMessage;
    type MessageContext = ();

    fn apply(
        &self,
        message: Self::Message,
        _context: &Self::MessageContext,
    ) -> RuleResult<Self::Message> {
        if self.matcher.is_match(&message) {
            self.action.apply(message)
        } else {
            RuleResult::NoMatch(message)
        }
    }

    fn is_optional(&self) -> bool {
        self.optional
    }
}
