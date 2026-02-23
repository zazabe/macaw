use crate::lib::*;

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct HttpRequestRule {
    #[serde(default, rename = "match", skip_serializing_if = "is_default")]
    matcher: HttpRequestMatcher,
    #[serde(default, skip_serializing_if = "is_default")]
    action: HttpRequestAction,
    #[serde(default)]
    optional: bool,
}

impl Rule for HttpRequestRule {
    type Message = HttpRequestEvent;
    type MessageContext = ();

    fn apply(
        &self,
        request: Self::Message,
        _context: &Self::MessageContext,
    ) -> RuleResult<Self::Message> {
        if self.matcher.is_match(&request) {
            RuleResult::MessageTransformed(self.action.apply(request))
        } else {
            RuleResult::NoMatch(request)
        }
    }

    fn is_optional(&self) -> bool {
        self.optional
    }
}

// ##### HttpResponse

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct HttpResponseRule {
    #[serde(default, rename = "match", skip_serializing_if = "is_default")]
    matcher: HttpResponseMatcher,
    #[serde(default, skip_serializing_if = "is_default")]
    action: HttpResponseAction,
    #[serde(default)]
    optional: bool,
}

impl Rule for HttpResponseRule {
    type Message = HttpResponseEvent;
    type MessageContext = HttpRequestEvent;

    fn apply(
        &self,
        response: Self::Message,
        context: &Self::MessageContext,
    ) -> RuleResult<Self::Message> {
        if self.matcher.is_match(&response, context) {
            RuleResult::MessageTransformed(self.action.apply(response))
        } else {
            RuleResult::NoMatch(response)
        }
    }

    fn is_optional(&self) -> bool {
        self.optional
    }
}

fn is_default<T: Default + PartialEq>(value: &T) -> bool {
    value == &T::default()
}
