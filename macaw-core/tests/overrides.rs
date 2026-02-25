//! Tests for override rules: matchers, actions, parsing, and rule chain behavior.

use macaw_core::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::BTreeMap;

#[test]
fn test_override_rules_all_matchers_and_actions() {
    let rules: Vec<TestRule> = serde_json::from_value(json!([
        {
            "match": { "value": "^(redact|mask)_.*" },
            "action": {
                "value": "REDACTED",
                "headers": { "x-secret": "***" }
            }
        },
        {
            "match": { "value": "replace_me" },
            "action": { "value": "replaced" }
        },
        {
            "match": {
                "value": "search_(?P<part>\\w+)_here",
                "headers": { "content-type": "application/.*" }
            },
            "action": {
                "value": { "search": "search_(?P<part>\\w+)_here", "replace": { "part": "done" } },
                "headers": { "content-type": "text/plain" }
            }
        }
    ]))
    .unwrap();

    let chain: OverrideRulesChain<TestRule> = rules.into_iter().collect();

    // Event 1: RegexMatcher + Replace + MapAction
    let e1: TestEvent = serde_json::from_value(json!({
        "value": "redact_sensitive",
        "headers": { "x-secret": "password123" }
    }))
    .unwrap();
    let out1 = chain.apply_rules(e1, ());
    let msg1 = match &out1 {
        OverrideOutput::Message(m) => m,
        OverrideOutput::Suppress => panic!("expected message"),
    };
    insta::assert_json_snapshot!(msg1, @r#"
    {
      "value": "REDACTED",
      "headers": {
        "x-secret": "***"
      }
    }
    "#);

    // Event 2: Replace action
    let e2: TestEvent = serde_json::from_value(json!({
        "value": "replace_me",
        "headers": {}
    }))
    .unwrap();
    let out2 = chain.apply_rules(e2, ());
    let msg2 = match &out2 {
        OverrideOutput::Message(m) => m,
        OverrideOutput::Suppress => panic!("expected message"),
    };
    insta::assert_json_snapshot!(msg2, @r#"
    {
      "value": "replaced",
      "headers": {}
    }
    "#);

    // Event 3: RegexMatcher + MapMatcher + SearchAndReplace (captures) + Replace header
    let e3: TestEvent = serde_json::from_value(json!({
        "value": "search_foo_here",
        "headers": { "content-type": "application/json" }
    }))
    .unwrap();
    let out3 = chain.apply_rules(e3, ());
    let msg3 = match &out3 {
        OverrideOutput::Message(m) => m,
        OverrideOutput::Suppress => panic!("expected message"),
    };
    // SearchAndReplace with captures replaces only the capture range: "foo" -> "done"
    insta::assert_json_snapshot!(msg3, @r#"
    {
      "value": "search_done_here",
      "headers": {
        "content-type": "text/plain"
      }
    }
    "#);

    // Event 4: SearchAndReplace with Match (string replacement template)
    let rules_sr: Vec<TestRule> = serde_json::from_value(json!([{
        "match": {
            "value": "hello_world"
        },
        "action": {
            "value": {
                "search": "hello_(.+)", "replace": "hi_$1"
            }
        }
    }]))
    .unwrap();
    let chain_sr: OverrideRulesChain<TestRule> = rules_sr.into_iter().collect();
    let e4: TestEvent = serde_json::from_value(json!({
        "value": "hello_world",
        "headers": {}
    }))
    .unwrap();
    let out4 = chain_sr.apply_rules(e4, ());
    let msg4 = match &out4 {
        OverrideOutput::Message(m) => m,
        OverrideOutput::Suppress => panic!("expected message"),
    };
    insta::assert_json_snapshot!(msg4, @r#"
    {
      "value": "hi_world",
      "headers": {}
    }
    "#);

    // Event 5: SuppressMessage - chain returns OverrideOutput::Suppress
    let rules_suppress: Vec<TestRule> = serde_json::from_value(json!([{
        "match": { "value": "drop_me" },
        "action": {},
        "suppress": true
    }]))
    .unwrap();
    let chain_suppress: OverrideRulesChain<TestRule> = rules_suppress.into_iter().collect();
    let e5: TestEvent = serde_json::from_value(json!({
        "value": "drop_me",
        "headers": {}
    }))
    .unwrap();
    let out5 = chain_suppress.apply_rules(e5, ());
    assert!(matches!(out5, OverrideOutput::Suppress));
}

#[test]
fn test_unexpected_syntax() {
    // Invalid type (expect array of rules)
    let err1 = serde_json::from_value::<Vec<TestRule>>(json!(42)).unwrap_err();
    assert_eq!(
        err1.to_string(),
        "invalid type: integer `42`, expected a sequence"
    );

    // Unknown field in rule
    let err2 = serde_json::from_value::<Vec<TestRule>>(json!([{
        "match": {},
        "action": {},
        "unknown_field": 42
    }]))
    .unwrap_err();
    assert_eq!(
        err2.to_string(),
        "unknown field `unknown_field`, expected one of `match`, `action`, `optional`, `suppress`"
    );

    // Invalid regex in matcher
    let err3 = serde_json::from_value::<Vec<TestRule>>(json!([{
        "match": { "value": "[" },
        "action": {}
    }]))
    .unwrap_err();
    assert_eq!(
        err3.to_string(),
        "regex parse error:\n    [\n    ^\nerror: unclosed character class"
    );
}

// ##### Tests: search-and-replace errors

#[test]
fn test_search_and_replace_capture_errors() {
    // Missing capture key
    let err1 = serde_json::from_value::<FieldAction>(json!({
        "search": "(?P<foo>\\w+)-(?P<bar>\\d+)",
        "replace": { "foo": "x" }
    }))
    .unwrap_err();
    assert_eq!(
        err1.to_string(),
        "Replacement keys differ from regex capture names, missing: [\"bar\"], extra: []."
    );

    // Extra capture key
    let err2 = serde_json::from_value::<FieldAction>(json!({
        "search": "(?P<foo>\\w+)",
        "replace": { "foo": "x", "bar": "y" }
    }))
    .unwrap_err();
    assert_eq!(
        err2.to_string(),
        "Replacement keys differ from regex capture names, missing: [], extra: [\"bar\"]."
    );

    // Both missing and extra
    let err3 = serde_json::from_value::<FieldAction>(json!({
        "search": "(?P<a>.)(?P<b>.)",
        "replace": { "b": "2", "c": "3" }
    }))
    .unwrap_err();
    assert_eq!(
        err3.to_string(),
        "Replacement keys differ from regex capture names, missing: [\"a\"], extra: [\"c\"]."
    );
}

#[test]
fn test_match_multiple_events() {
    let rules: Vec<TestRule> = serde_json::from_value(json!([
        { "match": { "value": "transform" }, "action": { "value": "transformed" } },
        { "match": { "value": "chain" }, "action": { "value": "chained" } }
    ]))
    .unwrap();

    let chain: OverrideRulesChain<TestRule> = rules.into_iter().collect();

    let events: Vec<TestEvent> = serde_json::from_value(json!(
        [
            { "value": "transform", "headers": {} },
            { "value": "chain", "headers": {} },
            { "value": "no_match", "headers": {} }
        ]
    ))
    .unwrap();

    let results: Vec<_> = events
        .into_iter()
        .map(|e| chain.apply_rules(e, ()))
        .collect();

    let results_json: Vec<serde_json::Value> = results
        .iter()
        .map(|r| match r {
            OverrideOutput::Message(m) => serde_json::to_value(m).unwrap(),
            OverrideOutput::Suppress => json!("Suppress"),
        })
        .collect();
    insta::assert_json_snapshot!(results_json, @r#"
    [
      {
        "headers": {},
        "value": "transformed"
      },
      {
        "headers": {},
        "value": "chained"
      },
      {
        "headers": {},
        "value": "no_match"
      }
    ]
    "#);
}

#[test]
fn test_unmatched_rules_honors_optional_flag() {
    let rules: Vec<TestRule> = serde_json::from_value(json!([
        { "match": { "value": "will_match" }, "action": { "value": "ok" }, "optional": false },
        { "match": { "value": "never_matches" }, "action": { "value": "x" }, "optional": false },
        { "match": { "value": "optional_never" }, "action": { "value": "x" }, "optional": true }
    ]))
    .unwrap();

    let chain: OverrideRulesChain<TestRule> = rules.into_iter().collect();

    // Only one event that matches the first rule
    let event: TestEvent = serde_json::from_value(json!({
        "value": "will_match",
        "headers": {}
    }))
    .unwrap();
    let _ = chain.apply_rules(event, ());

    let unmatched = chain.get_unmatched_rules();

    // Rule 2 (never_matches) is required and didn't match -> in unmatched
    // Rule 3 (optional_never) is optional -> NOT in unmatched even though it didn't match
    insta::assert_json_snapshot!(unmatched, @r#"
    [
      {
        "match": {
          "value": "never_matches",
          "headers": {}
        },
        "action": {
          "value": "x",
          "headers": {}
        },
        "optional": false,
        "suppress": false
      }
    ]
    "#);
}

// ------------------------------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct TestEvent {
    value: String,
    headers: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
#[serde(deny_unknown_fields)]
struct TestMatcher {
    #[serde(default)]
    value: RegexMatcher,
    #[serde(default)]
    headers: MapMatcher,
}

impl TestMatcher {
    fn is_match(&self, event: &TestEvent) -> bool {
        self.value.is_match(&event.value) && self.headers.is_match(event.headers.iter())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
#[serde(deny_unknown_fields)]
struct TestAction {
    #[serde(default)]
    value: FieldAction,
    #[serde(default)]
    headers: MapAction,
}

impl TestAction {
    fn apply(&self, event: TestEvent) -> TestEvent {
        let new_value = self.value.transform(event.value.clone());
        let new_headers: BTreeMap<String, Option<String>> =
            self.headers.transform(event.headers.into_iter()).collect();
        serde_json::from_value(json!({ "value": new_value, "headers": new_headers })).unwrap()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
struct TestRule {
    #[serde(default, rename = "match")]
    matcher: TestMatcher,
    #[serde(default)]
    action: TestAction,
    #[serde(default)]
    optional: bool,
    /// When true and rule matches, returns SuppressMessage (OverrideOutput::Suppress).
    #[serde(default)]
    suppress: bool,
}

impl Rule for TestRule {
    type Message = TestEvent;
    type MessageContext = ();

    fn apply(
        &self,
        message: Self::Message,
        _context: &Self::MessageContext,
    ) -> RuleResult<Self::Message> {
        if self.matcher.is_match(&message) {
            if self.suppress {
                RuleResult::SuppressMessage
            } else {
                RuleResult::MessageTransformed(self.action.apply(message))
            }
        } else {
            RuleResult::NoMatch(message)
        }
    }

    fn is_optional(&self) -> bool {
        self.optional
    }
}
