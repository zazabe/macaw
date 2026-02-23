use assert_matches::assert_matches;
use bytes::Bytes;
use macaw_ws::prelude::*;
use serde_json::json;

#[test]
fn test_match_message_upstream() {
    let rules = rules_from_json(json!([
        {"WsUpstreamMessage": {"match": {"message": "secret.*"}, "action": {"message": "REDACTED"}}}
    ]));

    let event = WsEvent::message(WsMessage::Text("secret_data_here".into()));
    let out = rules.ws_upstream_override_event(event);
    let msg = out.expect("event should not be suppressed");
    assert_matches!(msg, WsEvent::Message(WsMessageEvent { message: WsMessage::Text(t) }) if t == "REDACTED");

    let event = WsEvent::message(WsMessage::Text("public_data".into()));
    let out = rules.ws_upstream_override_event(event);
    let msg = out.expect("event should not be suppressed");
    assert_matches!(msg, WsEvent::Message(WsMessageEvent { message: WsMessage::Text(t) }) if t == "public_data");
}

#[test]
fn test_match_message_downstream() {
    let rules = rules_from_json(json!([
        {"WsDownstreamMessage": {"match": {"message": "error.*"}, "action": {"message": "OVERRIDDEN_ERROR"}}}
    ]));

    let event = WsEvent::message(WsMessage::Text("error: connection failed".into()));
    let out = rules.ws_downstream_override_event(event);
    let msg = out.expect("event should not be suppressed");
    assert_matches!(msg, WsEvent::Message(WsMessageEvent { message: WsMessage::Text(t) }) if t == "OVERRIDDEN_ERROR");

    let event = WsEvent::message(WsMessage::Text("ok".into()));
    let out = rules.ws_downstream_override_event(event);
    let msg = out.expect("event should not be suppressed");
    assert_matches!(msg, WsEvent::Message(WsMessageEvent { message: WsMessage::Text(t) }) if t == "ok");
}

#[test]
fn test_upstream_and_downstream_rules_apply_separately() {
    let rules = rules_from_json(json!([
        {"WsUpstreamMessage": {"match": {"message": "up_.*"}, "action": {"message": "up_matched"}}},
        {"WsDownstreamMessage": {"match": {"message": "down_.*"}, "action": {"message": "down_matched"}}}
    ]));

    // Upstream rule applies to upstream messages only
    let event = WsEvent::message(WsMessage::Text("up_foo".into()));
    let msg = rules
        .ws_upstream_override_event(event)
        .expect("event should not be suppressed");
    assert_matches!(msg, WsEvent::Message(WsMessageEvent { message: WsMessage::Text(t) }) if t == "up_matched");

    // Downstream rule does not apply to upstream
    let event = WsEvent::message(WsMessage::Text("down_foo".into()));
    let msg = rules
        .ws_upstream_override_event(event)
        .expect("event should not be suppressed");
    assert_matches!(msg, WsEvent::Message(WsMessageEvent { message: WsMessage::Text(t) }) if t == "down_foo");

    // Downstream rule applies to downstream messages only
    let event = WsEvent::message(WsMessage::Text("down_bar".into()));
    let msg = rules
        .ws_downstream_override_event(event)
        .expect("event should not be suppressed");
    assert_matches!(msg, WsEvent::Message(WsMessageEvent { message: WsMessage::Text(t) }) if t == "down_matched");

    // Upstream rule does not apply to downstream
    let event = WsEvent::message(WsMessage::Text("up_bar".into()));
    let msg = rules
        .ws_downstream_override_event(event)
        .expect("event should not be suppressed");
    assert_matches!(msg, WsEvent::Message(WsMessageEvent { message: WsMessage::Text(t) }) if t == "up_bar");
}

#[test]
fn test_message_suppression() {
    let rules = rules_from_json(json!([
        {"WsUpstreamMessage": {"match": {"message": "drop_me"}, "action": "ignore"}},
        {"WsDownstreamMessage": {"match": {"message": "filter_.*"}, "action": "ignore"}}
    ]));

    // Matching upstream message is suppressed
    let event = WsEvent::message(WsMessage::Text("drop_me".into()));
    let out = rules.ws_upstream_override_event(event);
    assert_matches!(out, None);

    // Non-matching upstream message passes through
    let event = WsEvent::message(WsMessage::Text("keep_me".into()));
    let msg = rules
        .ws_upstream_override_event(event)
        .expect("event should not be suppressed");
    assert_matches!(msg, WsEvent::Message(WsMessageEvent { message: WsMessage::Text(t) }) if t == "keep_me");

    // Matching downstream message is suppressed
    let event = WsEvent::message(WsMessage::Text("filter_secret".into()));
    let out = rules.ws_downstream_override_event(event);
    assert_matches!(out, None);

    // Non-matching downstream message passes through
    let event = WsEvent::message(WsMessage::Text("ok".into()));
    let msg = rules
        .ws_downstream_override_event(event)
        .expect("event should not be suppressed");
    assert_matches!(msg, WsEvent::Message(WsMessageEvent { message: WsMessage::Text(t) }) if t == "ok");
}

#[test]
fn test_action_regex_replace_body() {
    let rules = rules_from_json(json!([
        {"WsUpstreamMessage": {
            "match": {"message": "hello_(.+)"},
            "action": {"message": {"search": "hello_(.+)", "replace": "hi_$1"}}
        }}
    ]));

    let event = WsEvent::message(WsMessage::Text("hello_world".into()));
    let msg = rules
        .ws_upstream_override_event(event)
        .expect("event should not be suppressed");
    assert_matches!(msg, WsEvent::Message(WsMessageEvent { message: WsMessage::Text(t) }) if t == "hi_world");
}

#[test]
fn test_action_regex_replace_body_with_captures() {
    let rules = rules_from_json(json!([
        {"WsDownstreamMessage": {
            "match": {"message": "id_(?P<id>\\d+)"},
            "action": {"message": {"search": "id_(?P<id>\\d+)", "replace": {"id": "XXX"}}}
        }}
    ]));

    let event = WsEvent::message(WsMessage::Text("id_12345".into()));
    let msg = rules
        .ws_downstream_override_event(event)
        .expect("event should not be suppressed");
    assert_matches!(msg, WsEvent::Message(WsMessageEvent { message: WsMessage::Text(t) }) if t == "id_XXX");
}

#[test]
fn test_unsupported_body_type_binary_passes_through() {
    let rules = rules_from_json(json!([
        {"WsUpstreamMessage": {"match": {"message": ".*"}, "action": {"message": "would_override"}}}
    ]));

    let binary_data = Bytes::from_static(b"\x00\x01\x02\xff");
    let event = WsEvent::message(WsMessage::Binary(binary_data.clone()));
    let msg = rules
        .ws_upstream_override_event(event)
        .expect("event should not be suppressed");
    assert_matches!(msg, WsEvent::Message(WsMessageEvent { message: WsMessage::Binary(b) }) if b == binary_data);
}

#[test]
fn test_unsupported_body_type_ping_passes_through() {
    let rules = rules_from_json(json!([
        {"WsUpstreamMessage": {"match": {"message": ".*"}, "action": {"message": "would_override"}}}
    ]));

    let ping_data = Bytes::from_static(b"ping");
    let event = WsEvent::message(WsMessage::Ping(ping_data.clone()));
    let msg = rules
        .ws_upstream_override_event(event)
        .expect("event should not be suppressed");
    assert_matches!(msg, WsEvent::Message(WsMessageEvent { message: WsMessage::Ping(p) }) if p == ping_data);
}

#[test]
fn test_unsupported_body_type_pong_passes_through() {
    let rules = rules_from_json(json!([
        {"WsDownstreamMessage": {"match": {"message": ".*"}, "action": {"message": "would_override"}}}
    ]));

    let pong_data = Bytes::from_static(b"pong");
    let event = WsEvent::message(WsMessage::Pong(pong_data.clone()));
    let msg = rules
        .ws_downstream_override_event(event)
        .expect("event should not be suppressed");
    assert_matches!(msg, WsEvent::Message(WsMessageEvent { message: WsMessage::Pong(p) }) if p == pong_data);
}

#[test]
fn test_unsupported_body_type_close_passes_through() {
    let rules = rules_from_json(json!([
        {"WsUpstreamMessage": {"match": {"message": ".*"}, "action": {"message": "would_override"}}}
    ]));

    let event = WsEvent::message(WsMessage::Close(Some((1000, "normal".into()))));
    let msg = rules
        .ws_upstream_override_event(event)
        .expect("event should not be suppressed");
    assert_matches!(msg, WsEvent::Message(WsMessageEvent { message: WsMessage::Close(c) }) if c == Some((1000, "normal".into())));
}

#[test]
fn test_non_message_events_pass_through() {
    let rules = rules_from_json(json!([
        {"WsUpstreamMessage": {"match": {"message": ".*"}, "action": {"message": "would_override"}}}
    ]));

    // Open and Disconnect events are not messages - they pass through
    let open_event: WsEvent = serde_json::from_value(json!({
        "Open": {
            "request": {
                "method": "GET",
                "uri": "http://example.com/",
                "version": "HTTP/1.1",
                "headers": {}
            }
        }
    }))
    .unwrap();
    let out = rules.ws_upstream_override_event(open_event);
    assert_matches!(out, Some(WsEvent::Open(_)));

    let disconnect_event: WsEvent = serde_json::from_value(json!({"Disconnect": null})).unwrap();
    let out = rules.ws_upstream_override_event(disconnect_event);
    assert_matches!(out, Some(WsEvent::Disconnect));
}

// ---------------------------------------------------------------------------

fn rules_from_json(value: serde_json::Value) -> WsOverrideRules {
    serde_json::from_value(value).unwrap()
}
