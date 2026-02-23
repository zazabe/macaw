use macaw_http::prelude::*;
use serde_json::json;

#[test]
fn test_match_method() {
    let rules = rules_from_json(json!([
        {"HttpRequest": {"match": {"method": "POST"}, "action": {"body": "matched_by_method"}}}
    ]));

    let req = request_event("POST", "/api", "original", []);
    let out = rules.http_override_request(req);
    assert_eq!(out.body.to_text().unwrap(), "matched_by_method");

    let req = request_event("GET", "/api", "original", []);
    let out = rules.http_override_request(req);
    assert_eq!(out.body.to_text().unwrap(), "original");
}

#[test]
fn test_match_path() {
    let rules = rules_from_json(json!([
        {"HttpRequest": {"match": {"path": "/api/.*"}, "action": {"body": "matched_by_path"}}}
    ]));

    let req = request_event("GET", "/api/users", "original", []);
    let out = rules.http_override_request(req);
    assert_eq!(out.body.to_text().unwrap(), "matched_by_path");

    let req = request_event("GET", "/other", "original", []);
    let out = rules.http_override_request(req);
    assert_eq!(out.body.to_text().unwrap(), "original");
}

#[test]
fn test_match_body() {
    let rules = rules_from_json(json!([
        {"HttpRequest": {"match": {"body": "secret.*"}, "action": {"body": "REDACTED"}}}
    ]));

    let req = request_event("POST", "/", "secret_data_here", []);
    let out = rules.http_override_request(req);
    assert_eq!(out.body.to_text().unwrap(), "REDACTED");

    let req = request_event("POST", "/", "public_data", []);
    let out = rules.http_override_request(req);
    assert_eq!(out.body.to_text().unwrap(), "public_data");
}

#[test]
fn test_match_headers() {
    let rules = rules_from_json(json!([
        {"HttpRequest": {"match": {"headers": {"x-api-key": ".*"}}, "action": {"body": "matched_by_header"}}}
    ]));

    let req = request_event("GET", "/", "", [("x-api-key".into(), "sk-12345".into())]);
    let out = rules.http_override_request(req);
    assert_eq!(out.body.to_text().unwrap(), "matched_by_header");

    let req = request_event("GET", "/", "", []);
    let out = rules.http_override_request(req);
    assert_eq!(out.body.to_text().unwrap(), "");
}

#[test]
fn test_match_response_status_and_body() {
    let rules = rules_from_json(json!([
        {"HttpResponse": {"match": {"status": "5..", "body": "error"}, "action": {"body": "OVERRIDDEN_ERROR"}}}
    ]));

    let req = request_event("GET", "/", "", []);
    let res = response_event(
        "00000000-0000-0000-0000-000000000001",
        500,
        "internal error",
        [],
    );
    let out = rules.http_override_response(res, req);
    assert_eq!(out.body.to_text().unwrap(), "OVERRIDDEN_ERROR");

    let req = request_event("GET", "/", "", []);
    let res = response_event("00000000-0000-0000-0000-000000000001", 200, "ok", []);
    let out = rules.http_override_response(res, req);
    assert_eq!(out.body.to_text().unwrap(), "ok");
}

#[test]
fn test_match_response_on_request() {
    let rules = rules_from_json(json!([
        {"HttpResponse": {
            "match": {"request": {"path": "/api/users", "method": "POST"}},
            "action": {"body": {"search": "user_id=(\\d+)", "replace": "user_id=REDACTED"}}
        }}
    ]));

    let req = request_event("POST", "/api/users", "create", []);
    let res = response_event(
        "00000000-0000-0000-0000-000000000001",
        200,
        "created user_id=12345",
        [],
    );
    let out = rules.http_override_response(res, req);
    assert_eq!(out.body.to_text().unwrap(), "created user_id=REDACTED");

    let req = request_event("GET", "/api/users", "", []);
    let res = response_event(
        "00000000-0000-0000-0000-000000000001",
        200,
        "user_id=999",
        [],
    );
    let out = rules.http_override_response(res, req);
    assert_eq!(out.body.to_text().unwrap(), "user_id=999");
}

#[test]
fn test_action_regex_replace_body() {
    let rules = rules_from_json(json!([
        {"HttpRequest": {
            "match": {"body": "hello_(.+)"},
            "action": {"body": {"search": "hello_(.+)", "replace": "hi_$1"}}
        }}
    ]));

    let req = request_event("POST", "/", "hello_world", []);
    let out = rules.http_override_request(req);
    assert_eq!(out.body.to_text().unwrap(), "hi_world");
}

#[test]
fn test_action_regex_replace_body_with_captures() {
    let rules = rules_from_json(json!([
        {"HttpRequest": {
            "match": {"body": "id_(?P<id>\\d+)"},
            "action": {"body": {"search": "id_(?P<id>\\d+)", "replace": {"id": "XXX"}}}
        }}
    ]));

    let req = request_event("POST", "/", "id_12345", []);
    let out = rules.http_override_request(req);
    assert_eq!(out.body.to_text().unwrap(), "id_XXX");
}

#[test]
fn test_action_replace_headers() {
    let rules = rules_from_json(json!([
        {"HttpRequest": {
            "match": {"body": ".*"},
            "action": {"headers": {"x-custom": "custom_value"}}
        }}
    ]));

    let req = request_event("POST", "/", "body", [("x-custom".into(), "old".into())]);
    let out = rules.http_override_request(req);
    assert_eq!(
        out.headers.get("x-custom"),
        Some(&"custom_value".to_string())
    );
}

#[test]
fn test_action_regex_replace_header() {
    let rules = rules_from_json(json!([
        {"HttpRequest": {
            "match": {"headers": {"authorization": "Bearer .*"}},
            "action": {"headers": {"authorization": "Bearer REDACTED"}}
        }}
    ]));

    let req = request_event(
        "GET",
        "/",
        "",
        [("authorization".into(), "Bearer sk-abc123".into())],
    );
    let out = rules.http_override_request(req);
    assert_eq!(
        out.headers.get("authorization"),
        Some(&"Bearer REDACTED".to_string())
    );
}

#[test]
fn test_action_response_body_and_headers() {
    let rules = rules_from_json(json!([
        {"HttpResponse": {
            "match": {"body": "replace_me"},
            "action": {
                "body": {"search": "replace_me", "replace": "replaced"},
                "headers": {"content-type": "application/json"}
            }
        }}
    ]));

    let req = request_event("GET", "/", "", []);
    let res = response_event(
        "00000000-0000-0000-0000-000000000001",
        200,
        "replace_me",
        [("content-type".into(), "text/plain".into())],
    );
    let out = rules.http_override_response(res, req);
    assert_eq!(out.body.to_text().unwrap(), "replaced");
    assert_eq!(
        out.headers.get("content-type"),
        Some(&"application/json".to_string())
    );
}

// ---------------------------------------------------------------------------

fn request_event(
    method: &str,
    path: &str,
    body: &str,
    headers: impl IntoIterator<Item = (String, String)>,
) -> HttpRequestEvent {
    serde_json::from_value(json!({
        "request_id": "00000000-0000-0000-0000-000000000001",
        "method": method,
        "uri": path,
        "version": "HTTP/1.1",
        "headers": headers.into_iter().collect::<std::collections::BTreeMap<_, _>>(),
        "body": body
    }))
    .unwrap()
}

fn response_event(
    request_id: &str,
    status: u16,
    body: &str,
    headers: impl IntoIterator<Item = (String, String)>,
) -> HttpResponseEvent {
    serde_json::from_value(json!({
        "request_id": request_id,
        "status": status,
        "version": "HTTP/1.1",
        "headers": headers.into_iter().collect::<std::collections::BTreeMap<_, _>>(),
        "body": body
    }))
    .unwrap()
}

fn rules_from_json(value: serde_json::Value) -> HttpOverrideRules {
    serde_json::from_value(value).unwrap()
}
