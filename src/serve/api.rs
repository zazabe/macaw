use super::model::{
    API_VERSION, CreateProfileRequest, CreateSessionRequest, ErrorDetail, ErrorResponse,
    HealthResponse, ModelError, ProfileResponse, SessionResponse, TrafficStreamEvent,
};
use axum::extract::rejection::JsonRejection;
use axum::extract::{DefaultBodyLimit, Path, State};
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use macaw::session::{ProfileId, SessionError, SessionErrorCode, SessionId, SessionManagerHandle};
use std::time::Duration;
use std::{convert::Infallible, time};

const JSON_BODY_LIMIT: usize = 1024 * 1024;
const QUERY_TIMEOUT: Duration = Duration::from_secs(5);
const LIFECYCLE_TIMEOUT: Duration = Duration::from_secs(30);

pub fn router(manager: SessionManagerHandle) -> Router {
    Router::new()
        .route("/v1/health", get(health))
        .route("/v1/profiles", post(create_profile).get(list_profiles))
        .route("/v1/profiles/{id}", get(get_profile).delete(remove_profile))
        .route(
            "/v1/profiles/{id}/sessions",
            post(create_session).get(list_profile_sessions),
        )
        .route("/v1/sessions", get(list_sessions))
        .route("/v1/sessions/{id}", get(get_session).delete(remove_session))
        .route("/v1/sessions/{id}/stop", post(stop_session))
        .route("/v1/sessions/{id}/events", get(watch_session))
        .fallback(not_found)
        .layer(DefaultBodyLimit::max(JSON_BODY_LIMIT))
        .with_state(manager)
}

async fn health(
    State(manager): State<SessionManagerHandle>,
) -> Result<Json<HealthResponse>, ApiError> {
    let ready = with_timeout(QUERY_TIMEOUT, manager.is_ready()).await?;
    Ok(Json(HealthResponse {
        api_version: API_VERSION,
        package_version: env!("CARGO_PKG_VERSION"),
        ready,
    }))
}

async fn create_session(
    State(manager): State<SessionManagerHandle>,
    Path(profile_id): Path<String>,
    request: Result<Json<CreateSessionRequest>, JsonRejection>,
) -> Result<Response, ApiError> {
    let profile_id = parse_profile_id(&profile_id)?;
    let Json(request) = request.map_err(ApiError::json)?;
    let request = request
        .into_actor_request(profile_id)
        .map_err(ApiError::model)?;
    let snapshot = with_timeout(LIFECYCLE_TIMEOUT, manager.create(request)).await?;
    let location = format!("/v1/sessions/{}", snapshot.id);
    let mut response = (StatusCode::CREATED, Json(SessionResponse::from(snapshot))).into_response();
    response.headers_mut().insert(
        header::LOCATION,
        HeaderValue::from_str(&location).map_err(|_| ApiError::internal())?,
    );
    Ok(response)
}

async fn create_profile(
    State(manager): State<SessionManagerHandle>,
    request: Result<Json<CreateProfileRequest>, JsonRejection>,
) -> Result<Response, ApiError> {
    let Json(request) = request.map_err(ApiError::json)?;
    let (id, config) = request.into_profile().map_err(ApiError::model)?;
    let profile = with_timeout(QUERY_TIMEOUT, manager.create_profile(id, config)).await?;
    let location = format!("/v1/profiles/{}", profile.id);
    let mut response = (StatusCode::CREATED, Json(ProfileResponse::from(profile))).into_response();
    response.headers_mut().insert(
        header::LOCATION,
        HeaderValue::from_str(&location).map_err(|_| ApiError::internal())?,
    );
    Ok(response)
}

async fn list_profiles(
    State(manager): State<SessionManagerHandle>,
) -> Result<Json<Vec<ProfileResponse>>, ApiError> {
    let profiles = with_timeout(QUERY_TIMEOUT, manager.list_profiles()).await?;
    Ok(Json(
        profiles.into_iter().map(ProfileResponse::from).collect(),
    ))
}

async fn get_profile(
    State(manager): State<SessionManagerHandle>,
    Path(id): Path<String>,
) -> Result<Json<ProfileResponse>, ApiError> {
    let profile = with_timeout(QUERY_TIMEOUT, manager.get_profile(parse_profile_id(&id)?)).await?;
    Ok(Json(ProfileResponse::from(profile)))
}

async fn remove_profile(
    State(manager): State<SessionManagerHandle>,
    Path(id): Path<String>,
) -> Result<StatusCode, ApiError> {
    with_timeout(
        QUERY_TIMEOUT,
        manager.remove_profile(parse_profile_id(&id)?),
    )
    .await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn list_profile_sessions(
    State(manager): State<SessionManagerHandle>,
    Path(id): Path<String>,
) -> Result<Json<Vec<SessionResponse>>, ApiError> {
    let snapshots = with_timeout(
        QUERY_TIMEOUT,
        manager.list_by_profile(parse_profile_id(&id)?),
    )
    .await?;
    Ok(Json(
        snapshots.into_iter().map(SessionResponse::from).collect(),
    ))
}

async fn list_sessions(
    State(manager): State<SessionManagerHandle>,
) -> Result<Json<Vec<SessionResponse>>, ApiError> {
    let snapshots = with_timeout(QUERY_TIMEOUT, manager.list()).await?;
    Ok(Json(
        snapshots.into_iter().map(SessionResponse::from).collect(),
    ))
}

async fn get_session(
    State(manager): State<SessionManagerHandle>,
    Path(id): Path<String>,
) -> Result<Json<SessionResponse>, ApiError> {
    let id = parse_id(&id)?;
    let snapshot = with_timeout(QUERY_TIMEOUT, manager.get(id)).await?;
    Ok(Json(SessionResponse::from(snapshot)))
}

async fn stop_session(
    State(manager): State<SessionManagerHandle>,
    Path(id): Path<String>,
) -> Result<Json<SessionResponse>, ApiError> {
    let id = parse_id(&id)?;
    let snapshot = with_timeout(LIFECYCLE_TIMEOUT, manager.stop_session(id)).await?;
    Ok(Json(SessionResponse::from(snapshot)))
}

async fn watch_session(
    State(manager): State<SessionManagerHandle>,
    Path(id): Path<String>,
) -> Result<Sse<impl futures::Stream<Item = Result<Event, Infallible>>>, ApiError> {
    let id = parse_id(&id)?;
    let mut receiver = with_timeout(QUERY_TIMEOUT, manager.subscribe_traffic(id)).await?;
    let stream = async_stream::stream! {
        loop {
            let payload = match receiver.recv().await {
                Ok(event) => TrafficStreamEvent::from(event),
                Err(tokio::sync::broadcast::error::RecvError::Lagged(count)) => {
                    TrafficStreamEvent::DroppedEvents { count }
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
            };
            let data = serde_json::to_string(&payload)
                .expect("traffic stream event must serialize");
            yield Ok(Event::default().event("traffic").data(data));
        }
    };
    Ok(Sse::new(stream).keep_alive(
        KeepAlive::new()
            .interval(time::Duration::from_secs(15))
            .text("keep-alive"),
    ))
}

async fn remove_session(
    State(manager): State<SessionManagerHandle>,
    Path(id): Path<String>,
) -> Result<StatusCode, ApiError> {
    let id = parse_id(&id)?;
    with_timeout(QUERY_TIMEOUT, manager.remove(id)).await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn not_found() -> ApiError {
    ApiError::new(
        StatusCode::NOT_FOUND,
        SessionErrorCode::NotFound,
        "endpoint not found",
    )
}

fn parse_id(value: &str) -> Result<SessionId, ApiError> {
    value.parse().map_err(|_| {
        ApiError::new(
            StatusCode::BAD_REQUEST,
            SessionErrorCode::InvalidConfig,
            "invalid session id",
        )
    })
}

fn parse_profile_id(value: &str) -> Result<ProfileId, ApiError> {
    value.parse().map_err(|message| {
        ApiError::new(
            StatusCode::BAD_REQUEST,
            SessionErrorCode::InvalidConfig,
            message,
        )
    })
}

async fn with_timeout<T>(
    duration: Duration,
    operation: impl Future<Output = Result<T, SessionError>>,
) -> Result<T, ApiError> {
    tokio::time::timeout(duration, operation)
        .await
        .map_err(|_| {
            ApiError::new(
                StatusCode::SERVICE_UNAVAILABLE,
                SessionErrorCode::ActorUnavailable,
                "session manager request timed out",
            )
        })?
        .map_err(ApiError::session)
}

#[derive(Debug)]
pub struct ApiError {
    status: StatusCode,
    detail: ErrorDetail,
}

impl ApiError {
    fn new(status: StatusCode, code: SessionErrorCode, message: impl Into<String>) -> Self {
        Self {
            status,
            detail: ErrorDetail {
                code,
                message: message.into(),
            },
        }
    }

    fn session(error: SessionError) -> Self {
        let status = match error.code {
            SessionErrorCode::InvalidConfig => StatusCode::BAD_REQUEST,
            SessionErrorCode::NotFound => StatusCode::NOT_FOUND,
            SessionErrorCode::Duplicate | SessionErrorCode::NotTerminal => StatusCode::CONFLICT,
            SessionErrorCode::Unsupported => StatusCode::UNPROCESSABLE_ENTITY,
            SessionErrorCode::ShuttingDown | SessionErrorCode::ActorUnavailable => {
                StatusCode::SERVICE_UNAVAILABLE
            }
            SessionErrorCode::StartupFailed | SessionErrorCode::RuntimeFailed => {
                StatusCode::INTERNAL_SERVER_ERROR
            }
        };
        Self {
            status,
            detail: ErrorDetail::from_session(&error),
        }
    }

    fn model(error: ModelError) -> Self {
        match error {
            ModelError::Invalid(message) => Self::new(
                StatusCode::BAD_REQUEST,
                SessionErrorCode::InvalidConfig,
                message,
            ),
            ModelError::Unsupported(message) => Self::new(
                StatusCode::UNPROCESSABLE_ENTITY,
                SessionErrorCode::Unsupported,
                message,
            ),
        }
    }

    fn json(rejection: JsonRejection) -> Self {
        if rejection.status() == StatusCode::PAYLOAD_TOO_LARGE {
            Self::new(
                StatusCode::PAYLOAD_TOO_LARGE,
                SessionErrorCode::InvalidConfig,
                "request body exceeds the configured limit",
            )
        } else {
            Self::new(
                StatusCode::BAD_REQUEST,
                SessionErrorCode::InvalidConfig,
                "malformed or invalid JSON request",
            )
        }
    }

    fn internal() -> Self {
        Self::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            SessionErrorCode::ActorUnavailable,
            "internal server error",
        )
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            self.status,
            [(header::CONTENT_TYPE, "application/json")],
            Json(ErrorResponse { error: self.detail }),
        )
            .into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use http_body_util::BodyExt;
    use macaw::session::SessionManager;
    use serde_json::{Value, json};
    use tower::ServiceExt;

    async fn response_json(response: Response) -> Value {
        let bytes = response
            .into_body()
            .collect()
            .await
            .expect("response body")
            .to_bytes();
        serde_json::from_slice(&bytes).expect("JSON response")
    }

    fn json_request(method: &str, uri: &str, value: Value) -> Request<Body> {
        Request::builder()
            .method(method)
            .uri(uri)
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(value.to_string()))
            .expect("request")
    }

    #[tokio::test]
    async fn health_reports_version_and_readiness() {
        let manager = SessionManager::start();
        let response = router(manager.clone())
            .oneshot(
                Request::builder()
                    .uri("/v1/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = response_json(response).await;
        assert_eq!(body["api_version"], "v1");
        assert_eq!(body["package_version"], env!("CARGO_PKG_VERSION"));
        assert_eq!(body["ready"], true);
        manager.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn sessions_require_an_existing_profile() {
        let manager = SessionManager::start();
        let app = router(manager.clone());
        let request = json!({"mode": {"type": "replay", "recording": "recording.json"}});

        let response = app
            .clone()
            .oneshot(json_request("POST", "/v1/sessions", request.clone()))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);

        let response = app
            .oneshot(json_request(
                "POST",
                "/v1/profiles/missing/sessions",
                request,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        manager.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn shutdown_state_rejects_new_sessions() {
        let manager = SessionManager::start();
        manager.begin_shutdown().await.unwrap();
        let app = router(manager.clone());

        let health = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v1/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response_json(health).await["ready"], false);

        let response = app
            .oneshot(json_request(
                "POST",
                "/v1/profiles",
                json!({
                    "id": "late",
                    "proxies": {}
                }),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        manager.shutdown().await.unwrap();
    }

    #[cfg(feature = "http")]
    #[tokio::test]
    async fn session_lifecycle_uses_structured_http_contract() {
        let directory = tempfile::tempdir().unwrap();
        let manager = SessionManager::start();
        let app = router(manager.clone());
        let profile = json!({
            "id": "example",
            "config_root": directory.path(),
            "proxies": {
                "api": {
                    "type": "http",
                    "config": {
                        "bind": "127.0.0.1:0",
                        "target": "https://example.com"
                    }
                }
            }
        });
        let response = app
            .clone()
            .oneshot(json_request("POST", "/v1/profiles", profile.clone()))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);
        assert_eq!(response.headers()[header::LOCATION], "/v1/profiles/example");
        let profile_body = response_json(response).await;
        assert_eq!(profile_body["id"], "example");
        assert_eq!(
            profile_body["config_root"],
            directory.path().to_str().unwrap()
        );
        assert_eq!(profile_body["proxies"]["api"]["protocol"], "http");
        assert_eq!(profile_body["proxies"]["api"]["bind"], "127.0.0.1:0");
        assert_eq!(
            profile_body["proxies"]["api"]["target"],
            "https://example.com"
        );

        let response = app
            .clone()
            .oneshot(json_request("POST", "/v1/profiles", profile))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CONFLICT);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v1/profiles")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let profiles = response_json(response).await;
        assert_eq!(profiles.as_array().unwrap().len(), 1);
        assert_eq!(profiles[0]["id"], "example");

        let response = app
            .clone()
            .oneshot(json_request(
                "POST",
                "/v1/profiles/example/sessions",
                json!({
                    "name": "integration-test",
                    "mode": {"type": "record", "output": "api.json"}
                }),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);
        let location = response.headers()[header::LOCATION]
            .to_str()
            .unwrap()
            .to_owned();
        let created = response_json(response).await;
        assert_eq!(created["name"], "integration-test");
        assert_eq!(created["profile_id"], "example");
        assert_eq!(created["state"], "running");
        assert_eq!(created["proxies"]["api"]["protocol"], "http");
        assert!(
            created["proxies"]["api"]["url"]
                .as_str()
                .unwrap()
                .starts_with("http://127.0.0.1:")
        );

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(&location)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri(&location)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CONFLICT);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v1/profiles/example/sessions")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let sessions = response_json(response).await;
        assert_eq!(sessions.as_array().unwrap().len(), 1);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/v1/profiles/example")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NO_CONTENT);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("{location}/stop"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response_json(response).await["state"], "stopped");
        assert!(directory.path().join("api.json").exists());

        let response = app
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri(&location)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        manager.shutdown().await.unwrap();
    }

    #[cfg(feature = "http")]
    #[tokio::test]
    async fn session_events_stream_as_sse() {
        let target_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let target_address = target_listener.local_addr().unwrap();
        let target = tokio::spawn(async move {
            axum::serve(
                target_listener,
                Router::new().route("/{*path}", get(|| async { "ok" })),
            )
            .await
        });

        let directory = tempfile::tempdir().unwrap();
        let manager = SessionManager::start();
        let app = router(manager.clone());
        let response = app
            .clone()
            .oneshot(json_request(
                "POST",
                "/v1/profiles",
                json!({
                    "id": "events",
                    "config_root": directory.path(),
                    "proxies": {
                        "api": {
                            "type": "http",
                            "config": {
                                "bind": "127.0.0.1:0",
                                "target": format!("http://{target_address}")
                            }
                        }
                    }
                }),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);

        let response = app
            .clone()
            .oneshot(json_request(
                "POST",
                "/v1/profiles/events/sessions",
                json!({"mode": {"type": "record", "output": "events.json"}}),
            ))
            .await
            .unwrap();
        let created = response_json(response).await;
        let id = created["id"].as_str().unwrap();
        let proxy_url = created["proxies"]["api"]["url"].as_str().unwrap();

        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/sessions/{id}/events"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert!(
            response.headers()[header::CONTENT_TYPE]
                .to_str()
                .unwrap()
                .starts_with("text/event-stream")
        );
        let mut body = response.into_body();

        let proxied = reqwest::get(format!("{proxy_url}/hello")).await.unwrap();
        assert_eq!(proxied.status(), StatusCode::OK);
        let frame = tokio::time::timeout(Duration::from_secs(2), body.frame())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let data = frame.into_data().unwrap();
        let data = String::from_utf8(data.to_vec()).unwrap();
        assert!(data.contains("\"type\":\"traffic\""));
        assert!(data.contains("\"proxy\":\"api\""));
        assert!(data.contains("\"timestamp\":"));
        assert!(data.contains("\"HttpRequest\":"));
        assert!(data.contains("\"GET\""));
        assert!(data.contains("\"headers\":{"));

        manager.stop_session(id.parse().unwrap()).await.unwrap();
        manager.shutdown().await.unwrap();
        target.abort();
    }

    #[tokio::test]
    async fn rejects_unknown_fields_and_oversized_bodies() {
        let manager = SessionManager::start();
        let app = router(manager.clone());
        let response = app
            .clone()
            .oneshot(json_request(
                "POST",
                "/v1/profiles",
                json!({
                    "id": "invalid",
                    "proxies": {},
                    "secret": "must not be accepted"
                }),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert!(!response_json(response).await.to_string().contains("secret"));

        let oversized = format!("{{\"padding\":\"{}\"}}", "x".repeat(JSON_BODY_LIMIT + 1));
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/profiles")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(oversized))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
        manager.shutdown().await.unwrap();
    }

    #[cfg(not(feature = "wasm"))]
    #[tokio::test]
    async fn disabled_wasm_plugin_is_unsupported_without_echoing_config() {
        let manager = SessionManager::start();
        let response = router(manager.clone())
            .oneshot(json_request(
                "POST",
                "/v1/profiles",
                json!({
                    "id": "wasm",
                    "proxies": {
                        "api": {
                            "type": "wasm_http",
                            "config": {
                                "target": "https://example.com",
                                "component": "plugin.wasm",
                                "config": {"password": "do-not-echo"}
                            }
                        }
                    }
                }),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
        assert!(
            !response_json(response)
                .await
                .to_string()
                .contains("do-not-echo")
        );
        manager.shutdown().await.unwrap();
    }
}
