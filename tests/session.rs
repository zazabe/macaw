#![cfg(feature = "http")]

use anyhow::{Context, Result};
use macaw::http::HttpProxyConfig;
use macaw::session::{
    CreateSession, ProfileId, SessionConfig, SessionError, SessionErrorCode, SessionManager,
    SessionMode, SessionState,
};
use std::collections::BTreeMap;
use std::net::TcpListener;

fn recorder_config(
    root: &std::path::Path,
    proxy_name: &str,
    target: std::net::SocketAddr,
) -> SessionConfig {
    SessionConfig {
        root: root.to_path_buf(),
        proxies: BTreeMap::from([(
            proxy_name.to_string(),
            Box::new(HttpProxyConfig::new(
                "127.0.0.1:0",
                format!("http://{target}"),
            )) as Box<dyn macaw::core::ProxyConfig>,
        )]),
        debug_tx: None,
    }
}

async fn upstream() -> Result<std::net::SocketAddr> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    tokio::spawn(async move {
        let mut connections = Vec::new();
        while let Ok((connection, _)) = listener.accept().await {
            connections.push(connection);
        }
    });
    Ok(address)
}

#[test]
fn protocol_config_deserializes_through_trait_object() -> Result<()> {
    let config: SessionConfig = toml::from_str(
        r#"
        [proxies.api]
        type = "http"
        target = "https://example.test"
        "#,
    )?;
    let proxy = config.proxies.get("api").context("API proxy missing")?;

    assert_eq!(proxy.bind(), "127.0.0.1:0");
    assert_eq!(proxy.target(), Some("https://example.test"));
    Ok(())
}

#[tokio::test]
async fn recorder_sessions_stop_independently_and_release_ports() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let target = upstream().await?;
    let manager = SessionManager::start();
    let first = manager
        .record(
            "first.json",
            recorder_config(directory.path(), "first", target),
        )
        .await?;
    let second = manager
        .record(
            "second.json",
            recorder_config(directory.path(), "second", target),
        )
        .await?;

    let first_endpoint = first
        .endpoints
        .get("first")
        .context("first proxy endpoint missing")?
        .address;
    let stopped = manager.stop_session(first.id).await?;
    assert_eq!(stopped.state, SessionState::Stopped);
    assert!(directory.path().join("first.json").exists());
    TcpListener::bind(first_endpoint).context("stopped session must release its listener")?;

    let second_status = manager.get(second.id).await?;
    assert_eq!(second_status.state, SessionState::Running);
    let second_endpoint = second
        .endpoints
        .get("second")
        .context("second proxy endpoint missing")?
        .address;
    assert!(TcpListener::bind(second_endpoint).is_err());

    manager.stop_session(second.id).await?;
    assert!(directory.path().join("second.json").exists());
    manager.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn concurrent_stop_requests_share_one_outcome() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let target = upstream().await?;
    let manager = SessionManager::start();
    let session = manager
        .record(
            "recording.json",
            recorder_config(directory.path(), "proxy", target),
        )
        .await?;

    let first = {
        let manager = manager.clone();
        tokio::spawn(async move { manager.stop_session(session.id).await })
    };
    let second = {
        let manager = manager.clone();
        tokio::spawn(async move { manager.stop_session(session.id).await })
    };
    let (first, second) = tokio::join!(first, second);
    assert_eq!(first??, second??);

    manager.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn natural_replay_completion_does_not_stop_recorder() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let target = upstream().await?;
    let manager = SessionManager::start();

    let seed = manager
        .record(
            "seed.json",
            recorder_config(directory.path(), "seed", target),
        )
        .await?;
    manager.stop_session(seed.id).await?;

    let recorder = manager
        .record(
            "concurrent.json",
            recorder_config(directory.path(), "recorder", target),
        )
        .await?;
    let replay = manager
        .replay(
            "seed.json",
            recorder_config(directory.path(), "replayer", target),
        )
        .await?;

    let replay_status = tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            let status = manager.get(replay.id).await?;
            if status.state.is_terminal() {
                break Ok::<_, SessionError>(status);
            }
            tokio::task::yield_now().await;
        }
    })
    .await??;
    assert_eq!(replay_status.state, SessionState::Stopped);
    assert_eq!(manager.get(recorder.id).await?.state, SessionState::Running);

    manager.stop_session(recorder.id).await?;
    manager.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn manager_shutdown_waits_for_recording_flush() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let target = upstream().await?;
    let manager = SessionManager::start();
    let session = manager
        .record(
            "shutdown.json",
            recorder_config(directory.path(), "proxy", target),
        )
        .await?;
    let endpoint = session
        .endpoints
        .get("proxy")
        .context("proxy endpoint missing")?
        .address;

    manager.shutdown().await?;

    assert!(directory.path().join("shutdown.json").exists());
    TcpListener::bind(endpoint).context("manager completion must include listener shutdown")?;
    Ok(())
}

#[tokio::test]
async fn recording_write_failure_is_reported_as_session_failure() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let target = upstream().await?;
    let manager = SessionManager::start();
    let session = manager
        .record(
            // Saving to a directory is guaranteed to fail on supported platforms.
            ".",
            recorder_config(directory.path(), "proxy", target),
        )
        .await?;

    let stopped = manager.stop_session(session.id).await?;
    assert_eq!(stopped.state, SessionState::Failed);
    assert!(stopped.outcome.is_none());
    assert!(stopped.error.is_some());

    manager.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn profile_supports_concurrent_sessions_and_independent_deletion() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let target = upstream().await?;
    let manager = SessionManager::start();
    let profile_id = ProfileId::new("shared").unwrap();
    manager
        .create_profile(
            profile_id.clone(),
            recorder_config(directory.path(), "api", target),
        )
        .await?;

    let first = manager
        .create(CreateSession::new(
            profile_id.clone(),
            SessionMode::Record {
                output: "first.json".into(),
            },
        ))
        .await?;
    let first = manager.start_session(first.id).await?;
    let second = manager
        .create(CreateSession::new(
            profile_id.clone(),
            SessionMode::Record {
                output: "second.json".into(),
            },
        ))
        .await?;
    let second = manager.start_session(second.id).await?;
    assert_eq!(first.profile_id, profile_id);
    assert_eq!(second.profile_id, profile_id);
    assert_ne!(
        first.endpoints.get("api").unwrap().address,
        second.endpoints.get("api").unwrap().address
    );
    assert_eq!(manager.list_by_profile(profile_id.clone()).await?.len(), 2);

    manager.remove_profile(profile_id.clone()).await?;
    assert_eq!(manager.get(first.id).await?.state, SessionState::Running);
    let error = manager
        .create(CreateSession::new(
            profile_id,
            SessionMode::Replay {
                recording: "missing.json".into(),
            },
        ))
        .await
        .unwrap_err();
    assert_eq!(error.code, SessionErrorCode::NotFound);

    manager.stop_session(first.id).await?;
    manager.stop_session(second.id).await?;
    manager.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn profile_validation_defers_mode_specific_requirements_to_session_creation() -> Result<()> {
    let manager = SessionManager::start();
    let profile_id = ProfileId::new("replay-only").unwrap();
    let config = SessionConfig {
        root: Default::default(),
        proxies: BTreeMap::from([(
            "api".to_owned(),
            Box::new(HttpProxyConfig::replay("127.0.0.1:0")) as Box<dyn macaw::core::ProxyConfig>,
        )]),
        debug_tx: None,
    };
    manager.create_profile(profile_id.clone(), config).await?;

    let error = manager
        .create(CreateSession::new(
            profile_id,
            SessionMode::Record {
                output: "recording.json".into(),
            },
        ))
        .await
        .unwrap_err();
    assert_eq!(error.code, SessionErrorCode::InvalidConfig);
    assert!(manager.list().await?.is_empty());

    manager.shutdown().await?;
    Ok(())
}
