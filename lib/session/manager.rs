use super::{
    GetSessionStatus, SessionActor, SessionConfig, SessionError, SessionErrorCode, SessionId,
    SessionMode, SessionSnapshot, StopSession,
};
use macaw_core::prelude::*;
use std::collections::HashMap;
use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct CreateSession {
    pub id: Option<SessionId>,
    pub mode: SessionMode,
    pub config: SessionConfig,
}

impl CreateSession {
    pub fn new(mode: SessionMode, config: SessionConfig) -> Self {
        Self {
            id: None,
            mode,
            config,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct GetSession {
    pub id: SessionId,
}

#[derive(Debug)]
pub struct ListSessions;

#[derive(Debug, Clone, Copy)]
pub struct StopSessionById {
    pub id: SessionId,
}

#[derive(Debug, Clone, Copy)]
pub struct RemoveSession {
    pub id: SessionId,
}

#[derive(Debug)]
pub struct StopAllSessions;

#[derive(Debug)]
pub struct SessionManager {
    context: ActorContext,
    sessions: HashMap<SessionId, ActorHandle<SessionActor>>,
}

/// Typed façade over the session-manager actor.
///
/// Actor transport failures are mapped to [`SessionErrorCode::ActorUnavailable`],
/// so callers see one semantic error layer instead of a nested `Result`.
#[derive(Debug, Clone)]
pub struct SessionManagerHandle {
    actor: ActorHandle<SessionManager>,
}

impl SessionManager {
    pub fn start() -> SessionManagerHandle {
        let actor_system = ActorSystem::new();
        let actor = Self {
            context: actor_system.actor_context("session-manager"),
            sessions: HashMap::new(),
        }
        .run();
        SessionManagerHandle { actor }
    }

    async fn snapshot(handle: &ActorHandle<SessionActor>) -> Result<SessionSnapshot, SessionError> {
        handle
            .request(GetSessionStatus)
            .await
            .map_err(SessionError::actor)
    }

    async fn stop(handle: &ActorHandle<SessionActor>) -> Result<SessionSnapshot, SessionError> {
        handle
            .request(StopSession)
            .await
            .map_err(SessionError::actor)?
    }
}

impl SessionManagerHandle {
    async fn request<M, R>(&self, message: M) -> Result<R, SessionError>
    where
        SessionManager: ActorHandler<M, Reply = R>,
        M: ActorMessage,
        R: Send + 'static,
    {
        self.actor
            .request(message)
            .await
            .map_err(SessionError::actor)
    }

    pub async fn record(
        &self,
        output: impl Into<PathBuf>,
        config: SessionConfig,
    ) -> Result<SessionSnapshot, SessionError> {
        self.request(CreateSession::new(
            SessionMode::Record {
                output: output.into(),
            },
            config,
        ))
        .await?
    }

    pub async fn replay(
        &self,
        recording: impl Into<PathBuf>,
        config: SessionConfig,
    ) -> Result<SessionSnapshot, SessionError> {
        self.request(CreateSession::new(
            SessionMode::Replay {
                recording: recording.into(),
            },
            config,
        ))
        .await?
    }

    pub async fn get(&self, id: SessionId) -> Result<SessionSnapshot, SessionError> {
        self.request(GetSession { id }).await?
    }

    pub async fn list(&self) -> Result<Vec<SessionSnapshot>, SessionError> {
        self.request(ListSessions).await
    }

    pub async fn stop_session(&self, id: SessionId) -> Result<SessionSnapshot, SessionError> {
        self.request(StopSessionById { id }).await?
    }

    pub async fn remove(&self, id: SessionId) -> Result<(), SessionError> {
        self.request(RemoveSession { id }).await?
    }

    pub async fn stop_all(
        &self,
    ) -> Result<Vec<Result<SessionSnapshot, SessionError>>, SessionError> {
        self.request(StopAllSessions).await
    }

    /// Gracefully stop all sessions and await manager completion.
    pub async fn shutdown(&self) -> Result<(), SessionError> {
        self.actor.stop();
        self.actor.wait().await.map_err(SessionError::actor)
    }
}

impl Actor for SessionManager {
    fn context(&self) -> &ActorContext {
        &self.context
    }

    async fn on_stop(&mut self, _reason: ActorStopReason) {
        let handles = self.sessions.values().cloned().collect::<Vec<_>>();
        for handle in handles {
            let _ = Self::stop(&handle).await;
            handle.stop();
            let _ = handle.wait().await;
        }
    }
}

impl ActorHandler<CreateSession> for SessionManager {
    type Reply = Result<SessionSnapshot, SessionError>;

    async fn handle(&mut self, request: CreateSession) -> Self::Reply {
        let id = request.id.unwrap_or_default();
        if self.sessions.contains_key(&id) {
            return Err(SessionError::new(
                SessionErrorCode::Duplicate,
                format!("session {id} already exists"),
            ));
        }
        let (handle, snapshot) = SessionActor::start(id, request.mode, request.config).await?;
        if self.sessions.insert(id, handle).is_some() {
            return Err(SessionError::new(
                SessionErrorCode::Duplicate,
                format!("session {id} already exists"),
            ));
        }
        Ok(snapshot)
    }
}

impl ActorHandler<GetSession> for SessionManager {
    type Reply = Result<SessionSnapshot, SessionError>;

    async fn handle(&mut self, request: GetSession) -> Self::Reply {
        let handle = self.sessions.get(&request.id).ok_or_else(|| {
            SessionError::new(
                SessionErrorCode::NotFound,
                format!("session {} not found", request.id),
            )
        })?;
        Self::snapshot(handle).await
    }
}

impl ActorHandler<ListSessions> for SessionManager {
    type Reply = Vec<SessionSnapshot>;

    async fn handle(&mut self, _request: ListSessions) -> Self::Reply {
        let handles = self.sessions.values().cloned().collect::<Vec<_>>();
        let mut snapshots = Vec::with_capacity(handles.len());
        for handle in handles {
            if let Ok(snapshot) = Self::snapshot(&handle).await {
                snapshots.push(snapshot);
            }
        }
        snapshots.sort_by_key(|snapshot| snapshot.id.to_string());
        snapshots
    }
}

impl ActorHandler<StopSessionById> for SessionManager {
    type Reply = Result<SessionSnapshot, SessionError>;

    async fn handle(&mut self, request: StopSessionById) -> Self::Reply {
        let handle = self.sessions.get(&request.id).ok_or_else(|| {
            SessionError::new(
                SessionErrorCode::NotFound,
                format!("session {} not found", request.id),
            )
        })?;
        Self::stop(handle).await
    }
}

impl ActorHandler<RemoveSession> for SessionManager {
    type Reply = Result<(), SessionError>;

    async fn handle(&mut self, request: RemoveSession) -> Self::Reply {
        let handle = self.sessions.get(&request.id).ok_or_else(|| {
            SessionError::new(
                SessionErrorCode::NotFound,
                format!("session {} not found", request.id),
            )
        })?;
        let snapshot = Self::snapshot(handle).await?;
        if !snapshot.state.is_terminal() {
            return Err(SessionError::new(
                SessionErrorCode::NotTerminal,
                format!("session {} is not terminal", request.id),
            ));
        }
        if let Some(handle) = self.sessions.remove(&request.id) {
            handle.stop();
            let _ = handle.wait().await;
        }
        Ok(())
    }
}

impl ActorHandler<StopAllSessions> for SessionManager {
    type Reply = Vec<Result<SessionSnapshot, SessionError>>;

    async fn handle(&mut self, _request: StopAllSessions) -> Self::Reply {
        let handles = self.sessions.values().cloned().collect::<Vec<_>>();
        let mut outcomes = Vec::with_capacity(handles.len());
        for handle in handles {
            outcomes.push(Self::stop(&handle).await);
        }
        outcomes
    }
}
