use super::{
    GetSessionStatus, ProfileId, ProfileSnapshot, SessionActor, SessionConfig, SessionError,
    SessionErrorCode, SessionId, SessionMode, SessionName, SessionSnapshot, StartSession,
    StopSession, SubscribeSessionTraffic, TrafficSubscription,
};
use macaw_core::prelude::*;
use std::collections::HashMap;
use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct CreateSession {
    pub id: Option<SessionId>,
    pub name: Option<SessionName>,
    pub profile_id: ProfileId,
    pub mode: SessionMode,
}

impl CreateSession {
    pub fn new(profile_id: ProfileId, mode: SessionMode) -> Self {
        Self {
            id: None,
            name: None,
            profile_id,
            mode,
        }
    }
}

#[derive(Debug, Clone)]
pub struct CreateProfile {
    pub id: ProfileId,
    pub config: SessionConfig,
}

#[derive(Debug, Clone)]
pub struct GetProfile {
    pub id: ProfileId,
}

#[derive(Debug)]
pub struct ListProfiles;

#[derive(Debug, Clone)]
pub struct RemoveProfile {
    pub id: ProfileId,
}

#[derive(Debug, Clone)]
pub struct ListSessionsByProfile {
    pub profile_id: ProfileId,
}

#[derive(Debug, Clone, Copy)]
pub struct GetSession {
    pub id: SessionId,
}

#[derive(Debug, Clone)]
pub struct GetSessionByName {
    pub name: SessionName,
}

#[derive(Debug)]
pub struct ListSessions;

#[derive(Debug, Clone, Copy)]
pub struct StopSessionById {
    pub id: SessionId,
}

#[derive(Debug, Clone, Copy)]
pub struct StartSessionById {
    pub id: SessionId,
}

#[derive(Debug, Clone, Copy)]
pub struct RemoveSession {
    pub id: SessionId,
}

#[derive(Debug)]
pub struct StopAllSessions;

#[derive(Debug)]
pub struct BeginShutdown;

#[derive(Debug)]
pub struct GetManagerReadiness;

#[derive(Debug, Clone, Copy)]
pub struct SubscribeTraffic {
    pub id: SessionId,
    pub after: Option<u64>,
}

#[derive(Debug)]
pub struct SessionManager {
    context: ActorContext,
    profiles: HashMap<ProfileId, SessionConfig>,
    sessions: HashMap<SessionId, ActorHandle<SessionActor>>,
    accepting_sessions: bool,
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
            profiles: HashMap::new(),
            sessions: HashMap::new(),
            accepting_sessions: true,
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

    async fn start_session(
        handle: &ActorHandle<SessionActor>,
    ) -> Result<SessionSnapshot, SessionError> {
        handle
            .request(StartSession)
            .await
            .map_err(SessionError::actor)?
    }

    fn profile_snapshot(id: ProfileId, config: &SessionConfig) -> ProfileSnapshot {
        ProfileSnapshot {
            id,
            config_root: config.root.clone(),
            proxies: config
                .proxies
                .iter()
                .map(|(name, proxy)| {
                    (
                        name.clone(),
                        super::ProfileProxySnapshot {
                            protocol: proxy.protocol().to_owned(),
                            bind: proxy.bind().to_owned(),
                            target: proxy.target().map(str::to_owned),
                            overrides: proxy.overrides().map(str::to_owned),
                        },
                    )
                })
                .collect(),
        }
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
        let profile_id = transient_profile_id();
        self.create_profile(profile_id.clone(), config).await?;
        let snapshot = self
            .request(CreateSession::new(
                profile_id,
                SessionMode::Record {
                    output: output.into(),
                },
            ))
            .await??;
        self.start_session(snapshot.id).await
    }

    pub async fn create_profile(
        &self,
        id: ProfileId,
        config: SessionConfig,
    ) -> Result<ProfileSnapshot, SessionError> {
        self.request(CreateProfile { id, config }).await?
    }

    pub async fn get_profile(&self, id: ProfileId) -> Result<ProfileSnapshot, SessionError> {
        self.request(GetProfile { id }).await?
    }

    pub async fn list_profiles(&self) -> Result<Vec<ProfileSnapshot>, SessionError> {
        self.request(ListProfiles).await
    }

    pub async fn remove_profile(&self, id: ProfileId) -> Result<(), SessionError> {
        self.request(RemoveProfile { id }).await?
    }

    pub async fn create(&self, request: CreateSession) -> Result<SessionSnapshot, SessionError> {
        self.request(request).await?
    }

    pub async fn replay(
        &self,
        recording: impl Into<PathBuf>,
        config: SessionConfig,
    ) -> Result<SessionSnapshot, SessionError> {
        let profile_id = transient_profile_id();
        self.create_profile(profile_id.clone(), config).await?;
        let snapshot = self
            .request(CreateSession::new(
                profile_id,
                SessionMode::Replay {
                    recording: recording.into(),
                },
            ))
            .await??;
        self.start_session(snapshot.id).await
    }

    pub async fn get(&self, id: SessionId) -> Result<SessionSnapshot, SessionError> {
        self.request(GetSession { id }).await?
    }

    pub async fn get_by_name(&self, name: SessionName) -> Result<SessionSnapshot, SessionError> {
        self.request(GetSessionByName { name }).await?
    }

    pub async fn list(&self) -> Result<Vec<SessionSnapshot>, SessionError> {
        self.request(ListSessions).await?
    }

    pub async fn list_by_profile(
        &self,
        profile_id: ProfileId,
    ) -> Result<Vec<SessionSnapshot>, SessionError> {
        self.request(ListSessionsByProfile { profile_id }).await?
    }

    pub async fn stop_session(&self, id: SessionId) -> Result<SessionSnapshot, SessionError> {
        self.request(StopSessionById { id }).await?
    }

    pub async fn start_session(&self, id: SessionId) -> Result<SessionSnapshot, SessionError> {
        self.request(StartSessionById { id }).await?
    }

    pub async fn remove(&self, id: SessionId) -> Result<(), SessionError> {
        self.request(RemoveSession { id }).await?
    }

    pub async fn stop_all(
        &self,
    ) -> Result<Vec<Result<SessionSnapshot, SessionError>>, SessionError> {
        self.request(StopAllSessions).await
    }

    pub async fn begin_shutdown(&self) -> Result<(), SessionError> {
        self.request(BeginShutdown).await
    }

    pub async fn is_ready(&self) -> Result<bool, SessionError> {
        self.request(GetManagerReadiness).await
    }

    pub async fn subscribe_traffic(
        &self,
        id: SessionId,
        after: Option<u64>,
    ) -> Result<TrafficSubscription, SessionError> {
        self.request(SubscribeTraffic { id, after }).await?
    }

    /// Gracefully stop all sessions and await manager completion.
    pub async fn shutdown(&self) -> Result<(), SessionError> {
        self.actor.stop();
        self.actor.wait().await.map_err(SessionError::actor)
    }
}

fn transient_profile_id() -> ProfileId {
    ProfileId::new(format!("cli-{}", SessionId::new()))
        .expect("generated transient profile id must be valid")
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

impl ActorHandler<CreateProfile> for SessionManager {
    type Reply = Result<ProfileSnapshot, SessionError>;

    async fn handle(&mut self, request: CreateProfile) -> Self::Reply {
        if !self.accepting_sessions {
            return Err(SessionError::new(
                SessionErrorCode::ShuttingDown,
                "session manager is shutting down",
            ));
        }
        if self.profiles.contains_key(&request.id) {
            return Err(SessionError::new(
                SessionErrorCode::Duplicate,
                format!("profile {} already exists", request.id),
            ));
        }
        SessionActor::validate_profile(&request.config)?;
        let snapshot = Self::profile_snapshot(request.id.clone(), &request.config);
        self.profiles.insert(request.id, request.config);
        Ok(snapshot)
    }
}

impl ActorHandler<GetProfile> for SessionManager {
    type Reply = Result<ProfileSnapshot, SessionError>;

    async fn handle(&mut self, request: GetProfile) -> Self::Reply {
        self.profiles
            .get(&request.id)
            .map(|config| Self::profile_snapshot(request.id, config))
            .ok_or_else(|| SessionError::new(SessionErrorCode::NotFound, "profile not found"))
    }
}

impl ActorHandler<ListProfiles> for SessionManager {
    type Reply = Vec<ProfileSnapshot>;

    async fn handle(&mut self, _request: ListProfiles) -> Self::Reply {
        let mut profiles = self
            .profiles
            .iter()
            .map(|(id, config)| Self::profile_snapshot(id.clone(), config))
            .collect::<Vec<_>>();
        profiles.sort_by_key(|profile| profile.id.to_string());
        profiles
    }
}

impl ActorHandler<RemoveProfile> for SessionManager {
    type Reply = Result<(), SessionError>;

    async fn handle(&mut self, request: RemoveProfile) -> Self::Reply {
        self.profiles
            .remove(&request.id)
            .map(|_| ())
            .ok_or_else(|| {
                SessionError::new(
                    SessionErrorCode::NotFound,
                    format!("profile {} not found", request.id),
                )
            })
    }
}

impl ActorHandler<CreateSession> for SessionManager {
    type Reply = Result<SessionSnapshot, SessionError>;

    async fn handle(&mut self, request: CreateSession) -> Self::Reply {
        if !self.accepting_sessions {
            return Err(SessionError::new(
                SessionErrorCode::ShuttingDown,
                "session manager is shutting down",
            ));
        }
        let id = request.id.unwrap_or_default();
        if self.sessions.contains_key(&id) {
            return Err(SessionError::new(
                SessionErrorCode::Duplicate,
                format!("session {id} already exists"),
            ));
        }
        if let Some(name) = &request.name {
            let handles = self.sessions.values().cloned().collect::<Vec<_>>();
            for handle in handles {
                if Self::snapshot(&handle).await?.name.as_ref() == Some(name) {
                    return Err(SessionError::new(
                        SessionErrorCode::Duplicate,
                        format!("session name {name} already exists"),
                    ));
                }
            }
        }
        let config = self
            .profiles
            .get(&request.profile_id)
            .cloned()
            .ok_or_else(|| {
                SessionError::new(
                    SessionErrorCode::NotFound,
                    format!("profile {} not found", request.profile_id),
                )
            })?;
        let (handle, snapshot) =
            SessionActor::prepare(id, request.name, request.profile_id, request.mode, config)?;
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

impl ActorHandler<GetSessionByName> for SessionManager {
    type Reply = Result<SessionSnapshot, SessionError>;

    async fn handle(&mut self, request: GetSessionByName) -> Self::Reply {
        let handles = self.sessions.values().cloned().collect::<Vec<_>>();
        for handle in handles {
            let snapshot = Self::snapshot(&handle).await?;
            if snapshot.name.as_ref() == Some(&request.name) {
                return Ok(snapshot);
            }
        }
        Err(SessionError::new(
            SessionErrorCode::NotFound,
            format!("session {} not found", request.name),
        ))
    }
}

impl ActorHandler<ListSessions> for SessionManager {
    type Reply = Result<Vec<SessionSnapshot>, SessionError>;

    async fn handle(&mut self, _request: ListSessions) -> Self::Reply {
        let handles = self.sessions.values().cloned().collect::<Vec<_>>();
        let mut snapshots = Vec::with_capacity(handles.len());
        for handle in handles {
            snapshots.push(Self::snapshot(&handle).await?);
        }
        snapshots.sort_by_key(|snapshot| snapshot.id.to_string());
        Ok(snapshots)
    }
}

impl ActorHandler<ListSessionsByProfile> for SessionManager {
    type Reply = Result<Vec<SessionSnapshot>, SessionError>;

    async fn handle(&mut self, request: ListSessionsByProfile) -> Self::Reply {
        if !self.profiles.contains_key(&request.profile_id) {
            return Err(SessionError::new(
                SessionErrorCode::NotFound,
                format!("profile {} not found", request.profile_id),
            ));
        }
        let handles = self.sessions.values().cloned().collect::<Vec<_>>();
        let mut snapshots = Vec::new();
        for handle in handles {
            let snapshot = Self::snapshot(&handle).await?;
            if snapshot.profile_id == request.profile_id {
                snapshots.push(snapshot);
            }
        }
        snapshots.sort_by_key(|snapshot| snapshot.id.to_string());
        Ok(snapshots)
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

impl ActorHandler<StartSessionById> for SessionManager {
    type Reply = Result<SessionSnapshot, SessionError>;

    async fn handle(&mut self, request: StartSessionById) -> Self::Reply {
        let handle = self.sessions.get(&request.id).ok_or_else(|| {
            SessionError::new(
                SessionErrorCode::NotFound,
                format!("session {} not found", request.id),
            )
        })?;
        Self::start_session(handle).await
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
        if !snapshot.state.is_terminal() && snapshot.state != super::SessionState::Ready {
            return Err(SessionError::new(
                SessionErrorCode::NotTerminal,
                format!("session {} is running", request.id),
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
        self.accepting_sessions = false;
        let handles = self.sessions.values().cloned().collect::<Vec<_>>();
        let mut outcomes = Vec::with_capacity(handles.len());
        for handle in handles {
            outcomes.push(Self::stop(&handle).await);
        }
        outcomes
    }
}

impl ActorHandler<BeginShutdown> for SessionManager {
    type Reply = ();

    async fn handle(&mut self, _request: BeginShutdown) {
        self.accepting_sessions = false;
    }
}

impl ActorHandler<GetManagerReadiness> for SessionManager {
    type Reply = bool;

    async fn handle(&mut self, _request: GetManagerReadiness) -> Self::Reply {
        self.accepting_sessions
    }
}

impl ActorHandler<SubscribeTraffic> for SessionManager {
    type Reply = Result<TrafficSubscription, SessionError>;

    async fn handle(&mut self, request: SubscribeTraffic) -> Self::Reply {
        let handle = self.sessions.get(&request.id).ok_or_else(|| {
            SessionError::new(
                SessionErrorCode::NotFound,
                format!("session {} not found", request.id),
            )
        })?;
        handle
            .request(SubscribeSessionTraffic {
                after: request.after,
            })
            .await
            .map_err(SessionError::actor)?
    }
}
