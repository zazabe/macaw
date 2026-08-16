use super::{
    ProfileId, SessionConfig, SessionEndpoint, SessionError, SessionId, SessionMode, SessionName,
    SessionOutcome, SessionSnapshot, SessionState,
};
use macaw_core::prelude::*;
use std::collections::BTreeMap;
use std::net::SocketAddr;
use tokio::sync::{broadcast, watch};

const TRAFFIC_CHANNEL_CAPACITY: usize = 1024;

#[derive(Debug)]
pub struct GetSessionStatus;

#[derive(Debug)]
pub struct StopSession;

#[derive(Debug)]
pub struct SubscribeSessionTraffic;

#[derive(Debug)]
struct SessionCompleted(SessionOutcome);

#[derive(Debug)]
struct SessionFailed(SessionError);

#[derive(Debug)]
pub struct SessionActor {
    context: ActorContext,
    id: SessionId,
    name: Option<SessionName>,
    profile_id: ProfileId,
    mode: SessionMode,
    state: SessionState,
    endpoints: BTreeMap<String, SessionEndpoint>,
    outcome: Option<SessionOutcome>,
    error: Option<SessionError>,
    runtime_exit: AppExitHandle,
    completion: watch::Receiver<Option<Result<SessionOutcome, SessionError>>>,
    traffic_tx: Option<broadcast::Sender<RecordedEvent>>,
}

impl SessionActor {
    pub(crate) fn validate_profile(config: &SessionConfig) -> Result<(), SessionError> {
        validate_profile(config)
    }

    pub(crate) async fn start(
        id: SessionId,
        name: Option<SessionName>,
        profile_id: ProfileId,
        mode: SessionMode,
        mut config: SessionConfig,
    ) -> Result<(ActorHandle<Self>, SessionSnapshot), SessionError> {
        config.resolve_proxy_paths();
        validate(&mode, &config)?;
        let external_debug_tx = config.debug_tx.take();
        let (debug_tx, mut debug_rx) = tokio::sync::mpsc::unbounded_channel();
        let (traffic_tx, _) = broadcast::channel(TRAFFIC_CHANNEL_CAPACITY);
        let traffic_bridge_tx = traffic_tx.clone();
        config.debug_tx = Some(debug_tx);
        let (runtime_exit, completion_future) = start_runtime(&mode, &config).await?;
        let actor_system = ActorSystem::new();
        let context = actor_system.actor_context(&format!("session:{id}"));
        let (tx, rx) = actor_channel();
        let (completion_tx, completion_rx) = watch::channel(None);

        let actor = Self {
            context,
            id,
            name,
            profile_id,
            mode,
            state: SessionState::Running,
            endpoints: completion_future.endpoints,
            outcome: None,
            error: None,
            runtime_exit,
            completion: completion_rx,
            traffic_tx: Some(traffic_tx),
        };
        let snapshot = actor.snapshot();
        let handle = actor.run_with_channel(tx.clone(), rx);

        tokio::spawn(async move {
            while let Some(recorded) = debug_rx.recv().await {
                if let Some(ref external) = external_debug_tx {
                    let _ = external.send(recorded.clone());
                }
                let _ = traffic_bridge_tx.send(recorded);
            }
        });

        tokio::spawn(async move {
            let result = completion_future.future.await;
            completion_tx.send_replace(Some(result.clone()));
            match result {
                Ok(outcome) => {
                    let _ = tx.send(SessionCompleted(outcome));
                }
                Err(error) => {
                    let _ = tx.send(SessionFailed(error));
                }
            }
        });

        Ok((handle, snapshot))
    }

    fn snapshot(&self) -> SessionSnapshot {
        SessionSnapshot {
            id: self.id,
            name: self.name.clone(),
            profile_id: self.profile_id.clone(),
            mode: self.mode.clone(),
            state: self.state,
            endpoints: self.endpoints.clone(),
            outcome: self.outcome.clone(),
            error: self.error.clone(),
        }
    }

    fn apply_completion(&mut self, result: Result<SessionOutcome, SessionError>) {
        if self.state.is_terminal() {
            return;
        }
        self.traffic_tx.take();
        match result {
            Ok(outcome) => {
                self.state = SessionState::Stopped;
                self.outcome = Some(outcome);
            }
            Err(error) => {
                self.state = SessionState::Failed;
                self.error = Some(error);
            }
        }
    }

    async fn wait_for_completion(
        &mut self,
    ) -> Result<Result<SessionOutcome, SessionError>, SessionError> {
        loop {
            if let Some(result) = self.completion.borrow_and_update().clone() {
                return Ok(result);
            }
            self.completion
                .changed()
                .await
                .map_err(|_| SessionError::runtime("session completion channel closed"))?;
        }
    }
}

impl Actor for SessionActor {
    fn context(&self) -> &ActorContext {
        &self.context
    }

    async fn on_stop(&mut self, _reason: ActorStopReason) {
        if !self.state.is_terminal() {
            self.runtime_exit.exit();
        }
    }
}

impl ActorHandler<GetSessionStatus> for SessionActor {
    type Reply = SessionSnapshot;

    async fn handle(&mut self, _message: GetSessionStatus) -> Self::Reply {
        let completion = self.completion.borrow().clone();
        if let Some(result) = completion {
            self.apply_completion(result);
        }
        self.snapshot()
    }
}

impl ActorHandler<StopSession> for SessionActor {
    type Reply = Result<SessionSnapshot, SessionError>;

    async fn handle(&mut self, _message: StopSession) -> Self::Reply {
        if self.state.is_terminal() {
            return Ok(self.snapshot());
        }
        if self.state != SessionState::Stopping {
            self.state = SessionState::Stopping;
            self.runtime_exit.exit();
        }
        let result = self.wait_for_completion().await?;
        self.apply_completion(result);
        Ok(self.snapshot())
    }
}

impl ActorHandler<SubscribeSessionTraffic> for SessionActor {
    type Reply = Result<broadcast::Receiver<RecordedEvent>, SessionError>;

    async fn handle(&mut self, _message: SubscribeSessionTraffic) -> Self::Reply {
        self.traffic_tx
            .as_ref()
            .map(broadcast::Sender::subscribe)
            .ok_or_else(|| {
                SessionError::new(
                    super::SessionErrorCode::NotTerminal,
                    "session traffic stream is no longer available",
                )
            })
    }
}

impl ActorHandler<SessionCompleted> for SessionActor {
    type Reply = ();

    async fn handle(&mut self, message: SessionCompleted) {
        self.apply_completion(Ok(message.0));
    }
}

impl ActorHandler<SessionFailed> for SessionActor {
    type Reply = ();

    async fn handle(&mut self, message: SessionFailed) {
        self.apply_completion(Err(message.0));
    }
}

struct RuntimeCompletion {
    endpoints: BTreeMap<String, SessionEndpoint>,
    future: std::pin::Pin<
        Box<dyn Future<Output = Result<SessionOutcome, SessionError>> + Send + 'static>,
    >,
}

async fn start_runtime(
    mode: &SessionMode,
    config: &SessionConfig,
) -> Result<(AppExitHandle, RuntimeCompletion), SessionError> {
    match mode {
        SessionMode::Record { output } => {
            let output = config.resolve_path(output);
            let mut runtime = Macaw::<Recorder>::recorder_with_options(RecorderOptions {
                debug_tx: config.debug_tx.clone(),
            });
            let endpoints = match bind_recorder(&mut runtime, config).await {
                Ok(endpoints) => endpoints,
                Err(error) => {
                    let _ = runtime.shutdown().await;
                    return Err(error);
                }
            };
            let exit = runtime.exit_handle();
            let future = Box::pin(async move {
                let outcome = runtime
                    .record_when_exit(output)
                    .await
                    .map_err(|error| SessionError::runtime(error.to_string()))?;
                Ok(SessionOutcome::Record {
                    recording_path: outcome.recording_path,
                    total_events: outcome.total_events,
                    total_bytes: outcome.total_bytes,
                    total_time_millis: outcome
                        .total_time
                        .map(|duration| duration.as_millis().min(u128::from(u64::MAX)) as u64),
                })
            });
            Ok((exit, RuntimeCompletion { endpoints, future }))
        }
        SessionMode::Replay { recording } => {
            let recording = config.resolve_path(recording);
            let mut runtime = Macaw::<Replayer>::replayer_with_options(
                recording,
                ReplayerOptions {
                    debug_tx: config.debug_tx.clone(),
                },
            )
            .map_err(|error| SessionError::startup(error.to_string()))?;
            let endpoints = match bind_replayer(&mut runtime, config).await {
                Ok(endpoints) => endpoints,
                Err(error) => {
                    let _ = runtime.shutdown().await;
                    return Err(error);
                }
            };
            runtime
                .play()
                .map_err(|error| SessionError::startup(error.to_string()))?;
            let exit = runtime.exit_handle();
            let future = Box::pin(async move {
                runtime
                    .wait_until_stopped()
                    .await
                    .map_err(|error| SessionError::runtime(error.to_string()))?;
                Ok(SessionOutcome::Replay)
            });
            Ok((exit, RuntimeCompletion { endpoints, future }))
        }
    }
}

fn validate(mode: &SessionMode, config: &SessionConfig) -> Result<(), SessionError> {
    validate_profile(config)?;
    for (name, proxy) in &config.proxies {
        proxy
            .validate(matches!(mode, SessionMode::Record { .. }))
            .map_err(|message| SessionError::invalid(format!("proxy {name}: {message}")))?;
    }
    Ok(())
}

fn validate_profile(config: &SessionConfig) -> Result<(), SessionError> {
    if config.proxies.is_empty() {
        return Err(SessionError::invalid("at least one proxy is required"));
    }
    for (name, proxy) in &config.proxies {
        name.parse::<ProxyId>()
            .map_err(|_| SessionError::invalid(format!("invalid proxy id: {name}")))?;
        proxy
            .bind()
            .parse::<SocketAddr>()
            .map_err(|_| SessionError::invalid(format!("invalid bind address for proxy {name}")))?;
        proxy
            .validate(false)
            .map_err(|message| SessionError::invalid(format!("proxy {name}: {message}")))?;
    }
    Ok(())
}

async fn bind_recorder(
    runtime: &mut Macaw<Recorder>,
    config: &SessionConfig,
) -> Result<BTreeMap<String, SessionEndpoint>, SessionError> {
    let mut endpoints = BTreeMap::new();
    for (name, proxy) in &config.proxies {
        let endpoint = proxy
            .bind_to_recorder(name, runtime)
            .await
            .map_err(|error| {
                SessionError::startup(format!("failed to bind proxy {name}: {error}"))
            })?;
        endpoints.insert(
            name.clone(),
            SessionEndpoint::new(proxy.protocol(), endpoint),
        );
    }
    Ok(endpoints)
}

async fn bind_replayer(
    runtime: &mut Macaw<Replayer>,
    config: &SessionConfig,
) -> Result<BTreeMap<String, SessionEndpoint>, SessionError> {
    let mut endpoints = BTreeMap::new();
    for (name, proxy) in &config.proxies {
        let endpoint = proxy
            .bind_to_replayer(name, runtime)
            .await
            .map_err(|error| {
                SessionError::startup(format!("failed to bind proxy {name}: {error}"))
            })?;
        endpoints.insert(
            name.clone(),
            SessionEndpoint::new(proxy.protocol(), endpoint),
        );
    }
    Ok(endpoints)
}
