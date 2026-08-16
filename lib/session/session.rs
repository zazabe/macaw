use super::{
    ProfileId, SessionConfig, SessionEndpoint, SessionError, SessionId, SessionMode, SessionName,
    SessionOutcome, SessionSnapshot, SessionState,
};
use macaw_core::prelude::*;
use std::collections::{BTreeMap, VecDeque};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use tokio::sync::{broadcast, watch};

const TRAFFIC_CHANNEL_CAPACITY: usize = 1024;
const TRAFFIC_HISTORY_CAPACITY: usize = 1024;
const DEFAULT_TRAFFIC_HISTORY: usize = 3;

#[derive(Debug)]
pub struct GetSessionStatus;

#[derive(Debug)]
pub struct StopSession;

#[derive(Debug)]
pub struct StartSession;

#[derive(Debug)]
pub struct SubscribeSessionTraffic {
    pub after: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct SequencedRecordedEvent {
    pub sequence: u64,
    pub event: RecordedEvent,
}

#[derive(Debug)]
pub struct TrafficSubscription {
    pub history: Vec<SequencedRecordedEvent>,
    pub receiver: Option<broadcast::Receiver<SequencedRecordedEvent>>,
    pub dropped: u64,
}

#[derive(Debug, Default)]
struct TrafficHistory {
    next_sequence: u64,
    events: VecDeque<SequencedRecordedEvent>,
}

impl TrafficHistory {
    fn push(&mut self, event: RecordedEvent) -> SequencedRecordedEvent {
        self.next_sequence += 1;
        let event = SequencedRecordedEvent {
            sequence: self.next_sequence,
            event,
        };
        self.events.push_back(event.clone());
        if self.events.len() > TRAFFIC_HISTORY_CAPACITY {
            self.events.pop_front();
        }
        event
    }

    fn snapshot(&self, after: Option<u64>) -> (Vec<SequencedRecordedEvent>, u64) {
        match after {
            Some(after) => {
                let first = self.events.front().map(|event| event.sequence);
                let dropped = first
                    .filter(|first| after.saturating_add(1) < *first)
                    .map(|first| first.saturating_sub(after.saturating_add(1)))
                    .unwrap_or(0);
                (
                    self.events
                        .iter()
                        .filter(|event| event.sequence > after)
                        .cloned()
                        .collect(),
                    dropped,
                )
            }
            None => (
                self.events
                    .iter()
                    .rev()
                    .take(DEFAULT_TRAFFIC_HISTORY)
                    .cloned()
                    .collect::<Vec<_>>()
                    .into_iter()
                    .rev()
                    .collect(),
                0,
            ),
        }
    }
}

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
    config: Option<SessionConfig>,
    runtime_exit: Option<AppExitHandle>,
    completion: Option<watch::Receiver<Option<Result<SessionOutcome, SessionError>>>>,
    traffic_history: Arc<Mutex<TrafficHistory>>,
    traffic_tx: Option<broadcast::Sender<SequencedRecordedEvent>>,
    actor_tx: ActorChannelSender<SessionActor>,
}

impl SessionActor {
    pub(crate) fn validate_profile(config: &SessionConfig) -> Result<(), SessionError> {
        validate_profile(config)
    }

    pub(crate) fn prepare(
        id: SessionId,
        name: Option<SessionName>,
        profile_id: ProfileId,
        mode: SessionMode,
        mut config: SessionConfig,
    ) -> Result<(ActorHandle<Self>, SessionSnapshot), SessionError> {
        config.resolve_proxy_paths();
        validate(&mode, &config)?;
        let (traffic_tx, _) = broadcast::channel(TRAFFIC_CHANNEL_CAPACITY);
        let actor_system = ActorSystem::new();
        let context = actor_system.actor_context(&format!("session:{id}"));
        let (actor_tx, actor_rx) = actor_channel();

        let actor = Self {
            context,
            id,
            name,
            profile_id,
            mode,
            state: SessionState::Ready,
            endpoints: BTreeMap::new(),
            outcome: None,
            error: None,
            config: Some(config),
            runtime_exit: None,
            completion: None,
            traffic_history: Arc::new(Mutex::new(TrafficHistory::default())),
            traffic_tx: Some(traffic_tx),
            actor_tx: actor_tx.clone(),
        };
        let snapshot = actor.snapshot();
        Ok((actor.run_with_channel(actor_tx, actor_rx), snapshot))
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
        let completion = self
            .completion
            .as_mut()
            .ok_or_else(|| SessionError::runtime("session runtime was not started"))?;
        loop {
            if let Some(result) = completion.borrow_and_update().clone() {
                return Ok(result);
            }
            completion
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
        if !self.state.is_terminal()
            && let Some(runtime_exit) = &self.runtime_exit
        {
            runtime_exit.exit();
        }
    }
}

impl ActorHandler<GetSessionStatus> for SessionActor {
    type Reply = SessionSnapshot;

    async fn handle(&mut self, _message: GetSessionStatus) -> Self::Reply {
        let completion = self
            .completion
            .as_ref()
            .and_then(|completion| completion.borrow().clone());
        if let Some(result) = completion {
            self.apply_completion(result);
        }
        self.snapshot()
    }
}

impl ActorHandler<StartSession> for SessionActor {
    type Reply = Result<SessionSnapshot, SessionError>;

    async fn handle(&mut self, _message: StartSession) -> Self::Reply {
        if self.state != SessionState::Ready {
            return Err(SessionError::new(
                super::SessionErrorCode::Unsupported,
                format!("session cannot start from state {:?}", self.state),
            ));
        }
        self.state = SessionState::Starting;
        let mut config = self
            .config
            .take()
            .ok_or_else(|| SessionError::startup("session configuration is unavailable"))?;
        let external_debug_tx = config.debug_tx.take();
        let (debug_tx, mut debug_rx) = tokio::sync::mpsc::unbounded_channel();
        config.debug_tx = Some(debug_tx);

        let (runtime_exit, completion_future) = match start_runtime(&self.mode, &config).await {
            Ok(runtime) => runtime,
            Err(error) => {
                self.state = SessionState::Failed;
                self.error = Some(error);
                self.traffic_tx.take();
                return Ok(self.snapshot());
            }
        };
        self.endpoints = completion_future.endpoints;
        self.runtime_exit = Some(runtime_exit);
        self.state = SessionState::Running;

        let traffic_history = self.traffic_history.clone();
        let traffic_tx = self
            .traffic_tx
            .as_ref()
            .expect("ready session must have a traffic sender")
            .clone();
        let traffic_bridge = tokio::spawn(async move {
            while let Some(recorded) = debug_rx.recv().await {
                if let Some(ref external) = external_debug_tx {
                    let _ = external.send(recorded.clone());
                }
                let mut history = traffic_history
                    .lock()
                    .expect("traffic history lock poisoned");
                let sequenced = history.push(recorded);
                let _ = traffic_tx.send(sequenced);
                drop(history);
            }
        });

        let (completion_tx, completion_rx) = watch::channel(None);
        self.completion = Some(completion_rx);
        let actor_tx = self.actor_tx.clone();
        tokio::spawn(async move {
            let result = completion_future.future.await;
            let _ = traffic_bridge.await;
            completion_tx.send_replace(Some(result.clone()));
            match result {
                Ok(outcome) => {
                    let _ = actor_tx.send(SessionCompleted(outcome));
                }
                Err(error) => {
                    let _ = actor_tx.send(SessionFailed(error));
                }
            }
        });

        Ok(self.snapshot())
    }
}

impl ActorHandler<StopSession> for SessionActor {
    type Reply = Result<SessionSnapshot, SessionError>;

    async fn handle(&mut self, _message: StopSession) -> Self::Reply {
        if self.state.is_terminal() {
            return Ok(self.snapshot());
        }
        if self.state == SessionState::Ready {
            self.state = SessionState::Stopped;
            self.config.take();
            self.traffic_tx.take();
            return Ok(self.snapshot());
        }
        if self.state != SessionState::Stopping {
            self.state = SessionState::Stopping;
            if let Some(runtime_exit) = &self.runtime_exit {
                runtime_exit.exit();
            }
        }
        let result = self.wait_for_completion().await?;
        self.apply_completion(result);
        Ok(self.snapshot())
    }
}

impl ActorHandler<SubscribeSessionTraffic> for SessionActor {
    type Reply = Result<TrafficSubscription, SessionError>;

    async fn handle(&mut self, message: SubscribeSessionTraffic) -> Self::Reply {
        let history = self
            .traffic_history
            .lock()
            .expect("traffic history lock poisoned");
        let (events, dropped) = history.snapshot(message.after);
        let receiver = self.traffic_tx.as_ref().map(broadcast::Sender::subscribe);
        Ok(TrafficSubscription {
            history: events,
            receiver,
            dropped,
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
