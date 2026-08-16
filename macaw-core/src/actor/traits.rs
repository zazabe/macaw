use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::task::JoinHandle;

use crate::lib::*;
use futures::FutureExt;

pub trait Actor: Send + Sized + 'static {
    fn context(&self) -> &ActorContext;

    fn on_start(&mut self) -> impl Future<Output = ()> + Send {
        debug!("Actor starting: {}", self.context().name);
        futures::future::ready(())
    }

    fn on_stop(&mut self, reason: ActorStopReason) -> impl Future<Output = ()> + Send {
        debug!(
            "Actor stopped: {}, reason: {:?}",
            self.context().name,
            reason
        );
        futures::future::ready(())
    }

    fn on_error(&mut self, error: anyhow::Error) {
        error!("Actor stopped with error: {}", self.context().name);
        self.context().exit_with_error(error);
    }

    fn run_with_channel(
        self,
        tx: ActorChannelSender<Self>,
        rx: ActorChannelReceiver<Self>,
    ) -> ActorHandle<Self> {
        let context = self.context().clone();
        let task = tokio::task::Builder::new()
            .name(&context.name)
            .spawn(run_actor(self, rx))
            .expect("failed to spawn actor task");
        let (completion_tx, completion_rx) = watch::channel(ActorTaskStatus::Running);
        tokio::spawn(async move {
            let status = match task.await {
                Ok(Ok(())) => ActorTaskStatus::Stopped,
                Ok(Err(error)) => ActorTaskStatus::Failed(Arc::from(error.to_string())),
                Err(error) => ActorTaskStatus::Failed(Arc::from(error.to_string())),
            };
            completion_tx.send_replace(status);
        });

        ActorHandle::new(tx, context, completion_rx)
    }

    fn run(self) -> ActorHandle<Self> {
        let (tx, rx) = actor_channel::<Self>();
        self.run_with_channel(tx, rx)
    }
}

pub trait ActorHandler<M>: Send + 'static
where
    M: ActorMessage,
{
    type Reply: Send + 'static;

    fn handle(&mut self, msg: M) -> impl Future<Output = Self::Reply> + Send;
}

// ------------------------------------------------------------

#[derive(Debug)]
pub struct AppContext {
    terminate: AppTerminator,
}

impl AppContext {
    pub fn new() -> Self {
        Self {
            terminate: AppTerminator::new(),
        }
    }

    pub fn actor_context(&self, name: &str) -> ActorContext {
        let notifier = self.terminate.exit_handle();
        ActorContext::new(name, notifier)
    }

    pub fn exit_handle(&self) -> AppExitHandle {
        self.terminate.exit_handle()
    }

    #[allow(unused)]
    pub fn exit(&self) {
        self.terminate.exit();
    }

    #[allow(unused)]
    pub fn exit_with_error(&self, error: anyhow::Error) {
        self.terminate.exit_with_error(error);
    }

    pub async fn wait_until_exit(self) -> Result<(), AppError> {
        self.terminate.wait_until_exit().await
    }
}

/// Public factory and lifecycle domain for a related group of actors.
pub type ActorSystem = AppContext;

impl Default for AppContext {
    fn default() -> Self {
        Self::new()
    }
}

// ------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct ActorContext {
    name: String,
    exit_notifier: AppExitHandle,
    task_terminator: TaskTerminator,
    tasks: Arc<ActorTaskTracker>,
}

impl ActorContext {
    fn new(name: &str, exit_notifier: AppExitHandle) -> Self {
        Self {
            name: name.to_string(),
            exit_notifier,
            task_terminator: TaskTerminator::new(),
            tasks: Arc::new(ActorTaskTracker::default()),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn create_child(&self, name: &str) -> Self {
        Self {
            name: self.name.clone() + ":" + name,
            exit_notifier: self.exit_notifier.clone(),
            task_terminator: TaskTerminator::new(),
            tasks: Arc::new(ActorTaskTracker::default()),
        }
    }

    pub fn exit(&self) {
        self.exit_notifier.exit();
    }

    pub fn exit_with_error(&self, error: anyhow::Error) {
        self.exit_notifier.exit_with_error(error);
    }

    pub fn stop(&self) {
        self.task_terminator.stop();
    }

    pub fn spawn<F, R>(
        &self,
        name: &str,
        future: F,
    ) -> Result<JoinHandle<TerminationReason<R>>, anyhow::Error>
    where
        F: Future<Output = R> + Send + 'static,
        R: Send + 'static,
    {
        let name = self.name.clone() + ":" + name;
        self.tasks.started();
        let tasks = Arc::clone(&self.tasks);
        let handle = tokio::task::Builder::new().name(&name).spawn({
            let mut context = self.clone();
            async move {
                let _guard = ActorTaskGuard(tasks);
                terminatable_future(&mut context, future).await
            }
        });
        if handle.is_err() {
            self.tasks.finished();
        }
        let handle = handle?;
        Ok(handle)
    }

    async fn stop_and_join_children(&self, stop_tasks: bool) {
        if stop_tasks {
            self.task_terminator.stop();
        }
        self.tasks.wait().await;
    }
}

struct ActorTaskGuard(Arc<ActorTaskTracker>);

impl Drop for ActorTaskGuard {
    fn drop(&mut self) {
        self.0.finished();
    }
}

#[derive(Debug, Default)]
struct ActorTaskTracker {
    active: AtomicUsize,
    notify: tokio::sync::Notify,
}

impl ActorTaskTracker {
    fn started(&self) {
        self.active.fetch_add(1, Ordering::AcqRel);
    }

    fn finished(&self) {
        if self.active.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.notify.notify_waiters();
        }
    }

    async fn wait(&self) {
        loop {
            let notified = self.notify.notified();
            if self.active.load(Ordering::Acquire) == 0 {
                return;
            }
            notified.await;
        }
    }
}

// ------------------------------------------------------------

pub trait ErasedActorHandle: Send {
    fn stop(&self);

    fn exit(&self);

    fn exit_with_error(&self, error: anyhow::Error);

    fn completion(&self) -> ActorCompletion;
}

impl fmt::Debug for Box<dyn ErasedActorHandle> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ErasedActorHandle")
    }
}

#[derive(Debug)]
pub struct ActorHandle<A>
where
    A: Actor,
{
    inner: Arc<ActorHandleInner<A>>,
}

impl<A> ActorHandle<A>
where
    A: Actor,
{
    pub fn new(
        tx: ActorChannelSender<A>,
        context: ActorContext,
        completion: watch::Receiver<ActorTaskStatus>,
    ) -> Self {
        Self {
            inner: Arc::new(ActorHandleInner {
                tx,
                context,
                completion,
            }),
        }
    }

    pub(crate) fn sender(&self) -> ActorChannelSender<A> {
        self.inner.sender()
    }

    pub fn send<M>(&self, message: M) -> Result<(), anyhow::Error>
    where
        A: ActorHandler<M, Reply = ()>,
        M: ActorMessage,
    {
        self.inner.send(message)
    }

    pub async fn request<M, R>(&self, message: M) -> Result<R, anyhow::Error>
    where
        A: ActorHandler<M, Reply = R>,
        M: ActorMessage,
        R: Send + 'static,
    {
        self.inner.request(message).await
    }

    /// Stop only this actor, leaving other actors in its system running.
    pub fn stop(&self) {
        self.inner.context.task_terminator.stop();
    }

    /// Request exit for the actor's whole application lifecycle.
    pub fn exit(&self) {
        self.inner.context.exit();
    }

    pub fn exit_with_error(&self, error: anyhow::Error) {
        self.inner.context.exit_with_error(error);
    }

    /// Return a cloneable completion observer. Every observer sees the same result.
    pub fn completion(&self) -> ActorCompletion {
        ActorCompletion {
            receiver: self.inner.completion.clone(),
        }
    }

    pub async fn wait(&self) -> Result<(), anyhow::Error> {
        self.completion().wait().await
    }
}

impl<A> Clone for ActorHandle<A>
where
    A: Actor,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

#[derive(Debug)]
struct ActorHandleInner<A>
where
    A: Actor,
{
    tx: ActorChannelSender<A>,
    context: ActorContext,
    completion: watch::Receiver<ActorTaskStatus>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ActorTaskStatus {
    Running,
    Stopped,
    Failed(Arc<str>),
}

#[derive(Debug, Clone)]
pub struct ActorCompletion {
    receiver: watch::Receiver<ActorTaskStatus>,
}

impl ActorCompletion {
    pub async fn wait(mut self) -> Result<(), anyhow::Error> {
        loop {
            match self.receiver.borrow_and_update().clone() {
                ActorTaskStatus::Running => {}
                ActorTaskStatus::Stopped => return Ok(()),
                ActorTaskStatus::Failed(error) => {
                    return Err(anyhow::anyhow!(error.to_string()));
                }
            }
            self.receiver
                .changed()
                .await
                .map_err(|_| anyhow::anyhow!("actor completion channel closed"))?;
        }
    }
}

impl<A> ActorHandleInner<A>
where
    A: Actor,
{
    pub(crate) fn sender(&self) -> ActorChannelSender<A> {
        self.tx.clone()
    }

    pub fn send<M>(&self, message: M) -> Result<(), anyhow::Error>
    where
        A: ActorHandler<M, Reply = ()>,
        M: ActorMessage,
    {
        self.tx.send(message)
    }

    pub async fn request<M, R>(&self, message: M) -> Result<R, anyhow::Error>
    where
        A: ActorHandler<M, Reply = R>,
        M: ActorMessage,
        R: Send + 'static,
    {
        self.tx.request(message).await
    }
}

impl<A: Actor> ErasedActorHandle for ActorHandle<A> {
    fn stop(&self) {
        self.inner.context.task_terminator.stop();
    }

    fn exit(&self) {
        self.inner.context.exit();
    }

    fn exit_with_error(&self, error: anyhow::Error) {
        self.inner.context.exit_with_error(error);
    }

    fn completion(&self) -> ActorCompletion {
        ActorHandle::completion(self)
    }
}

// ------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ActorStopReason {
    TaskTerminatedReceived,
    ExitNotificationReceived,
    ChannelClosed,
    FailedToHandleMessage,
}

async fn run_actor<A>(
    mut actor: A,
    mut message_rx: ActorChannelReceiver<A>,
) -> Result<(), anyhow::Error>
where
    A: Actor + 'static,
{
    let mut context = actor.context().clone();
    let stop_reason: ActorStopReason;
    actor.on_start().await;
    loop {
        if context.task_terminator.is_stopped() {
            stop_reason = ActorStopReason::TaskTerminatedReceived;
            break;
        }
        match terminatable_future(&mut context, message_rx.recv()).await {
            TerminationReason::Finished(result) => match result {
                Ok(message) => {
                    if let Err(e) = message.into_inner().handle_with_actor(&mut actor).await {
                        actor.on_error(anyhow::anyhow!(
                            "[{}] Failed to handle message: {}",
                            context.name,
                            e
                        ));
                        stop_reason = ActorStopReason::FailedToHandleMessage;
                        break;
                    }
                }
                Err(_) => {
                    actor.on_error(anyhow::anyhow!(
                        "[{}] Channel closed, actor handle dropped?",
                        context.name
                    ));
                    stop_reason = ActorStopReason::ChannelClosed;
                    break;
                }
            },
            TerminationReason::TaskTerminatedReceived => {
                stop_reason = ActorStopReason::TaskTerminatedReceived;
                break;
            }
            TerminationReason::ExitNotificationReceived => {
                stop_reason = ActorStopReason::ExitNotificationReceived;
                break;
            }
        }
    }
    let stop_tasks = stop_reason != ActorStopReason::ExitNotificationReceived;
    actor.on_stop(stop_reason).await;
    context.stop_and_join_children(stop_tasks).await;
    Ok(())
}

pub enum TerminationReason<R> {
    Finished(R),
    TaskTerminatedReceived,
    ExitNotificationReceived,
}

async fn terminatable_future<'a, F, R>(
    context: &mut ActorContext,
    future: F,
) -> TerminationReason<R>
where
    F: Future<Output = R> + Send + 'a,
{
    futures::select! {
        result = future.fuse() => {
            TerminationReason::Finished(result)
        }
        _ = context.task_terminator.wait().fuse() => {
            TerminationReason::TaskTerminatedReceived
        }
        _ = context.exit_notifier.notified().fuse() => {
            TerminationReason::ExitNotificationReceived
        }
    }
}
