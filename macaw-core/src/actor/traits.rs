use std::marker::PhantomData;

use pin_project_lite::pin_project;
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
        debug!("Actor stopped: {}", self.context().name);
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
            .spawn(run_actor(self, rx));

        ActorHandle::new(tx, context)
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
pub(crate) struct AppContext {
    terminate: AppTerminator,
}

impl AppContext {
    pub(crate) fn new() -> Self {
        Self {
            terminate: AppTerminator::new(),
        }
    }

    pub(crate) fn actor_context(&self, name: &str) -> ActorContext {
        let notifier = self.terminate.exit_handle();
        ActorContext::new(name, notifier)
    }

    pub(crate) fn exit_handle(&self) -> AppExitHandle {
        self.terminate.exit_handle()
    }

    pub(crate) fn exit(&self) {
        self.terminate.exit();
    }

    pub(crate) fn exit_with_error(&self, error: anyhow::Error) {
        self.terminate.exit_with_error(error);
    }

    pub(crate) async fn wait_until_exit(self) -> Result<(), AppError> {
        self.terminate.wait_until_exit().await
    }
}

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
}

impl ActorContext {
    fn new(name: &str, exit_notifier: AppExitHandle) -> Self {
        Self {
            name: name.to_string(),
            exit_notifier,
            task_terminator: TaskTerminator::new(),
        }
    }

    pub fn create_child(&self, name: &str) -> Self {
        Self {
            name: self.name.clone() + ":" + name,
            exit_notifier: self.exit_notifier.clone(),
            task_terminator: TaskTerminator::new(),
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
        let handle = tokio::task::Builder::new().name(&name).spawn({
            let mut context = self.clone();
            async move { terminatable_future(&mut context, future).await }
        })?;
        Ok(handle)
    }
}

// ------------------------------------------------------------

pub trait ErasedActorHandle: Send {
    fn stop(&self);

    fn exit(&self);

    fn exit_with_error(&self, error: anyhow::Error);
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
    pub fn new(tx: ActorChannelSender<A>, context: ActorContext) -> Self {
        Self {
            inner: Arc::new(ActorHandleInner { tx, context }),
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
                Err(e) => {
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
    actor.on_stop(stop_reason).await;
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
