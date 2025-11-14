use std::marker::PhantomData;

use pin_project_lite::pin_project;
use tokio::task::JoinHandle;

use crate::lib::*;
use futures::FutureExt;

pub trait Actor: Send + Sized + 'static {
    fn name(&self) -> &str;

    fn on_start(&mut self, context: &ActorContext) -> impl Future<Output = ()> + Send {
        debug!("Actor starting: {}", self.name());
        futures::future::ready(())
    }

    fn on_stop(
        &mut self,
        context: &ActorContext,
        stop_reason: Option<ActorStopReason>,
    ) -> impl Future<Output = ()> + Send {
        futures::future::ready(())
    }

    fn on_error(&mut self, context: &ActorContext, error: anyhow::Error) {
        error!("Actor stopped with error: {}", self.name());
        context.exit_with_error(error);
    }

    fn run_with_channel(
        self,
        context: &ActorContext,
        tx: ActorChannelSender<Self>,
        rx: ActorChannelReceiver<Self>,
    ) -> ActorHandle<Self> {
        let name = self.name().to_owned();
        let task =
            tokio::task::Builder::new()
                .name(&name)
                .spawn(run_actor(self, rx, context.clone()));

        ActorHandle::new(tx, context.clone())
    }

    fn run(self, context: &ActorContext) -> ActorHandle<Self> {
        let (tx, rx) = actor_channel::<Self>();
        self.run_with_channel(context, tx, rx)
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

    pub(crate) fn actor_context(&self) -> ActorContext {
        let notifier = self.terminate.exit_handle();
        ActorContext::new(notifier)
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
    exit_notifier: AppExitHandle,
    task_terminator: TaskTerminator,
}

impl ActorContext {
    fn new(exit_notifier: AppExitHandle) -> Self {
        Self {
            exit_notifier,
            task_terminator: TaskTerminator::new(),
        }
    }

    pub fn create_child(&self) -> Self {
        Self {
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
}

// ------------------------------------------------------------

pub trait ErasedActorHandle: Send {
    fn stop(&self);

    fn exit(&self);
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
}

// ------------------------------------------------------------

#[derive(Debug)]
pub enum ActorStopReason {
    TaskTerminatedReceived,
    ExitNotificationReceived,
    ChannelClosed,
    FailedToHandleMessage,
}

async fn run_actor<A>(
    mut actor: A,
    mut message_rx: ActorChannelReceiver<A>,
    mut context: ActorContext,
) -> Result<(), anyhow::Error>
where
    A: Actor + 'static,
{
    #[allow(unused_assignments)]
    let mut stop_reason = None;
    actor.on_start(&context).await;
    loop {
        if context.task_terminator.is_stopped() {
            stop_reason = Some(ActorStopReason::TaskTerminatedReceived);
            break;
        }
        futures::select! {
            message = message_rx.recv().fuse() => match message {
                Ok(message) => {
                    if let Err(e) = message.into_inner().handle_with_actor(&mut actor).await {
                        actor.on_error(&context, e);
                        stop_reason = Some(ActorStopReason::FailedToHandleMessage);
                        break;
                    }
                }
                Err(e) => {
                    actor.on_error(&context, e);
                    stop_reason = Some(ActorStopReason::ChannelClosed);
                    break;
                }
            },

            _ = context.task_terminator.wait().fuse() => {
                stop_reason = Some(ActorStopReason::TaskTerminatedReceived);
                break;
            }

            _ = context.exit_notifier.notified().fuse() => {
                stop_reason = Some(ActorStopReason::ExitNotificationReceived);
                break;
            }
        }
    }
    actor.on_stop(&context, stop_reason).await;
    Ok(())
}
