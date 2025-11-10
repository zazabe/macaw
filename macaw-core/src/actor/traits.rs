use std::marker::PhantomData;

use pin_project_lite::pin_project;
use tokio::task::JoinHandle;

use crate::lib::*;
use futures::FutureExt;

pub trait Actor: Send + Sized + 'static {
    fn on_start(&mut self, context: &ActorContext) -> impl Future<Output = ()> + Send {
        futures::future::ready(())
    }

    fn on_stop(&mut self, context: &ActorContext) -> impl Future<Output = ()> + Send {
        futures::future::ready(())
    }

    fn on_error(&mut self, context: &ActorContext, error: anyhow::Error) {
        context.exit_with_error(error);
    }

    fn run_with_channel(
        self,
        name: &str,
        context: &ActorContext,
        tx: ActorChannelSender<Self>,
        rx: ActorChannelReceiver<Self>,
    ) -> ActorHandle<Self> {
        let task =
            tokio::task::Builder::new()
                .name(name)
                .spawn(run_actor(self, rx, context.clone()));

        ActorHandle {
            tx,
            context: context.clone(),
        }
    }

    fn run(self, name: &str, context: &ActorContext) -> ActorHandle<Self> {
        let (tx, rx) = actor_channel::<Self>();
        self.run_with_channel(name, context, tx, rx)
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

#[derive(Debug, Clone)]
pub struct AppContext {
    terminate: AppTerminator,
}

impl AppContext {
    pub fn new() -> Self {
        Self {
            terminate: AppTerminator::new(),
        }
    }

    pub fn actor_context(&self) -> ActorContext {
        ActorContext::new(self.terminate.clone())
    }

    pub fn exit(&mut self) {
        self.terminate.exit();
    }

    pub fn exit_with_error(&mut self, error: anyhow::Error) {
        self.terminate.exit_with_error(error);
    }

    pub async fn wait_until_stopped(&mut self) {
        self.terminate.wait_until_stopped().await;
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
    app_terminator: AppTerminator,
    task_terminator: TaskTerminator,
}

impl ActorContext {
    fn new(app_terminator: AppTerminator) -> Self {
        Self {
            app_terminator,
            task_terminator: TaskTerminator::new(),
        }
    }
    pub fn exit(&self) {
        self.app_terminator.exit();
    }

    pub fn exit_with_error(&self, error: anyhow::Error) {
        self.app_terminator.exit_with_error(error);
    }

    pub fn stop(&self) {
        self.task_terminator.stop();
    }
}

// ------------------------------------------------------------

pub trait ErasedActorHandle {
    fn stop(&self);
}

#[derive(Debug)]
pub struct ActorHandle<A>
where
    A: Actor,
{
    tx: ActorChannelSender<A>,
    context: ActorContext,
}

impl<A> ActorHandle<A>
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
        self.context.task_terminator.stop();
    }
}

impl<A: Actor> Clone for ActorHandle<A> {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
            context: self.context.clone(),
        }
    }
}

// ------------------------------------------------------------

async fn run_actor<A>(
    mut actor: A,
    mut message_rx: ActorChannelReceiver<A>,
    mut context: ActorContext,
) -> Result<(), anyhow::Error>
where
    A: Actor + 'static,
{
    actor.on_start(&context).await;
    loop {
        if context.task_terminator.is_stopped() {
            break;
        }
        futures::select! {
            message = message_rx.recv().fuse() => match message {
                Ok(message) => {
                    if let Err(e) = message.into_inner().handle_with_actor(&mut actor).await {
                        actor.on_error(&context, e);
                        break;
                    }
                }
                Err(e) => {
                    actor.on_error(&context, e);
                    break;
                }
            },

            _ = context.task_terminator.wait().fuse() => {
                break;
            }

            _ = context.app_terminator.wait_until_stopped().fuse() => {
                break;
            }
        }
    }
    actor.on_stop(&context).await;
    Ok(())
}
