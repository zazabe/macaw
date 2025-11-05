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

    fn run_with_channel(
        self,
        context: &ActorContext,
        tx: ActorChannelSender<Self>,
        rx: ActorChannelReceiver<Self>,
    ) -> ActorHandle<Self> {
        tokio::spawn(RunActorFuture::new(self, rx, context.clone()));
        ActorHandle {
            tx,
            context: context.clone(),
        }
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

#[derive(Debug, Clone)]
pub struct ActorContext {
    terminate: TaskTerminator,
}

impl ActorContext {
    pub fn new() -> Self {
        Self {
            terminate: TaskTerminator::new(),
        }
    }

    pub fn stop(&self) {
        self.terminate.stop();
    }
}

impl Default for ActorContext {
    fn default() -> Self {
        Self::new()
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
        self.context.terminate.stop();
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

pin_project! {
    pub struct RunActorFuture {
        #[pin]
        fut: Pin<Box<dyn Future<Output = ()> + Send >>,
    }
}

impl RunActorFuture {
    fn new<A>(
        mut actor: A,
        mut message_rx: ActorChannelReceiver<A>,
        mut context: ActorContext,
    ) -> Self
    where
        A: Actor + 'static,
    {
        let fut = Box::pin(async move {
            actor.on_start(&context).await;
            loop {
                if context.terminate.is_stopped() {
                    break;
                }
                futures::select! {
                    message = message_rx.recv().fuse() => match message {
                        Ok(message) => {
                            if let Err(e) = message.into_inner().handle_with_actor(&mut actor).await {
                                error!("Failed to handle message: {}", e);
                                break;
                            }
                        }
                        Err(e) => {
                            error!("Failed to receive message: {}", e);
                            break;
                        }
                    },

                    _ = context.terminate.wait().fuse() => {
                        break;
                    }
                }
            }
            actor.on_stop(&context).await;
        });
        Self { fut }
    }
}

impl Future for RunActorFuture {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.project().fut.poll(cx)
    }
}
