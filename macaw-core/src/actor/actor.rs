use std::marker::PhantomData;

use pin_project_lite::pin_project;
use tokio::task::JoinHandle;

use crate::lib::*;
use futures::FutureExt;

pub trait ActorMessage: fmt::Debug + Send + 'static {}

impl<T> ActorMessage for T where T: fmt::Debug + Send + 'static {}

#[derive(Debug)]
struct Envelope<A>(Box<dyn EnvelopeMessageTrait<Actor = A> + Send>)
where
    A: Actor;

impl<A: Actor> Envelope<A> {
    fn new_message<M>(msg: M) -> Self
    where
        M: ActorMessage,
        A: Actor + ActorHandler<M> + 'static,
    {
        Self(Box::new(EnvelopeMessage {
            actor_phantom: PhantomData,
            msg,
        }))
    }

    fn into_inner(self) -> Box<dyn EnvelopeMessageTrait<Actor = A> + Send> {
        self.0
    }
}

trait EnvelopeMessageTrait: fmt::Debug {
    type Actor: Actor;

    fn handle_with_actor<'a>(
        self: Box<Self>,
        actor: &'a mut Self::Actor,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>>;
}

struct EnvelopeMessage<A, M>
where
    M: ActorMessage,
{
    actor_phantom: PhantomData<fn() -> A>,
    msg: M,
}

impl<A, M> EnvelopeMessageTrait for EnvelopeMessage<A, M>
where
    A: Actor + ActorHandler<M>,
    M: ActorMessage,
{
    type Actor = A;

    fn handle_with_actor<'a>(
        self: Box<Self>,
        actor: &'a mut Self::Actor,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
            <Self::Actor as ActorHandler<M>>::handle(actor, self.msg).await;
        })
    }
}

impl<A, M> fmt::Debug for EnvelopeMessage<A, M>
where
    A: Actor,
    M: ActorMessage,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "EnvelopeMessage")
    }
}

pub trait Actor: Send + Sized + 'static {
    fn on_start(&mut self, context: &ActorContext) -> impl Future<Output = ()> + Send {
        futures::future::ready(())
    }

    fn on_stop(&mut self, context: &ActorContext) -> impl Future<Output = ()> + Send {
        futures::future::ready(())
    }

    fn run(self, context: &ActorContext) -> ActorHandle<Self> {
        let (message_tx, message_rx) = mpsc::unbounded_channel();

        tokio::spawn(RunActorFuture::new(self, message_rx, context.clone()));

        ActorHandle {
            tx: message_tx,
            context: context.clone(),
        }
    }
}

pub trait ActorHandler<M>: Send + 'static
where
    M: ActorMessage,
{
    fn handle(&mut self, msg: M) -> impl Future<Output = ()> + Send;
}

pub struct ActorHandle<A>
where
    A: Actor,
{
    tx: mpsc::UnboundedSender<Envelope<A>>,
    context: ActorContext,
}

impl<A> ActorHandle<A>
where
    A: Actor,
{
    pub fn send<M>(&self, message: M) -> Result<(), anyhow::Error>
    where
        A: ActorHandler<M>,
        M: ActorMessage,
    {
        self.tx
            .send(Envelope::new_message(message))
            .map_err(|e| anyhow::anyhow!("Failed to send message: {}", e))?;
        Ok(())
    }

    pub fn stop(&self) {
        self.context.terminate.stop();
    }
}

#[derive(Clone)]
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

pin_project! {
    pub struct RunActorFuture {
        #[pin]
        fut: Pin<Box<dyn Future<Output = ()> + Send >>,
    }
}

impl RunActorFuture {
    fn new<A>(
        mut actor: A,
        mut message_rx: mpsc::UnboundedReceiver<Envelope<A>>,
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
                        Some(message) => {
                            message.into_inner().handle_with_actor(&mut actor).await;
                        }
                        None => {
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
