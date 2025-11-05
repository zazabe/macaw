use std::marker::PhantomData;

use tokio::sync::oneshot;

use crate::lib::*;

pub trait ActorMessage: fmt::Debug + Send + 'static {}

impl<T> ActorMessage for T where T: fmt::Debug + Send + 'static {}

#[derive(Debug)]
pub(crate) struct Envelope<A>(Box<dyn EnvelopeMessageTrait<Actor = A> + Send>)
where
    A: Actor;

impl<A> Envelope<A>
where
    A: Actor,
{
    pub(crate) fn new_message<M>(msg: M) -> Self
    where
        M: ActorMessage,
        A: Actor + ActorHandler<M, Reply = ()>,
    {
        Self(Box::new(EnvelopeMessage {
            actor_phantom: PhantomData,
            msg,
            reply: None,
        }))
    }
    pub(crate) fn new_request<M, R>(msg: M, reply: ReplySender<R>) -> Self
    where
        M: ActorMessage,
        A: Actor + ActorHandler<M, Reply = R>,
        R: Send + 'static,
    {
        Self(Box::new(EnvelopeMessage {
            actor_phantom: PhantomData,
            msg,
            reply: Some(reply),
        }))
    }

    pub(crate) fn into_inner(self) -> Box<dyn EnvelopeMessageTrait<Actor = A> + Send> {
        self.0
    }
}

pub(crate) trait EnvelopeMessageTrait: fmt::Debug {
    type Actor: Actor;

    fn handle_with_actor<'a>(
        self: Box<Self>,
        actor: &'a mut Self::Actor,
    ) -> Pin<Box<dyn Future<Output = Result<(), anyhow::Error>> + Send + 'a>>;
}

struct EnvelopeMessage<A, M, R>
where
    M: ActorMessage,
    R: Send,
{
    actor_phantom: PhantomData<fn() -> A>,
    msg: M,
    reply: Option<ReplySender<R>>,
}

impl<A, M, R> EnvelopeMessageTrait for EnvelopeMessage<A, M, R>
where
    A: Actor + ActorHandler<M, Reply = R>,
    M: ActorMessage,
    R: Send + 'static,
{
    type Actor = A;

    fn handle_with_actor<'a>(
        self: Box<Self>,
        actor: &'a mut Self::Actor,
    ) -> Pin<Box<dyn Future<Output = Result<(), anyhow::Error>> + Send + 'a>> {
        Box::pin(async move {
            match self.reply {
                Some(reply) => {
                    let outcome = <Self::Actor as ActorHandler<M>>::handle(actor, self.msg).await;
                    reply.send(outcome)?;
                    Ok(())
                }
                None => {
                    <Self::Actor as ActorHandler<M>>::handle(actor, self.msg).await;
                    Ok(())
                }
            }
        })
    }
}

impl<A, M, R> fmt::Debug for EnvelopeMessage<A, M, R>
where
    A: Actor,
    M: ActorMessage,
    R: Send,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "EnvelopeMessage")
    }
}

// ------------------------------------------------------------

pub fn actor_channel<A>() -> (ActorChannelSender<A>, ActorChannelReceiver<A>)
where
    A: Actor,
{
    let (tx, rx) = mpsc::unbounded_channel();
    (ActorChannelSender(tx), ActorChannelReceiver(rx))
}

#[derive(Debug)]
pub struct ActorChannelSender<A: Actor>(mpsc::UnboundedSender<Envelope<A>>);

impl<A: Actor> Clone for ActorChannelSender<A> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

#[derive(Debug)]
pub struct ActorChannelReceiver<A: Actor>(mpsc::UnboundedReceiver<Envelope<A>>);

impl<A: Actor> ActorChannelReceiver<A> {
    pub(crate) async fn recv(&mut self) -> Result<Envelope<A>, anyhow::Error> {
        self.0.recv().await.ok_or(anyhow::anyhow!("Channel closed"))
    }
}
// ------------------------------------------------------------

#[dyn_clonable::clonable]
pub trait ActorSender<M>: Clone + Send + Sync + 'static
where
    M: ActorMessage,
{
    fn send(&self, message: M) -> Result<(), anyhow::Error>;
}

impl<A, M> ActorSender<M> for ActorChannelSender<A>
where
    A: Actor + ActorHandler<M, Reply = ()>,
    M: ActorMessage,
{
    fn send(&self, message: M) -> Result<(), anyhow::Error> {
        self.0
            .send(Envelope::new_message(message))
            .map_err(|e| anyhow::anyhow!("Failed to send envelope: {}", e))?;
        Ok(())
    }
}

impl<M> fmt::Debug for Box<dyn ActorSender<M>>
where
    M: ActorMessage,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Box<dyn ActorSender<M>>")
    }
}

// ------------------------------------------------------------

#[dyn_clonable::clonable]
pub trait ActorRequester<M, R>: Clone + Send + Sync + 'static
where
    M: ActorMessage,
    R: Send + 'static,
{
    fn request<'a>(
        &'a self,
        message: M,
    ) -> Pin<Box<dyn Future<Output = Result<R, anyhow::Error>> + Send + 'a>>;
}

impl<A, M, R> ActorRequester<M, R> for ActorChannelSender<A>
where
    A: Actor + ActorHandler<M, Reply = R>,
    M: ActorMessage,
    R: Send + 'static,
{
    fn request<'a>(
        &'a self,
        message: M,
    ) -> Pin<Box<dyn Future<Output = Result<R, anyhow::Error>> + Send + 'a>> {
        Box::pin(async move {
            let (reply, response) = reply_channel();
            self.0
                .send(Envelope::new_request(message, reply))
                .map_err(|e| anyhow::anyhow!("Failed to send request: {}", e))?;
            let result = response
                .recv()
                .await
                .map_err(|e| anyhow::anyhow!("Failed to receive reply: {}", e))?;
            Ok(result)
        })
    }
}

impl<M, R> fmt::Debug for Box<dyn ActorRequester<M, R>>
where
    M: ActorMessage,
    R: Send + 'static,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Box<dyn ActorRequester<M, R>>")
    }
}

// ------------------------------------------------------------

pub fn reply_channel<T>() -> (ReplySender<T>, ReplyReceiver<T>) {
    let (tx, rx) = oneshot::channel();
    (ReplySender { tx }, ReplyReceiver { rx })
}

#[derive(Debug)]
pub struct ReplyReceiver<T> {
    rx: oneshot::Receiver<T>,
}

impl<T> ReplyReceiver<T> {
    pub async fn recv(self) -> Result<T, anyhow::Error> {
        self.rx
            .await
            .map_err(|e| anyhow::anyhow!("Failed to receive reply: {}", e))
    }
}

#[derive(Debug)]
pub struct ReplySender<T> {
    tx: oneshot::Sender<T>,
}

impl<T> ReplySender<T> {
    pub fn send(self, value: T) -> Result<(), anyhow::Error> {
        self.tx
            .send(value)
            .map_err(|e| anyhow::anyhow!("Failed to send reply"))
    }
}
