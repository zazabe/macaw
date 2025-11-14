use crate::lib::*;

pub trait Processor: Actor {}

pub trait ProxyActor: Actor {
    fn id(&self) -> ProxyId;
}

#[derive(Debug)]
pub struct Macaw<P: Processor> {
    context: AppContext,
    processor: ActorHandle<P>,
    proxies: ProxyHandles,
}

impl<P: Processor> Macaw<P> {
    pub fn exit_handle(&self) -> AppExitHandle {
        self.context.exit_handle()
    }

    pub async fn wait_until_stopped(self) -> Result<(), AppError> {
        self.context.wait_until_exit().await
    }

    pub fn processor_handle(&self) -> ActorHandle<P> {
        self.processor.clone()
    }
}

impl Macaw<Recorder> {
    pub fn recorder() -> Self {
        let context = AppContext::new();
        let actor_context = context.actor_context();
        let processor = Recorder::new(actor_context.clone());
        let handle = processor.run(&actor_context);
        Self {
            context,
            processor: handle,
            proxies: ProxyHandles::new(),
        }
    }

    pub async fn record_when_exit<P: AsRef<Path>>(self, path: P) -> Result<(), AppError> {
        self.context.wait_until_exit().await?;
        self.processor
            .request(RecorderCommand::WriteToFile(path.as_ref().to_path_buf()))
            .await?;
        Ok(())
    }

    pub fn add_proxy_with_channel<P: ProxyActor>(
        &mut self,
        proxy: P,
        rx: ActorChannelReceiver<P>,
        tx: ActorChannelSender<P>,
    ) -> Result<(), anyhow::Error> {
        let proxy_id = proxy.id();
        let actor_context = self.context.actor_context();
        let handle = proxy.run_with_channel(&actor_context, tx, rx);
        self.proxies.push(handle);
        Ok(())
    }
}

impl Macaw<Replayer> {
    pub fn replayer<P: AsRef<Path>>(path: P) -> Result<Self, anyhow::Error> {
        let context = AppContext::new();
        let actor_context = context.actor_context();
        let processor = Replayer::new(actor_context.clone(), path)?;
        let handle = processor.run(&actor_context);
        Ok(Self {
            context,
            processor: handle,
            proxies: ProxyHandles::new(),
        })
    }

    pub fn play(&self) -> Result<(), anyhow::Error> {
        self.processor.send(ReplayerCommand::Play)
    }

    pub async fn add_proxy_with_channel<P>(
        &mut self,
        proxy: P,
        rx: ActorChannelReceiver<P>,
        tx: ActorChannelSender<P>,
    ) -> Result<(), anyhow::Error>
    where
        P: ProxyActor + ActorHandler<RecordedEventWithLock, Reply = ()>,
    {
        let proxy_id = proxy.id();
        let proxy_sender = Box::new(tx.clone());
        let actor_context = self.context.actor_context();
        let handle = proxy.run_with_channel(&actor_context, tx, rx);
        self.proxies.push(handle);
        self.processor
            .request(ReplayerCommand::RegisterProxy(proxy_id, proxy_sender))
            .await?;
        Ok(())
    }
}

// ------------------------------------------------------------

#[derive(Debug)]
struct ProxyHandles(Vec<Box<dyn ErasedActorHandle>>);

impl ProxyHandles {
    fn new() -> Self {
        Self(Vec::new())
    }

    fn push<H: ErasedActorHandle + 'static>(&mut self, handle: H) {
        self.0.push(Box::new(handle));
    }
}

impl Default for ProxyHandles {
    fn default() -> Self {
        Self::new()
    }
}
