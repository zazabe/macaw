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
}

impl Macaw<Recorder> {
    pub fn recorder() -> Self {
        let context = AppContext::new();
        let actor_context = context.actor_context("recorder");
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

    pub fn add_proxy<P, F>(&mut self, f: F) -> Result<(ProxyId, ActorHandle<P>), anyhow::Error>
    where
        P: ProxyActor,
        F: FnOnce(
            &ActorHandle<Recorder>,
            ActorContext,
        ) -> Result<(ProxyId, ActorHandle<P>), anyhow::Error>,
    {
        let actor_context = self.context.actor_context("proxy");
        let (proxy_id, handle) = f(&self.processor, actor_context)?;
        self.proxies.insert(proxy_id, handle.clone());
        Ok((proxy_id, handle))
    }
}

impl Macaw<Replayer> {
    pub fn replayer<P: AsRef<Path>>(path: P) -> Result<Self, anyhow::Error> {
        let context = AppContext::new();
        let actor_context = context.actor_context("replayer");
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

    pub async fn add_proxy<P, F>(
        &mut self,
        f: F,
    ) -> Result<(ProxyId, ActorHandle<P>), anyhow::Error>
    where
        P: ProxyActor + ActorHandler<RecordedEventWithLock, Reply = ()>,
        F: FnOnce(
            &ActorHandle<Replayer>,
            ActorContext,
        ) -> Result<(ProxyId, ActorHandle<P>), anyhow::Error>,
    {
        let actor_context = self.context.actor_context("proxy");
        let (proxy_id, handle) = f(&self.processor, actor_context)?;
        let sender = handle.sender();
        self.proxies.insert(proxy_id, handle.clone());
        self.processor
            .request(ReplayerCommand::RegisterProxy(proxy_id, Box::new(sender)))
            .await?;
        Ok((proxy_id, handle))
    }
}

// ------------------------------------------------------------

#[derive(Debug)]
struct ProxyHandles(HashMap<ProxyId, Box<dyn ErasedActorHandle>>);

impl ProxyHandles {
    fn new() -> Self {
        Self(HashMap::new())
    }

    fn insert<H: ErasedActorHandle + 'static>(&mut self, proxy_id: ProxyId, handle: H) {
        self.0.insert(proxy_id, Box::new(handle));
    }
}

impl Default for ProxyHandles {
    fn default() -> Self {
        Self::new()
    }
}
