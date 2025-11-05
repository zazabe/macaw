use crate::lib::*;

pub trait Processor: Actor {}

pub trait ProxyActor: Actor {
    fn id(&self) -> ProxyId;
}

pub struct Macaw<P: Processor> {
    context: ActorContext,
    processor: ActorHandle<P>,
    proxies: Vec<Box<dyn ErasedActorHandle>>,
}

impl Macaw<Recorder> {
    pub fn recorder() -> Self {
        let context = ActorContext::new();
        let processor = Recorder::new();
        let handle = processor.run(&context);
        Self {
            context,
            processor: handle,
            proxies: Vec::new(),
        }
    }

    pub async fn record(self, path: PathBuf) -> Result<(), anyhow::Error> {
        self.processor
            .request(RecorderCommand::WriteToFile(path))
            .await?
    }

    pub fn add_proxy_with_channel<P: ProxyActor>(
        &mut self,
        proxy: P,
        rx: ActorChannelReceiver<P>,
        tx: ActorChannelSender<P>,
    ) -> Result<(), anyhow::Error> {
        let handle = proxy.run_with_channel(&self.context, tx, rx);
        self.proxies.push(Box::new(handle));
        Ok(())
    }
}

impl Macaw<Replayer> {
    pub fn replayer<P: AsRef<Path>>(path: P) -> Result<Self, anyhow::Error> {
        let context = ActorContext::new();
        let processor = Replayer::new(path)?;
        let handle = processor.run(&context);
        Ok(Self {
            context,
            processor: handle,
            proxies: Vec::new(),
        })
    }

    pub async fn play(self) -> Result<(), anyhow::Error> {
        self.processor.request(ReplayerCommand::Play).await?
    }

    pub async fn add_proxy_with_channel<P>(
        &mut self,
        proxy: P,
        rx: ActorChannelReceiver<P>,
        tx: ActorChannelSender<P>,
    ) -> Result<(), anyhow::Error>
    where
        P: ProxyActor + ActorHandler<Record, Reply = Result<(), anyhow::Error>>,
    {
        let proxy_id = proxy.id();
        let handle = proxy.run_with_channel(&self.context, tx, rx);
        let sender = Box::new(handle.sender());
        self.processor
            .request(ReplayerCommand::RegisterProxy(proxy_id, sender))
            .await??;
        self.proxies.push(Box::new(handle));
        Ok(())
    }
}

impl<Proc: Processor> Macaw<Proc> {
    pub fn processor_handle(&self) -> ActorHandle<Proc> {
        self.processor.clone()
    }
}
