use crate::lib::*;

pub trait Processor: Actor {}

pub trait ProxyActor: Actor {
    fn id(&self) -> ProxyId;
}

pub struct Macaw<P: Processor> {
    actor_context: ActorContext,
    processor: ActorHandle<P>,
    proxies: Vec<Box<dyn ErasedActorHandle>>,
}

impl Macaw<Recorder> {
    pub fn recorder(actor_context: ActorContext) -> Self {
        let processor = Recorder::new(actor_context.clone());
        let handle = processor.run(&actor_context);
        Self {
            actor_context,
            processor: handle,
            proxies: Vec::new(),
        }
    }

    pub async fn record(self, path: PathBuf) -> Result<(), anyhow::Error> {
        self.processor
            .request(RecorderCommand::WriteToFile(path))
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
        let actor_context = self.actor_context.create_child();
        let handle = proxy.run_with_channel(&actor_context, tx, rx);
        self.proxies.push(Box::new(handle));
        Ok(())
    }
}

impl Macaw<Replayer> {
    pub fn replayer<P: AsRef<Path>>(
        actor_context: ActorContext,
        path: P,
    ) -> Result<Self, anyhow::Error> {
        let processor = Replayer::new(actor_context.clone(), path)?;
        let handle = processor.run(&actor_context);
        Ok(Self {
            actor_context,
            processor: handle,
            proxies: Vec::new(),
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
        let actor_context = self.actor_context.create_child();
        let handle = proxy.run_with_channel(&actor_context, tx, rx);
        self.proxies.push(Box::new(handle));
        self.processor
            .request(ReplayerCommand::RegisterProxy(proxy_id, proxy_sender))
            .await?;
        Ok(())
    }
}

impl<Proc: Processor> Macaw<Proc> {
    pub fn processor_handle(&self) -> ActorHandle<Proc> {
        self.processor.clone()
    }
}
