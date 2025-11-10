use crate::lib::*;

pub trait Processor: Actor {}

pub trait ProxyActor: Actor {
    fn id(&self) -> ProxyId;
}

pub struct Macaw<P: Processor> {
    app_context: AppContext,
    processor: ActorHandle<P>,
    proxies: Vec<Box<dyn ErasedActorHandle>>,
}

impl Macaw<Recorder> {
    pub fn recorder(app_context: AppContext) -> Self {
        let actor_context = app_context.actor_context();
        let processor = Recorder::new();
        let handle = processor.run("macaw:recorder", &actor_context);
        Self {
            app_context,
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
        let proxy_id = proxy.id();
        let proxy_name = proxy_id.to_string();
        let actor_context = self.app_context.actor_context();
        let handle = proxy.run_with_channel(&proxy_name, &actor_context, tx, rx);
        self.proxies.push(Box::new(handle));
        Ok(())
    }
}

impl Macaw<Replayer> {
    pub fn replayer<P: AsRef<Path>>(
        app_context: AppContext,
        path: P,
    ) -> Result<Self, anyhow::Error> {
        let actor_context = app_context.actor_context();
        let processor = Replayer::new(path)?;
        let handle = processor.run("macaw:replayer", &actor_context);
        Ok(Self {
            app_context,
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
        P: ProxyActor + ActorHandler<RecordedEventWithLock, Reply = ()>,
    {
        let proxy_id = proxy.id();
        let proxy_name = proxy_id.to_string();
        let actor_context = self.app_context.actor_context();
        let handle = proxy.run_with_channel(&proxy_name, &actor_context, tx, rx);
        self.proxies.push(Box::new(handle));
        Ok(())
    }
}

impl<Proc: Processor> Macaw<Proc> {
    pub fn processor_handle(&self) -> ActorHandle<Proc> {
        self.processor.clone()
    }
}
