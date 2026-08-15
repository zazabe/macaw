use crate::lib::*;

pub trait Processor: Actor {}

pub trait ProxyActor: Actor {
    fn proxy_id(&self) -> ProxyId;
}

#[derive(Debug)]
pub struct Macaw<P: Processor> {
    /// Lifecycle observed by callers. It is deliberately separate from the actor
    /// system so a graceful stop can drain proxies before stopping the processor.
    context: AppContext,
    actors: AppContext,
    processor: ActorHandle<P>,
    proxies: ProxyHandles,
}

impl<P: Processor> Macaw<P> {
    pub fn exit_handle(&self) -> AppExitHandle {
        self.context.exit_handle()
    }

    async fn wait_for_stop_request(
        context: AppContext,
        actors: AppContext,
    ) -> Result<(), AppError> {
        tokio::select! {
            result = context.wait_until_exit() => result,
            result = actors.wait_until_exit() => result,
        }
    }

    /// Stop all runtime actors without waiting for an application exit request.
    /// This is primarily used to roll back partially completed startup.
    pub async fn shutdown(self) -> Result<(), AppError> {
        let Self {
            context: _,
            actors: _,
            processor,
            mut proxies,
        } = self;
        proxies.stop_and_wait().await?;
        processor.stop();
        processor.wait().await?;
        Ok(())
    }
}

impl Macaw<Recorder> {
    pub fn recorder() -> Self {
        Self::recorder_with_options(RecorderOptions::default())
    }

    pub fn recorder_with_options(options: RecorderOptions) -> Self {
        let context = AppContext::new();
        let actors = AppContext::new();
        let actor_context = actors.actor_context("recorder");
        let processor = Recorder::new(actor_context, options);
        let handle = processor.run();
        Self {
            context,
            actors,
            processor: handle,
            proxies: ProxyHandles::new(),
        }
    }

    pub async fn record_when_exit<P: AsRef<Path>>(
        self,
        path: P,
    ) -> Result<RecorderOutcome, AppError> {
        let Self {
            context,
            actors,
            processor,
            mut proxies,
        } = self;
        let stop_result = Self::wait_for_stop_request(context, actors).await;

        // Stop listeners first. Once their actor mailboxes are closed, every
        // previously accepted event has either reached the recorder mailbox or
        // can no longer be produced.
        proxies.stop_and_wait().await?;

        if let Err(error) = stop_result {
            processor.stop();
            processor.wait().await?;
            return Err(error);
        }

        // Actor mailboxes are FIFO, so this request is handled after all events
        // already sent by the stopped proxies. This is the recording drain point.
        let stats = processor
            .request(RecorderCommand::WriteToFile(path.as_ref().to_path_buf()))
            .await??;
        processor.stop();
        processor.wait().await?;
        Ok(stats)
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
        Self::replayer_with_options(path, ReplayerOptions::default())
    }

    pub fn replayer_with_options<P: AsRef<Path>>(
        path: P,
        options: ReplayerOptions,
    ) -> Result<Self, anyhow::Error> {
        let context = AppContext::new();
        let actors = AppContext::new();
        let actor_context = actors.actor_context("replayer");
        let processor = Replayer::new(actor_context, path, options)?;
        let handle = processor.run();
        Ok(Self {
            context,
            actors,
            processor: handle,
            proxies: ProxyHandles::new(),
        })
    }

    pub fn play(&self) -> Result<(), anyhow::Error> {
        self.processor.send(ReplayerCommand::Play)
    }

    pub async fn wait_until_stopped(self) -> Result<(), AppError> {
        let Self {
            context,
            actors,
            processor,
            mut proxies,
        } = self;
        let stop_result = Self::wait_for_stop_request(context, actors).await;
        proxies.stop_and_wait().await?;
        processor.stop();
        processor.wait().await?;
        stop_result
    }

    pub async fn add_proxy<A, F>(
        &mut self,
        f: F,
    ) -> Result<(ProxyId, ActorHandle<A>), anyhow::Error>
    where
        A: ProxyActor + ActorHandler<RecordedEventWithLock, Reply = ()>,
        F: FnOnce(
            &ActorHandle<Replayer>,
            ActorContext,
        ) -> Result<(ProxyId, ActorHandle<A>), anyhow::Error>,
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

    async fn stop_and_wait(&mut self) -> Result<(), anyhow::Error> {
        for handle in self.0.values() {
            handle.stop();
        }
        let completions = self
            .0
            .values()
            .map(|handle| handle.completion())
            .collect::<Vec<_>>();
        for completion in completions {
            completion.wait().await?;
        }
        self.0.clear();
        Ok(())
    }
}

impl Default for ProxyHandles {
    fn default() -> Self {
        Self::new()
    }
}
