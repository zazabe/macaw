use crate::lib::*;

#[derive(Debug)]
pub(crate) enum ReplayerCommand {
    Play,
    RegisterProxy(ProxyId, ProxySender),
}

type ProxySender = Box<dyn ActorSender<RecordedEventWithLock>>;

#[derive(Debug)]
pub struct Replayer {
    events: EventStore,
    proxies: HashMap<ProxyId, ProxySender>,
    context: ActorContext,
}

impl Replayer {
    pub(crate) fn new<P: AsRef<Path>>(
        context: ActorContext,
        path: P,
    ) -> Result<Self, anyhow::Error> {
        Ok(Self {
            context,
            events: EventStore::from_file(path.as_ref())?,
            proxies: HashMap::new(),
        })
    }
}

impl Processor for Replayer {}

impl Actor for Replayer {
    fn context(&self) -> &ActorContext {
        &self.context
    }
}

impl ActorHandler<ReplayerCommand> for Replayer {
    type Reply = ();

    async fn handle(&mut self, request: ReplayerCommand) {
        debug!("Replayer - Handling command: {:?}", request);
        match request {
            ReplayerCommand::Play => {
                if let Err(e) = self.play().await {
                    self.context.exit_with_error(e);
                }
            }
            ReplayerCommand::RegisterProxy(proxy_id, proxy_sender) => {
                self.proxies.insert(proxy_id, proxy_sender);
            }
        }
    }
}

impl Replayer {
    async fn play(&mut self) -> Result<(), anyhow::Error> {
        for event in self.events.iter() {
            let Event { proxy_id, data, .. } = event;
            let (replay_lock_holder, replay_lock) = lock_channel();
            let proxy = self.proxies.get(&proxy_id).ok_or(anyhow::anyhow!(
                "Proxy {:?} not found in {:?}",
                proxy_id,
                self.proxies
            ))?;
            proxy.send(RecordedEventWithLock {
                proxy_id,
                event: data,
                replay_lock: replay_lock_holder,
            })?;
            replay_lock.wait().await;
        }
        self.context.stop();
        self.context.exit();
        Ok(())
    }
}
