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
}

impl Replayer {
    pub(crate) fn new<P: AsRef<Path>>(path: P) -> Result<Self, anyhow::Error> {
        Ok(Self {
            events: EventStore::from_file(path.as_ref())?,
            proxies: HashMap::new(),
        })
    }
}

impl Processor for Replayer {}

impl Actor for Replayer {}

impl ActorHandler<ReplayerCommand> for Replayer {
    type Reply = Result<(), anyhow::Error>;

    async fn handle(&mut self, request: ReplayerCommand) -> Result<(), anyhow::Error> {
        match request {
            ReplayerCommand::Play => self.play().await?,
            ReplayerCommand::RegisterProxy(proxy_id, proxy_sender) => {
                self.proxies.insert(proxy_id, proxy_sender);
            }
        }
        Ok(())
    }
}

impl Replayer {
    async fn play(&mut self) -> Result<(), anyhow::Error> {
        while let Some(event) = self.events.next() {
            let Event { proxy_id, data, .. } = event;
            let (replay_lock_holder, replay_lock) = lock_channel();
            let proxy = self
                .proxies
                .get(&proxy_id)
                .ok_or(anyhow::anyhow!("Proxy not found"))?;
            proxy.send(RecordedEventWithLock {
                proxy_id,
                event: data,
                replay_lock: replay_lock_holder,
            })?;
            replay_lock.wait().await;
        }
        Ok(())
    }
}
