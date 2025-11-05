use crate::lib::*;

#[derive(Debug)]
pub(crate) enum ReplayerCommand {
    Play,
    RegisterProxy(ProxyId, ProxyRequester),
}

type ProxyRequester = Box<dyn ActorRequester<Record, Result<(), anyhow::Error>>>;

#[derive(Debug)]
pub struct Replayer {
    events: EventStore,
    proxies: HashMap<ProxyId, ProxyRequester>,
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
            let proxy = self
                .proxies
                .get(&proxy_id)
                .ok_or(anyhow::anyhow!("Proxy not found"))?;
            proxy
                .request(Record {
                    proxy_id,
                    event: data,
                })
                .await
                .map_err(|e| anyhow::anyhow!("Failed to send event to proxy: {}", e))??;
        }
        Ok(())
    }
}
