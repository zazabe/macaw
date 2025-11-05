use crate::lib::*;

#[derive(Debug)]
pub(crate) enum RecorderCommand {
    WriteToFile(PathBuf),
}

#[derive(Debug)]
pub struct Recorder {
    pub(crate) events: EventStore,
}

impl Recorder {
    pub(crate) fn new() -> Self {
        Self {
            events: EventStore::new(),
        }
    }
}

impl Processor for Recorder {}

impl Actor for Recorder {}

impl ActorHandler<RecorderCommand> for Recorder {
    type Reply = Result<(), anyhow::Error>;

    async fn handle(&mut self, request: RecorderCommand) -> Result<(), anyhow::Error> {
        match request {
            RecorderCommand::WriteToFile(path) => self.events.save_file(path).await?,
        }
        Ok(())
    }
}

impl ActorHandler<Record> for Recorder {
    type Reply = ();

    async fn handle(&mut self, message: Record) {
        self.events.push(message.proxy_id, message.event);
    }
}

impl Default for Recorder {
    fn default() -> Self {
        Self::new()
    }
}
