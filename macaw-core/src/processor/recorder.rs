use crate::lib::*;

#[derive(Debug)]
pub(crate) enum RecorderCommand {
    WriteToFile(PathBuf),
}

#[derive(Debug)]
pub struct Recorder {
    context: ActorContext,
    pub(crate) events: EventStore,
}

impl Recorder {
    pub(crate) fn new(context: ActorContext) -> Self {
        Self {
            context,
            events: EventStore::new(),
        }
    }
}

impl Processor for Recorder {}

impl Actor for Recorder {
    fn context(&self) -> &ActorContext {
        &self.context
    }
}

impl ActorHandler<RecorderCommand> for Recorder {
    type Reply = ();

    async fn handle(&mut self, request: RecorderCommand) {
        match request {
            RecorderCommand::WriteToFile(path) => {
                if let Err(e) = self.events.save_file(path).await {
                    self.context.exit_with_error(e);
                }
            }
        }
    }
}

impl ActorHandler<RecordedEvent> for Recorder {
    type Reply = ();

    async fn handle(&mut self, message: RecordedEvent) {
        self.events.push(message.proxy_id, message.event);
    }
}
