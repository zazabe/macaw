use crate::lib::*;

#[derive(Debug)]
pub(crate) enum RecorderCommand {
    WriteToFile(PathBuf),
}

#[derive(Debug)]
pub struct RecorderOutcome {
    pub recording_path: PathBuf,
    pub total_events: usize,
    pub total_bytes: Option<usize>,
    pub total_time: Option<Duration>,
}

/// Options for the Recorder, including optional debug sink.
#[derive(Debug, Default)]
pub struct RecorderOptions {
    /// When set, each recorded event is cloned and sent here before storing.
    pub debug_tx: Option<mpsc::UnboundedSender<RecordedEvent>>,
}

#[derive(Debug)]
pub struct Recorder {
    context: ActorContext,
    pub(crate) events: EventStore,
    debug_tx: Option<mpsc::UnboundedSender<RecordedEvent>>,
}

impl Recorder {
    pub(crate) fn new(context: ActorContext, options: RecorderOptions) -> Self {
        Self {
            context,
            events: EventStore::new(),
            debug_tx: options.debug_tx,
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
    type Reply = Result<RecorderOutcome, anyhow::Error>;

    async fn handle(&mut self, request: RecorderCommand) -> Self::Reply {
        match request {
            RecorderCommand::WriteToFile(path) => {
                self.events.save_file(&path).await?;
                let total_bytes = path.metadata().ok().map(|m| m.len() as usize);
                Ok(RecorderOutcome {
                    recording_path: path,
                    total_bytes,
                    total_events: self.events.events_count(),
                    total_time: self.events.duration(),
                })
            }
        }
    }
}

impl ActorHandler<RecordedEvent> for Recorder {
    type Reply = ();

    async fn handle(&mut self, message: RecordedEvent) {
        if let Some(ref tx) = self.debug_tx {
            let _ = tx.send(message.clone());
        }
        self.events.push(message);
    }
}
