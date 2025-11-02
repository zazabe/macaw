use crate::{
    model::RecordEvent,
    processor::{Message, proxy::Proxy},
    support::{TaskExecutor, TokioTask},
};
use anyhow::Result;
use std::path::PathBuf;
use tokio::sync::{mpsc, oneshot};

pub struct MacawSetup<Exec, Proc>
where
    Exec: TaskExecutor + 'static,
    Proc: Processor + 'static,
{
    pub(crate) executor: Exec,
    pub(crate) processor: Proc,
}

impl<Exec, Proc> MacawSetup<Exec, Proc>
where
    Exec: TaskExecutor + 'static,
    Proc: Processor + 'static,
{
    pub fn new(executor: Exec, processor: Proc) -> Self {
        Self {
            executor,
            processor,
        }
    }

    pub fn start(self) -> Macaw<Proc::Command> {
        let Self {
            executor,
            mut processor,
        } = self;
        let (tx, rx) = mpsc::unbounded_channel();
        let task = executor.execute(Box::pin(async move {
            processor.start(rx).await?;
            Ok(())
        }));
        Macaw::new(task, tx)
    }

    pub fn processor(&mut self) -> &mut Proc {
        &mut self.processor
    }

    pub fn executor(&self) -> Exec {
        self.executor.clone()
    }
}

pub struct Macaw<Command> {
    task: Option<TokioTask>,
    tx: mpsc::UnboundedSender<MacawCommand<Command>>,
}

impl<Command> Macaw<Command> {
    fn new(task: TokioTask, tx: mpsc::UnboundedSender<MacawCommand<Command>>) -> Self {
        Self {
            task: Some(task),
            tx,
        }
    }

    pub fn stop(&mut self) {
        if let Some(task) = self.task.take() {
            task.cancel();
        }
    }
}

impl Macaw<RecordCommand> {
    pub async fn record(&mut self, path: PathBuf) -> Result<(), anyhow::Error> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(MacawCommand::record(tx, path))?;
        rx.await?
    }
}

impl<Command> Drop for Macaw<Command> {
    fn drop(&mut self) {
        self.stop();
    }
}

pub struct MacawCommand<Command> {
    pub(crate) kind: Command,
    pub(crate) reply_tx: oneshot::Sender<Result<(), anyhow::Error>>,
}

pub enum RecordCommand {
    Record(PathBuf),
}

impl MacawCommand<RecordCommand> {
    pub fn record(reply_tx: oneshot::Sender<Result<(), anyhow::Error>>, path: PathBuf) -> Self {
        Self {
            kind: RecordCommand::Record(path),
            reply_tx,
        }
    }
}

#[async_trait::async_trait(?Send)]
pub trait Processor {
    type Command;

    fn add_proxy<P: Proxy>(
        &mut self,
        rx: mpsc::UnboundedReceiver<
            Message<P::DownstreamInputMessage, P::DownstreamOutputMessage, P::UpstreamInputMessage>,
        >,
        proxy: P,
    ) where
        P::DownstreamInputMessage: RecordEvent + Clone,
        P::DownstreamOutputMessage: RecordEvent + Clone,
        P::UpstreamInputMessage: RecordEvent + Clone;

    async fn start(
        &mut self,
        rx: mpsc::UnboundedReceiver<MacawCommand<Self::Command>>,
    ) -> Result<(), anyhow::Error>;
}
