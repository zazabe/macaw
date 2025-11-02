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

    pub fn start(self) -> Macaw {
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

pub struct Macaw {
    task: Option<TokioTask>,
    tx: mpsc::UnboundedSender<MacawCommand>,
}

impl Macaw {
    fn new(task: TokioTask, tx: mpsc::UnboundedSender<MacawCommand>) -> Self {
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

    pub async fn record(&mut self, path: PathBuf) -> Result<(), anyhow::Error> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(MacawCommand::record(tx, path))?;
        rx.await?
    }
}

impl Drop for Macaw {
    fn drop(&mut self) {
        self.stop();
    }
}

pub struct MacawCommand {
    pub(crate) kind: MacawCommandKind,
    pub(crate) reply_tx: oneshot::Sender<Result<(), anyhow::Error>>,
}

impl MacawCommand {
    pub fn record(reply_tx: oneshot::Sender<Result<(), anyhow::Error>>, path: PathBuf) -> Self {
        Self {
            kind: MacawCommandKind::Record(path),
            reply_tx,
        }
    }
}

pub(crate) enum MacawCommandKind {
    Record(PathBuf),
}

#[async_trait::async_trait(?Send)]
pub trait Processor {
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
        rx: mpsc::UnboundedReceiver<MacawCommand>,
    ) -> Result<(), anyhow::Error>;
}
