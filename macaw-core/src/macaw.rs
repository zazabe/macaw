use crate::lib::*;

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

pub struct Macaw<Command: ProcessorCommand> {
    task: Option<TokioTask>,
    tx: mpsc::UnboundedSender<MacawCommand<Command>>,
}

impl<Command: ProcessorCommand> Macaw<Command> {
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

    pub(crate) async fn send_command(&self, command: Command) -> Result<(), anyhow::Error> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(MacawCommand::new(command.clone(), tx))
            .map_err(|_| anyhow::anyhow!("Failed to send command: {:?}", command))?;
        rx.await
            .map_err(|_| anyhow::anyhow!("Failed to receive command response: {:?}", command))??;
        Ok(())
    }
}

impl<Command: ProcessorCommand> Drop for Macaw<Command> {
    fn drop(&mut self) {
        self.stop();
    }
}

pub trait ProcessorCommand: fmt::Debug + Clone + 'static {}

pub struct MacawCommand<Command: ProcessorCommand> {
    pub(crate) kind: Command,
    pub(crate) reply_tx: oneshot::Sender<Result<(), anyhow::Error>>,
}

impl<Command: ProcessorCommand> MacawCommand<Command> {
    pub(crate) fn new(kind: Command, reply_tx: oneshot::Sender<Result<(), anyhow::Error>>) -> Self {
        Self { kind, reply_tx }
    }
}

#[async_trait::async_trait(?Send)]
pub trait Processor {
    type Command: ProcessorCommand;

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
