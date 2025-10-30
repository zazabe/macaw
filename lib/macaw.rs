use crate::lib::*;

pub struct MacawSetup<Exec, Sched>
where
    Exec: TaskExecutor + 'static,
    Sched: Scheduler + 'static,
{
    executor: Exec,
    scheduler: Sched,
}

impl<Exec, Sched> MacawSetup<Exec, Sched>
where
    Exec: TaskExecutor + 'static,
    Sched: Scheduler + 'static,
{
    pub fn new(executor: Exec, scheduler: Sched) -> Self {
        Self {
            executor,
            scheduler,
        }
    }

    pub async fn add_http_proxy(
        &mut self,
        addr: SocketAddr,
        target_url: http::Uri,
    ) -> Result<(), anyhow::Error> {
        self.scheduler
            .add_http_proxy(self.executor.clone(), addr, target_url)
            .await
    }

    pub fn start(self) -> Macaw {
        let Self {
            executor,
            mut scheduler,
        } = self;
        let (tx, rx) = mpsc::unbounded_channel();
        let task = executor.execute(Box::pin(async move {
            scheduler.start(rx).await?;
            Ok(())
        }));
        Macaw::new(task, tx)
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
pub trait Scheduler {
    async fn add_http_proxy<Exec>(
        &mut self,
        executor: Exec,
        addr: SocketAddr,
        target_url: http::Uri,
    ) -> Result<(), anyhow::Error>
    where
        Exec: TaskExecutor;
    async fn start(
        &mut self,
        rx: mpsc::UnboundedReceiver<MacawCommand>,
    ) -> Result<(), anyhow::Error>;
}
