use crate::lib::*;

pub struct MacawBuilder<Exec, Sched>
where
    Exec: TaskExecutor + 'static,
    Sched: Scheduler + 'static,
{
    executor: Exec,
    scheduler: Sched,
}

impl<Exec, Sched> MacawBuilder<Exec, Sched>
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

    pub fn run(self) -> MacawGuard {
        let Self {
            executor,
            mut scheduler,
        } = self;
        let task = executor.execute(Box::pin(async move {
            scheduler.start().await?;
            Ok(())
        }));
        MacawGuard::new(task)
    }
}

pub struct MacawGuard {
    task: Option<TokioTask>,
}

impl MacawGuard {
    fn new(task: TokioTask) -> Self {
        Self { task: Some(task) }
    }

    pub fn stop(&mut self) {
        if let Some(task) = self.task.take() {
            task.cancel();
        }
    }
}

impl Drop for MacawGuard {
    fn drop(&mut self) {
        self.stop();
    }
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
    async fn start(&mut self) -> Result<(), anyhow::Error>;
}
