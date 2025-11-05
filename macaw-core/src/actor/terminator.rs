use tokio::sync::watch;

/// A terminator can be used to signal tasks to terminate.
#[derive(Debug, Clone)]
pub struct TaskTerminator {
    tx: watch::Sender<bool>,
    rx: watch::Receiver<bool>,
}
impl TaskTerminator {
    pub fn new() -> Self {
        let (tx, rx) = watch::channel(false);
        Self { tx, rx }
    }

    pub fn stop(&self) {
        self.tx.send(true).ok();
    }

    pub fn is_stopped(&self) -> bool {
        *self.rx.borrow()
    }

    pub async fn wait(&mut self) {
        if !*self.rx.borrow_and_update() {
            self.rx.changed().await.ok();
        }
    }
}

impl Default for TaskTerminator {
    fn default() -> Self {
        Self::new()
    }
}
