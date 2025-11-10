use crate::lib::*;

#[derive(Debug, Error)]
pub enum AppError {
    #[error("Exited")]
    Exit,
    #[error("Exited with error: {0}")]
    ExitWithError(#[from] anyhow::Error),
}

/// A terminator can be used to signal tasks to terminate.
#[derive(Debug, Clone)]
pub struct AppTerminator {
    tx: watch::Sender<Option<Result<(), AppError>>>,
    rx: watch::Receiver<Option<Result<(), AppError>>>,
}
impl AppTerminator {
    pub fn new() -> Self {
        let (tx, rx) = watch::channel(None);
        Self { tx, rx }
    }

    pub fn exit(&self) {
        self.tx.send(Some(Ok(()))).ok();
    }

    pub fn exit_with_error(&self, error: anyhow::Error) {
        if self.rx.borrow().is_none() {
            self.tx.send(Some(Err(AppError::ExitWithError(error)))).ok();
        }
    }

    pub async fn wait_until_stopped(&mut self) {
        if let Err(e) = self.rx.changed().await {
            error!("Bug: Failed to wait for app terminator: {}", e);
            std::process::exit(1);
        }

        match &*self.rx.borrow_and_update() {
            Some(Ok(())) => (),
            Some(Err(error)) => {
                error!("App terminated with error: {}", error);
                std::process::exit(1);
            }
            None => {
                error!("Bug: Application already dropped");
                std::process::exit(1);
            }
        }
    }
}

impl Default for AppTerminator {
    fn default() -> Self {
        Self::new()
    }
}

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
