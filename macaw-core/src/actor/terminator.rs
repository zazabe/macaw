use crate::lib::*;

#[derive(Debug, Error)]
pub enum AppError {
    #[error("Exited with error: {0}")]
    ExitWithError(#[from] anyhow::Error),
    #[error("Bug: Unexpected error: {0}")]
    UnexpectedError(Box<dyn std::error::Error + Send + Sync>),
}

/// Terminator to signal the main application to exit.
#[derive(Debug)]
pub struct AppTerminator {
    tx: mpsc::Sender<Result<(), AppError>>,
    rx: mpsc::Receiver<Result<(), AppError>>,
    exit_notifier: Arc<tokio::sync::Notify>,
}

impl AppTerminator {
    pub fn new() -> Self {
        let (tx, rx) = mpsc::channel(1);
        let exit_notifier = Arc::new(tokio::sync::Notify::new());
        Self {
            tx,
            rx,
            exit_notifier,
        }
    }

    pub fn exit(&self) {
        self.tx.try_send(Ok(())).ok();
        self.exit_notifier.notify_waiters();
    }

    pub fn exit_with_error(&self, error: anyhow::Error) {
        self.tx.try_send(Err(error.into())).ok();
        self.exit_notifier.notify_waiters();
    }

    pub(crate) fn notified(&self) -> ExitNotifier {
        ExitNotifier::new(self)
    }

    pub async fn wait_until_stopped(mut self) -> Result<(), AppError> {
        self.rx
            .recv()
            .await
            .ok_or_else(|| AppError::UnexpectedError("App terminator channel closed".into()))
            .flatten()
    }
}

impl Default for AppTerminator {
    fn default() -> Self {
        Self::new()
    }
}

// Notifies when the main application exits.
#[derive(Debug, Clone)]
pub(crate) struct ExitNotifier {
    notifier: Arc<tokio::sync::Notify>,
    tx: mpsc::Sender<Result<(), AppError>>,
}
impl ExitNotifier {
    fn new(app_terminator: &AppTerminator) -> Self {
        let notifier = Arc::clone(&app_terminator.exit_notifier);
        let tx = app_terminator.tx.clone();
        Self { notifier, tx }
    }

    pub(crate) fn exit(&self) {
        self.tx.try_send(Ok(())).ok();
    }

    pub(crate) fn exit_with_error(&self, error: anyhow::Error) {
        self.tx.try_send(Err(error.into())).ok();
    }
    pub(crate) async fn notified(&self) {
        self.notifier.notified().await
    }
}

/// Terminator to signal a specific task to terminate.
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
