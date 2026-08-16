use crate::lib::*;
use std::sync::atomic::{AtomicBool, Ordering};

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
    exit_notifier: watch::Sender<bool>,
    exited: Arc<AtomicBool>,
}

impl AppTerminator {
    pub fn new() -> Self {
        let (tx, rx) = mpsc::channel(1);
        let (exit_notifier, _) = watch::channel(false);
        Self {
            tx,
            rx,
            exit_notifier,
            exited: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn exit(&self) {
        self.exit_handle().exit();
    }

    pub fn exit_with_error(&self, error: anyhow::Error) {
        self.exit_handle().exit_with_error(error);
    }

    pub(crate) fn exit_handle(&self) -> AppExitHandle {
        AppExitHandle::new(self)
    }

    pub async fn wait_until_exit(mut self) -> Result<(), AppError> {
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

/// App exit handle to signal and observe application exit events.
#[derive(Debug, Clone)]
pub struct AppExitHandle {
    notifier_tx: watch::Sender<bool>,
    notifier_rx: watch::Receiver<bool>,
    tx: mpsc::Sender<Result<(), AppError>>,
    exited: Arc<AtomicBool>,
}
impl AppExitHandle {
    fn new(app_terminator: &AppTerminator) -> Self {
        let notifier_tx = app_terminator.exit_notifier.clone();
        let notifier_rx = app_terminator.exit_notifier.subscribe();
        let tx = app_terminator.tx.clone();
        let exited = Arc::clone(&app_terminator.exited);
        Self {
            notifier_tx,
            notifier_rx,
            tx,
            exited,
        }
    }

    pub fn exit(&self) {
        self.publish(Ok(()));
    }

    pub fn exit_with_error(&self, error: anyhow::Error) {
        self.publish(Err(error.into()));
    }

    fn publish(&self, result: Result<(), AppError>) {
        if self
            .exited
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            self.tx.try_send(result).ok();
            // `send_replace` latches the notification even when no receiver is waiting.
            self.notifier_tx.send_replace(true);
        }
    }

    pub(crate) async fn notified(&mut self) {
        if *self.notifier_rx.borrow_and_update() {
            return;
        }
        while self.notifier_rx.changed().await.is_ok() {
            if *self.notifier_rx.borrow_and_update() {
                return;
            }
        }
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
