use crate::lib::*;

pub fn response_channel<T>() -> (ResponseSender<T>, ResponseReceiver<T>) {
    let (sender, receiver) = reply_channel();
    (ResponseSender::new(sender), ResponseReceiver::new(receiver))
}

#[derive(Debug)]
pub struct ResponseReceiver<T> {
    receiver: ReplyReceiver<T>,
}

impl<T> ResponseReceiver<T> {
    pub(crate) fn new(receiver: ReplyReceiver<T>) -> Self {
        Self { receiver }
    }

    pub async fn recv(self) -> Result<T, anyhow::Error> {
        self.receiver.recv().await
    }
}

#[derive(Debug)]
pub struct ResponseSender<T> {
    sender: ReplySender<T>,
}

impl<T> ResponseSender<T> {
    pub(crate) fn new(sender: ReplySender<T>) -> Self {
        Self { sender }
    }

    pub fn send(self, message: T) -> Result<(), anyhow::Error> {
        self.sender.send(message)
    }
}

// ------------------------------------------------------------

/// Channel to lock the replay of recordings, allowing to unlock it later or when dropping the lock.
pub(crate) fn lock_channel() -> (ReplayLockHolder, ReplayLock) {
    let notify = Arc::new(tokio::sync::Notify::new());

    (ReplayLockHolder(notify.clone()), ReplayLock(notify))
}

#[derive(Debug)]
pub(crate) struct ReplayLock(Arc<tokio::sync::Notify>);

impl ReplayLock {
    pub async fn wait(&self) {
        self.0.notified().await
    }
}

#[derive(Debug)]
pub struct ReplayLockHolder(Arc<tokio::sync::Notify>);

impl ReplayLockHolder {
    pub fn unlock(&self) {
        self.0.notify_one();
    }
}

impl Drop for ReplayLockHolder {
    fn drop(&mut self) {
        self.unlock();
    }
}
