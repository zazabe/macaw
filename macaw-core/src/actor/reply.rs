use tokio::sync::oneshot;

pub fn request_reply<T>() -> (Reply<T>, Response<T>) {
    let (tx, rx) = oneshot::channel();
    (Reply { tx }, Response { rx })
}

#[derive(Debug)]
pub struct Response<T> {
    rx: oneshot::Receiver<T>,
}

impl<T> Response<T> {
    pub async fn recv(self) -> Result<T, anyhow::Error> {
        self.rx
            .await
            .map_err(|e| anyhow::anyhow!("Failed to receive reply: {}", e))
    }
}

#[derive(Debug)]
pub struct Reply<T> {
    tx: oneshot::Sender<T>,
}

impl<T> Reply<T> {
    pub fn send(self, value: T) -> Result<(), anyhow::Error> {
        self.tx
            .send(value)
            .map_err(|e| anyhow::anyhow!("Failed to send reply"))
    }
}
