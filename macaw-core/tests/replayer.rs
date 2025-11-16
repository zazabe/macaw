use macaw_core::prelude::*;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tracing::error;

#[tokio::test]
async fn test_replayer_multiple_proxies() {
    let recording_path =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/recordings.yaml");
    let mut macaw = Macaw::<Replayer>::replayer(recording_path).unwrap();

    let (tx1, rx1) = actor_channel::<TestProxyActor>();
    let (replay_tx1, mut replay_rx1) = mpsc::unbounded_channel::<TestEvent>();
    let sender1 = tx1.clone();
    macaw
        .add_test_proxy("test_proxy1", tx1, rx1, replay_tx1)
        .await
        .unwrap();
    let (tx2, rx2) = actor_channel::<TestProxyActor>();
    let (replay_tx2, mut replay_rx2) = mpsc::unbounded_channel::<TestEvent>();
    let sender2 = tx2.clone();
    macaw
        .add_test_proxy("test_proxy2", tx2, rx2, replay_tx2)
        .await
        .unwrap();

    macaw.play().unwrap();

    // Give time for events to be replayed
    tokio::task::yield_now().await;

    // Replayer is waiting for requests to be sent to the proxies
    assert_eq!(replay_rx1.len(), 0);
    assert_eq!(replay_rx2.len(), 0);

    // Send request to proxy 1 and wait for response
    sender1
        .send(RequestEvent {
            value: "proxy1_event".to_string(),
        })
        .unwrap();
    assert_eq!(
        replay_rx1.recv().await.unwrap(),
        TestEvent::Response(ResponseEvent {
            value: "response:proxy1_event".to_string(),
        })
    );

    // Send request to proxy 2 and wait for response
    sender2
        .send(RequestEvent {
            value: "proxy2_event".to_string(),
        })
        .unwrap();

    assert_eq!(
        replay_rx2.recv().await.unwrap(),
        TestEvent::Response(ResponseEvent {
            value: "response:proxy2_event".to_string(),
        })
    );

    // Receive replayed incoming events from the proxies
    assert_eq!(
        replay_rx1.recv().await.unwrap(),
        TestEvent::Incoming(IncomingEvent {
            value: "incoming_event".to_string(),
        })
    );
    assert_eq!(
        replay_rx2.recv().await.unwrap(),
        TestEvent::Incoming(IncomingEvent {
            value: "incoming_event".to_string(),
        })
    );
}

// ------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct RequestEvent {
    value: String,
}

#[typetag::serde]
impl RecordEvent for RequestEvent {}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct ResponseEvent {
    value: String,
}

#[typetag::serde]
impl RecordEvent for ResponseEvent {}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct IncomingEvent {
    value: String,
}

#[typetag::serde]
impl RecordEvent for IncomingEvent {}

#[derive(Debug, Clone, PartialEq, Eq)]
enum TestEvent {
    Request(RequestEvent),
    Response(ResponseEvent),
    Incoming(IncomingEvent),
}

impl TestEvent {
    fn downcast(event: Box<dyn RecordEvent>) -> Result<Self, anyhow::Error>
    where
        Self: Sized,
    {
        event
            .downcast::<RequestEvent>()
            .map(|event| Self::Request(*event))
            .or_else(|event| {
                event
                    .downcast::<ResponseEvent>()
                    .map(|event| Self::Response(*event))
            })
            .or_else(|event| {
                event
                    .downcast::<IncomingEvent>()
                    .map(|event| Self::Incoming(*event))
            })
            .map_err(|event| anyhow::anyhow!("Failed to downcast test event: {:?}", event))
    }
}

#[derive(Debug, Default)]
struct Recordings(Vec<(TestEvent, Option<ReplayLockHolder>)>);

impl Recordings {
    fn push(&mut self, event: TestEvent, replay_lock: Option<ReplayLockHolder>) {
        self.0.push((event, replay_lock));
    }

    fn unlock(&mut self, request: &RequestEvent) {
        let event = self.0.iter_mut().find(
            |(event, _)| matches!(event, TestEvent::Request(event) if event.value == request.value),
        );
        if let Some((_event, replay_lock)) = event {
            let _ = replay_lock.take();
        }
    }
}

#[derive(Debug)]
struct TestProxyActor {
    context: ActorContext,
    proxy_id: ProxyId,
    recordings: Recordings,
    replay_tx: mpsc::UnboundedSender<TestEvent>,
}

impl TestProxyActor {
    fn new(
        context: ActorContext,
        proxy_id: ProxyId,
        replay_tx: mpsc::UnboundedSender<TestEvent>,
    ) -> Self {
        Self {
            context,
            proxy_id,
            recordings: Recordings::default(),
            replay_tx,
        }
    }

    fn handle_event(&mut self, event: RecordedEventWithLock) -> Result<(), anyhow::Error> {
        let RecordedEventWithLock {
            event, replay_lock, ..
        } = event;
        let message = TestEvent::downcast(event)?;
        match &message {
            TestEvent::Request(..) => {
                self.recordings.push(message.clone(), Some(replay_lock));
            }
            TestEvent::Response(..) | TestEvent::Incoming(..) => {
                self.replay_tx.send(message.clone()).unwrap();
                self.recordings.push(message, None);
            }
        }
        Ok(())
    }
}

impl Actor for TestProxyActor {
    fn context(&self) -> &ActorContext {
        &self.context
    }
}

impl ProxyActor for TestProxyActor {
    fn proxy_id(&self) -> ProxyId {
        self.proxy_id
    }
}

impl ActorHandler<RequestEvent> for TestProxyActor {
    type Reply = ();

    async fn handle(&mut self, request: RequestEvent) {
        self.recordings.unlock(&request);
    }
}

impl ActorHandler<RecordedEventWithLock> for TestProxyActor {
    type Reply = ();

    async fn handle(&mut self, event: RecordedEventWithLock) {
        if let Err(e) = self.handle_event(event) {
            error!("Failed to handle test event: {:?}", e);
        }
    }
}
trait TestMacawReplayerSetup {
    async fn add_test_proxy(
        &mut self,
        proxy_id: &str,
        tx: ActorChannelSender<TestProxyActor>,
        rx: ActorChannelReceiver<TestProxyActor>,
        replay_tx: mpsc::UnboundedSender<TestEvent>,
    ) -> Result<(), anyhow::Error>;
}

impl TestMacawReplayerSetup for Macaw<Replayer> {
    async fn add_test_proxy(
        &mut self,
        proxy_id: &str,
        tx: ActorChannelSender<TestProxyActor>,
        rx: ActorChannelReceiver<TestProxyActor>,
        replay_tx: mpsc::UnboundedSender<TestEvent>,
    ) -> Result<(), anyhow::Error> {
        self.add_proxy(move |_replayer, actor_context| {
            let proxy_id: ProxyId = proxy_id.parse()?;
            let context = actor_context.create_child(&proxy_id.to_string());
            let actor = TestProxyActor::new(context, proxy_id, replay_tx);
            Ok((proxy_id, actor.run_with_channel(tx, rx)))
        })
        .await?;
        Ok(())
    }
}
