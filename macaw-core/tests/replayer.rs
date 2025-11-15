use macaw_core::prelude::*;
use serde::{Deserialize, Serialize};
use std::{
    fmt,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::{
    sync::mpsc,
    time::{Duration, sleep},
};

#[tokio::test]
async fn test_replayer_multiple_proxies() {
    let recording_path =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/recordings.yaml");
    let mut macaw = Macaw::<Replayer>::replayer(recording_path).unwrap();

    let (tx1, rx1) = actor_channel::<ProxyReplayerActor<TestProxy>>();
    let (replay_tx1, mut replay_rx1) = mpsc::unbounded_channel::<TestEvent>();
    let sender1 = tx1.clone();
    macaw
        .add_test_proxy("test_proxy1", tx1, rx1, replay_tx1)
        .await
        .unwrap();
    let (tx2, rx2) = actor_channel::<ProxyReplayerActor<TestProxy>>();
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
        .send(DownstreamMessage::new(RequestEvent {
            value: "proxy1_event".to_string(),
        }))
        .unwrap();
    assert_eq!(
        replay_rx1.recv().await.unwrap(),
        TestEvent::Response(ResponseEvent {
            value: "response:proxy1_event".to_string(),
        })
    );

    // Send request to proxy 2 and wait for response
    sender2
        .send(DownstreamMessage::new(RequestEvent {
            value: "proxy2_event".to_string(),
        }))
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

impl RecordEventUntagged for TestEvent {
    fn downcast(event: Box<dyn RecordEvent>) -> anyhow::Result<Self, Box<dyn RecordEvent>>
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
        if let Some((event, replay_lock)) = event {
            let _ = replay_lock.take();
        }
    }
}

#[derive(Debug)]
struct TestProxy {
    id: ProxyId,
    recordings: Recordings,
    replay_tx: mpsc::UnboundedSender<TestEvent>,
}

impl TestProxy {
    fn new(id: ProxyId, replay_tx: mpsc::UnboundedSender<TestEvent>) -> Self {
        Self {
            id,
            recordings: Recordings::default(),
            replay_tx,
        }
    }
}

impl ProxyReplayer for TestProxy {
    type DownstreamIncomingMessage = RequestEvent;
    type DownstreamOutgoingMessage = ResponseEvent;
    type RecordedMessage = TestEvent;

    fn id(&self) -> ProxyId {
        self.id
    }

    async fn downstream_incoming_process(
        &mut self,
        message: Self::DownstreamIncomingMessage,
        response_sender: Option<ResponseSender<Self::DownstreamOutgoingMessage>>,
    ) -> Result<(), anyhow::Error> {
        self.recordings.unlock(&message);
        Ok(())
    }

    async fn handle_recorded_message(
        &mut self,
        message: Self::RecordedMessage,
        replay_lock: ReplayLockHolder,
    ) -> Result<(), anyhow::Error> {
        match &message {
            TestEvent::Request(event) => {
                self.recordings.push(message, Some(replay_lock));
            }
            TestEvent::Response(event) => {
                self.replay_tx.send(message.clone()).unwrap();
                self.recordings.push(message, None);
            }
            TestEvent::Incoming(event) => {
                self.replay_tx.send(message.clone()).unwrap();
                self.recordings.push(message, None);
            }
        }
        Ok(())
    }
}

trait TestMacawReplayerSetup {
    async fn add_test_proxy(
        &mut self,
        proxy_id: &str,
        tx: ActorChannelSender<ProxyReplayerActor<TestProxy>>,
        rx: ActorChannelReceiver<ProxyReplayerActor<TestProxy>>,
        replay_tx: mpsc::UnboundedSender<TestEvent>,
    ) -> Result<(), anyhow::Error>;
}

impl TestMacawReplayerSetup for Macaw<Replayer> {
    async fn add_test_proxy(
        &mut self,
        proxy_id: &str,
        tx: ActorChannelSender<ProxyReplayerActor<TestProxy>>,
        rx: ActorChannelReceiver<ProxyReplayerActor<TestProxy>>,
        replay_tx: mpsc::UnboundedSender<TestEvent>,
    ) -> Result<(), anyhow::Error> {
        let proxy_id = ProxyId::new(proxy_id).unwrap();
        let proxy = TestProxy::new(proxy_id, replay_tx);
        self.add_proxy(move |_replayer, actor_context| {
            let actor = ProxyReplayerActor::new(actor_context.clone(), proxy);
            Ok((proxy_id, actor.run_with_channel(&actor_context, tx, rx)))
        })
        .await?;
        Ok(())
    }
}
