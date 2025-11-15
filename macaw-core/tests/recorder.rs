use macaw_core::prelude::*;
use serde::{Deserialize, Serialize};
use tokio::time::{Duration, sleep};

#[tokio::test]
async fn test_recorder_multiple_proxies() {
    let temp_file = tempfile::NamedTempFile::new().expect("Failed to create temp file");
    let test_file = temp_file.path().to_path_buf();

    // Create recorder
    let mut macaw = Macaw::<Recorder>::recorder();
    let (tx, rx) = actor_channel::<ProxyRecorderActor<TestProxy>>();
    let sender1 = tx.clone();
    macaw.add_test_proxy("test_proxy1", tx, rx).await.unwrap();
    let (tx, rx) = actor_channel::<ProxyRecorderActor<TestProxy>>();
    let sender2 = tx.clone();
    macaw.add_test_proxy("test_proxy2", tx, rx).await.unwrap();

    // Send events from both proxies
    let event1 = RequestEvent {
        value: "proxy1_event".to_string(),
    };
    let event2 = RequestEvent {
        value: "proxy2_event".to_string(),
    };

    let _response1 = sender1
        .request(DownstreamMessage::new(event1.clone()))
        .await;
    let _response2 = sender2
        .request(DownstreamMessage::new(event2.clone()))
        .await;

    // Send incoming event from both proxies
    let event3 = IncomingEvent {
        value: "incoming_event".to_string(),
    };
    sender1.send(UpstreamMessage::new(event3.clone())).unwrap();
    sender2.send(UpstreamMessage::new(event3.clone())).unwrap();

    // Give time for events to be recorded
    tokio::task::yield_now().await;

    // Exit and save
    macaw.exit_handle().exit();
    macaw.record_when_exit(&test_file).await.unwrap();

    assert!(test_file.exists(), "Record file should exist");
    let file_content: serde_yaml::Value =
        serde_yaml::from_str(std::fs::read_to_string(test_file).unwrap().as_str()).unwrap();
    insta::assert_yaml_snapshot!(file_content, {
        r#"["header"]["record_id"]"# => "[record_id]",
        r#"["header"]["record_seed"]"# => "[record_seed]",
        r#".**["timestamp"]"# => "[timestamp]",
    }, @r#"
    header:
      record_id: "[record_id]"
      record_seed: "[record_seed]"
      timestamp: "[timestamp]"
    events:
      - proxy: test_proxy1
        timestamp: "[timestamp]"
        RequestEvent:
          value: proxy1_event
      - proxy: test_proxy1
        timestamp: "[timestamp]"
        ResponseEvent:
          value: "response:proxy1_event"
      - proxy: test_proxy2
        timestamp: "[timestamp]"
        RequestEvent:
          value: proxy2_event
      - proxy: test_proxy2
        timestamp: "[timestamp]"
        ResponseEvent:
          value: "response:proxy2_event"
      - proxy: test_proxy1
        timestamp: "[timestamp]"
        IncomingEvent:
          value: incoming_event
      - proxy: test_proxy2
        timestamp: "[timestamp]"
        IncomingEvent:
          value: incoming_event
    "#);
}

// ------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RequestEvent {
    value: String,
}

#[typetag::serde]
impl RecordEvent for RequestEvent {}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ResponseEvent {
    value: String,
}

#[typetag::serde]
impl RecordEvent for ResponseEvent {}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct IncomingEvent {
    value: String,
}

#[typetag::serde]
impl RecordEvent for IncomingEvent {}

#[derive(Debug, Clone)]
struct TestProxy {
    id: ProxyId,
}

impl TestProxy {
    fn new(id: ProxyId) -> Self {
        Self { id }
    }
}

impl ProxyRecorder for TestProxy {
    type DownstreamIncomingMessage = RequestEvent;
    type DownstreamOutgoingMessage = ResponseEvent;
    type UpstreamIncomingMessage = IncomingEvent;

    fn id(&self) -> ProxyId {
        self.id
    }

    async fn downstream_incoming_process(
        &mut self,
        message: Self::DownstreamIncomingMessage,
    ) -> Result<Option<Self::DownstreamOutgoingMessage>, anyhow::Error> {
        Ok(Some(ResponseEvent {
            value: format!("response:{}", message.value),
        }))
    }

    async fn upstream_incoming_process(
        &mut self,
        message: Self::UpstreamIncomingMessage,
    ) -> Result<(), anyhow::Error> {
        Ok(())
    }
}

trait TestMacawRecordSetup {
    fn add_test_proxy(
        &mut self,
        proxy_id: &str,
        tx: ActorChannelSender<ProxyRecorderActor<TestProxy>>,
        rx: ActorChannelReceiver<ProxyRecorderActor<TestProxy>>,
    ) -> impl Future<Output = Result<(), anyhow::Error>>;
}

impl TestMacawRecordSetup for Macaw<Recorder> {
    async fn add_test_proxy(
        &mut self,
        proxy_id: &str,
        tx: ActorChannelSender<ProxyRecorderActor<TestProxy>>,
        rx: ActorChannelReceiver<ProxyRecorderActor<TestProxy>>,
    ) -> Result<(), anyhow::Error> {
        let proxy = TestProxy::new(proxy_id.parse()?);
        self.add_proxy(move |recorder, actor_context| {
            let proxy_id = proxy.id();
            let actor = ProxyRecorderActor::new(proxy, recorder.clone());
            Ok((proxy_id, actor.run_with_channel(&actor_context, tx, rx)))
        })?;
        Ok(())
    }
}
