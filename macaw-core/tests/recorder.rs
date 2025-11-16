use macaw_core::prelude::*;
use serde::{Deserialize, Serialize};
use tracing::error;

#[tokio::test]
async fn test_recorder_multiple_proxies() {
    let temp_file = tempfile::NamedTempFile::new().expect("Failed to create temp file");
    let test_file = temp_file.path().to_path_buf();

    // Create recorder
    let mut macaw = Macaw::<Recorder>::recorder();
    let (tx, rx) = actor_channel::<TestProxyActor>();
    let sender1 = tx.clone();
    macaw.add_test_proxy("test_proxy1", tx, rx).await.unwrap();
    let (tx, rx) = actor_channel::<TestProxyActor>();
    let sender2 = tx.clone();
    macaw.add_test_proxy("test_proxy2", tx, rx).await.unwrap();

    // Send events from both proxies
    let event1 = RequestEvent {
        value: "proxy1_event".to_string(),
    };
    let event2 = RequestEvent {
        value: "proxy2_event".to_string(),
    };

    let _response1 = sender1.request(event1.clone()).await;
    let _response2 = sender2.request(event2.clone()).await;

    // Send incoming event from both proxies
    let event3 = IncomingEvent {
        value: "incoming_event".to_string(),
    };
    sender1.send(event3.clone()).unwrap();
    sender2.send(event3.clone()).unwrap();

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
struct TestProxyActor {
    context: ActorContext,
    proxy_id: ProxyId,
    recorder: ActorHandle<Recorder>,
}

impl TestProxyActor {
    fn new(context: ActorContext, proxy_id: ProxyId, recorder: ActorHandle<Recorder>) -> Self {
        Self {
            context,
            proxy_id,
            recorder,
        }
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
    type Reply = Result<ResponseEvent, anyhow::Error>;

    async fn handle(&mut self, request: RequestEvent) -> Result<ResponseEvent, anyhow::Error> {
        self.recorder
            .send(RecordedEvent::new(self.proxy_id, request.clone()))?;
        let response = ResponseEvent {
            value: format!("response:{}", request.value),
        };
        self.recorder
            .send(RecordedEvent::new(self.proxy_id, response.clone()))?;
        Ok(response)
    }
}

impl ActorHandler<IncomingEvent> for TestProxyActor {
    type Reply = ();

    async fn handle(&mut self, event: IncomingEvent) {
        if let Err(e) = self
            .recorder
            .send(RecordedEvent::new(self.proxy_id, event.clone()))
        {
            error!("Failed to record event: {:?}", e);
        }
    }
}

trait TestMacawRecordSetup {
    fn add_test_proxy(
        &mut self,
        proxy_id: &str,
        tx: ActorChannelSender<TestProxyActor>,
        rx: ActorChannelReceiver<TestProxyActor>,
    ) -> impl Future<Output = Result<(), anyhow::Error>>;
}

impl TestMacawRecordSetup for Macaw<Recorder> {
    async fn add_test_proxy(
        &mut self,
        proxy_id: &str,
        tx: ActorChannelSender<TestProxyActor>,
        rx: ActorChannelReceiver<TestProxyActor>,
    ) -> Result<(), anyhow::Error> {
        self.add_proxy(move |recorder, actor_context| {
            let proxy_id: ProxyId = proxy_id.parse()?;
            let context = actor_context.create_child(&proxy_id.to_string());
            let actor = TestProxyActor::new(context, proxy_id, recorder.clone());
            Ok((proxy_id, actor.run_with_channel(tx, rx)))
        })?;
        Ok(())
    }
}
