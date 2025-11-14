use crate::lib::*;
use std::sync::Arc;

#[tokio::test]
async fn test_actor_start_stop() {
    let app_context = AppContext::new();
    let actor_context = app_context.actor_context();

    let actor = TestActor::new("test_actor");
    let state = actor.state();

    let handle = actor.run(&actor_context);

    // Yield to allow actor to start
    tokio::task::yield_now().await;
    assert!(state.is_started());

    // Stop the actor
    handle.stop();

    tokio::task::yield_now().await;
    assert!(state.is_stopped());
    assert_eq!(
        state.stop_reason(),
        Some(ActorStopReason::TaskTerminatedReceived)
    );
}

#[tokio::test]
async fn test_app_exit() {
    let app_context = AppContext::new();
    let actor_context = app_context.actor_context();

    let actor1 = TestActor::new("test_actor_1");
    let actor2 = TestActor::new("test_actor_2");
    let state1 = actor1.state();
    let state2 = actor2.state();

    let handle1 = actor1.run(&actor_context);
    let handle2 = actor2.run(&actor_context);

    tokio::task::yield_now().await;
    assert!(state1.is_started());
    assert!(state2.is_started());

    app_context.exit();

    tokio::task::yield_now().await;
    assert!(state1.is_stopped());
    assert_eq!(
        state1.stop_reason(),
        Some(ActorStopReason::ExitNotificationReceived)
    );
    assert!(state2.is_stopped());
    assert_eq!(
        state2.stop_reason(),
        Some(ActorStopReason::ExitNotificationReceived)
    );

    // Verify app context exit result
    let result = app_context.wait_until_exit().await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_app_exit_with_error() {
    let app_context = AppContext::new();
    let actor_context = app_context.actor_context();

    let actor1 = TestActor::new("test_actor_1");
    let actor2 = TestActor::new("test_actor_2");
    let state1 = actor1.state();
    let state2 = actor2.state();

    let handle1 = actor1.run(&actor_context);
    let handle2 = actor2.run(&actor_context);

    tokio::task::yield_now().await;
    assert!(state1.is_started());
    assert!(state2.is_started());

    // Exit the app context with error
    app_context.exit_with_error(anyhow::anyhow!("Test application error"));

    tokio::task::yield_now().await;
    assert!(state1.is_stopped());
    assert!(state2.is_stopped());
    assert_eq!(
        state1.stop_reason(),
        Some(ActorStopReason::ExitNotificationReceived)
    );
    assert!(state2.is_stopped());
    assert_eq!(
        state2.stop_reason(),
        Some(ActorStopReason::ExitNotificationReceived)
    );

    let result = app_context.wait_until_exit().await;
    match result.unwrap_err() {
        AppError::ExitWithError(e) => {
            assert_eq!(e.to_string(), "Test application error");
        }
        _ => panic!("Expected ExitWithError"),
    }
}

#[tokio::test]
async fn test_actor_send() {
    let app_context = AppContext::new();
    let actor_context = app_context.actor_context();

    let actor = TestActor::new("test_actor");
    let state = actor.state();

    let handle = actor.run(&actor_context);

    tokio::task::yield_now().await;
    assert!(state.is_started());

    // Send messages
    handle.send(TestMessage::from_str("Hello")).unwrap();
    handle.send(TestMessage::from_str("World")).unwrap();

    tokio::task::yield_now().await;
    let messages = state.messages();
    assert_eq!(messages.len(), 2);
    assert_eq!(messages[0], "Hello");
    assert_eq!(messages[1], "World");

    handle.stop();
    tokio::task::yield_now().await;
    assert!(state.is_stopped());
    assert_eq!(
        state.stop_reason(),
        Some(ActorStopReason::TaskTerminatedReceived)
    );
}

#[tokio::test]
async fn test_actor_request() {
    let app_context = AppContext::new();
    let actor_context = app_context.actor_context();

    let actor = TestActor::new("test_actor");
    let state = actor.state();
    let handle = actor.run(&actor_context);

    // Yield to allow actor to start
    tokio::task::yield_now().await;
    assert!(state.is_started());

    // Send requests and verify replies
    let reply1 = handle.request(TestRequest { value: 5 }).await.unwrap();
    assert_eq!(reply1, 10, "Reply should be 5 * 2 = 10");

    let reply2 = handle.request(TestRequest { value: 7 }).await.unwrap();
    assert_eq!(reply2, 14, "Reply should be 7 * 2 = 14");

    let reply3 = handle.request(TestRequest { value: 0 }).await.unwrap();
    assert_eq!(reply3, 0, "Reply should be 0 * 2 = 0");

    handle.stop();
    tokio::task::yield_now().await;
    assert!(state.is_stopped());
    assert_eq!(
        state.stop_reason(),
        Some(ActorStopReason::TaskTerminatedReceived)
    );
}

#[tokio::test]
async fn test_actor_send_error_actor_stopped() {
    let app_context = AppContext::new();
    let actor_context = app_context.actor_context();

    let actor = TestActor::new("test_actor");
    let state = actor.state();

    let handle = actor.run(&actor_context);

    tokio::task::yield_now().await;
    assert!(state.is_started());

    app_context.exit();
    tokio::task::yield_now().await;

    assert!(state.is_stopped());
    assert_eq!(
        state.stop_reason(),
        Some(ActorStopReason::ExitNotificationReceived)
    );
    let error = handle.send(TestMessage::from_str("World")).unwrap_err();
    assert_eq!(
        error.to_string(),
        "Failed to send message, error: channel closed"
    );
}

#[tokio::test]
async fn test_actor_request_error_actor_stopped() {
    let app_context = AppContext::new();
    let actor_context = app_context.actor_context();

    let actor = TestActor::new("test_actor");
    let state = actor.state();

    let handle = actor.run(&actor_context);

    tokio::task::yield_now().await;
    assert!(state.is_started());

    app_context.exit();
    tokio::task::yield_now().await;

    assert!(state.is_stopped());
    assert_eq!(
        state.stop_reason(),
        Some(ActorStopReason::ExitNotificationReceived)
    );
    let error = handle.request(TestRequest { value: 5 }).await.unwrap_err();
    assert_eq!(error.to_string(), "Failed to send request: channel closed");
}

#[tokio::test]
async fn test_actor_stopped_due_to_channel_closed() {
    let app_context = AppContext::new();
    let actor_context = app_context.actor_context();

    let actor = TestActor::new("test_actor");
    let state = actor.state();

    let handle = actor.run(&actor_context);

    tokio::task::yield_now().await;
    assert!(state.is_started());

    // Drop the handle to close the channel
    drop(handle);

    tokio::task::yield_now().await;
    assert!(state.is_stopped());
    assert_eq!(state.stop_reason(), Some(ActorStopReason::ChannelClosed));

    // By default, any actor error should exit the app context
    let result = app_context.wait_until_exit().await;
    match result.unwrap_err() {
        AppError::ExitWithError(e) => {
            assert_eq!(
                e.to_string(),
                "[test_actor] Channel closed, actor handle dropped?"
            );
        }
        _ => panic!("Expected ExitWithError"),
    }
}

// ------------------------------------------------------------

#[derive(Debug)]
struct TestMessage {
    value: String,
}

impl TestMessage {
    fn from_str(s: &str) -> Self {
        Self {
            value: s.to_string(),
        }
    }
}

#[derive(Debug)]
struct TestRequest {
    value: u32,
}

#[derive(Debug)]
struct ErrorMessage;

#[derive(Debug, Clone, PartialEq, Eq)]
struct TestActorStateInner {
    messages_received: Vec<String>,
    started: bool,
    stopped: bool,
    stop_reason: Option<ActorStopReason>,
}

impl TestActorStateInner {
    fn new() -> Self {
        Self {
            messages_received: Vec::new(),
            started: false,
            stopped: false,
            stop_reason: None,
        }
    }
}

// Wrapper that hides Arc<Mutex<...>> complexity
#[derive(Debug, Clone)]
struct TestActorState {
    inner: Arc<std::sync::Mutex<TestActorStateInner>>,
}

impl TestActorState {
    fn new() -> Self {
        Self {
            inner: Arc::new(std::sync::Mutex::new(TestActorStateInner::new())),
        }
    }

    fn is_started(&self) -> bool {
        self.inner.lock().unwrap().started
    }

    fn is_stopped(&self) -> bool {
        self.inner.lock().unwrap().stopped
    }

    fn stop_reason(&self) -> Option<ActorStopReason> {
        self.inner.lock().unwrap().stop_reason.clone()
    }

    fn messages(&self) -> Vec<String> {
        self.inner.lock().unwrap().messages_received.clone()
    }

    fn set_started(&self, value: bool) {
        self.inner.lock().unwrap().started = value;
    }

    fn set_stopped(&self, value: bool) {
        self.inner.lock().unwrap().stopped = value;
    }

    fn set_stop_reason(&self, reason: Option<ActorStopReason>) {
        self.inner.lock().unwrap().stop_reason = reason;
    }

    fn push_message(&self, message: String) {
        self.inner.lock().unwrap().messages_received.push(message);
    }
}

// Test actor for basic functionality
#[derive(Debug)]
struct TestActor {
    name: String,
    context: Option<ActorContext>,
    state: TestActorState,
}

impl TestActor {
    fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            context: None,
            state: TestActorState::new(),
        }
    }

    fn state(&self) -> TestActorState {
        self.state.clone()
    }
}

impl Actor for TestActor {
    fn name(&self) -> &str {
        &self.name
    }

    async fn on_start(&mut self, _context: &ActorContext) {
        self.state.set_started(true);
    }

    async fn on_stop(&mut self, _context: &ActorContext, stop_reason: Option<ActorStopReason>) {
        self.state.set_stopped(true);
        self.state.set_stop_reason(stop_reason);
    }
}

impl ActorHandler<TestMessage> for TestActor {
    type Reply = ();

    async fn handle(&mut self, message: TestMessage) {
        self.state.push_message(message.value);
    }
}

impl ActorHandler<TestRequest> for TestActor {
    type Reply = u32;

    async fn handle(&mut self, message: TestRequest) -> u32 {
        message.value * 2
    }
}
