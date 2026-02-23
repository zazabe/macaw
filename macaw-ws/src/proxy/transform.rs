use itertools::Either;

use crate::lib::*;

#[derive(Deserialize, Debug)]
#[serde(from = "WsOverrideRulesRaw")]
pub struct WsOverrideRules {
    upstream: OverrideRulesChain<WsMessageRule>,
    downstream: OverrideRulesChain<WsMessageRule>,
}

impl WsOverrideRules {
    pub fn from_file(path: &Path) -> Result<Self, anyhow::Error> {
        parse_json_or_yaml(path)
    }
}

impl WsOverride for WsOverrideRules {
    fn ws_upstream_override_event(&self, event: WsEvent) -> Option<WsEvent> {
        match event {
            WsEvent::Message(WsMessageEvent { message }) => {
                match self.upstream.apply_rules(message, ()) {
                    OverrideOutput::Suppress => None,
                    OverrideOutput::Message(message) => {
                        Some(WsEvent::Message(WsMessageEvent { message }))
                    }
                }
            }
            event => Some(event),
        }
    }

    fn ws_downstream_override_event(&self, event: WsEvent) -> Option<WsEvent> {
        match event {
            WsEvent::Message(WsMessageEvent { message }) => {
                match self.downstream.apply_rules(message, ()) {
                    OverrideOutput::Suppress => None,
                    OverrideOutput::Message(message) => {
                        Some(WsEvent::Message(WsMessageEvent { message }))
                    }
                }
            }
            event => Some(event),
        }
    }
}

impl From<WsOverrideRulesRaw> for WsOverrideRules {
    fn from(raw: WsOverrideRulesRaw) -> Self {
        let (upstream, downstream): (Vec<WsMessageRule>, Vec<WsMessageRule>) =
            raw.0.into_iter().partition_map(|rule| match rule {
                WsOverrideRuleRaw::WsUpstreamMessage(rule) => Either::Left(rule),
                WsOverrideRuleRaw::WsDownstreamMessage(rule) => Either::Right(rule),
            });
        Self {
            upstream: OverrideRulesChain::from_iter(upstream),
            downstream: OverrideRulesChain::from_iter(downstream),
        }
    }
}

#[derive(Deserialize)]
struct WsOverrideRulesRaw(Vec<WsOverrideRuleRaw>);

#[derive(Deserialize)]
enum WsOverrideRuleRaw {
    WsUpstreamMessage(WsMessageRule),
    WsDownstreamMessage(WsMessageRule),
}

pub trait WsOverride: Send + Sync {
    fn ws_upstream_override_event(&self, event: WsEvent) -> Option<WsEvent>;

    fn ws_downstream_override_event(&self, event: WsEvent) -> Option<WsEvent>;
}

impl fmt::Debug for Box<dyn WsOverride> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "WsOverride")?;
        Ok(())
    }
}

pub struct NoopWsOverride;

impl WsOverride for NoopWsOverride {
    fn ws_upstream_override_event(&self, event: WsEvent) -> Option<WsEvent> {
        Some(event)
    }

    fn ws_downstream_override_event(&self, event: WsEvent) -> Option<WsEvent> {
        Some(event)
    }
}

impl Default for Box<dyn WsOverride> {
    fn default() -> Self {
        Box::new(NoopWsOverride)
    }
}

/// Redact WebSocket events before they are recorded or compared to recorded events.
/// Use case: redact nondeterministic parts, remove sensitive data, etc...
#[dyn_clonable::clonable]
pub trait WsRedact: Clone + Send + Sync {
    fn ws_redact_event(&self, event: WsEvent) -> WsEvent {
        event
    }
}

impl fmt::Debug for Box<dyn WsRedact> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "WsRedact")?;
        Ok(())
    }
}

#[derive(Clone)]
pub struct NoopWsRedact;

impl WsRedact for NoopWsRedact {}

impl Default for Box<dyn WsRedact> {
    fn default() -> Self {
        Box::new(NoopWsRedact)
    }
}

/// Transform WebSocket events when they enter or leave Macaw in destination of a remote client/server.
/// Use case: encrypt/decrypt messages, custom compression/decompression of the message, etc...
#[dyn_clonable::clonable]
pub trait WsTransform: Send + Sync + Clone {
    fn encode_event(&self, event: WsEvent) -> Result<WsEvent, anyhow::Error> {
        Ok(event)
    }

    fn decode_event(&self, event: WsEvent) -> Result<WsEvent, anyhow::Error> {
        Ok(event)
    }
}

impl fmt::Debug for Box<dyn WsTransform> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "WsTransform")?;
        Ok(())
    }
}

#[derive(Clone)]
pub struct NoopWsTransform;

impl WsTransform for NoopWsTransform {}

impl Default for Box<dyn WsTransform> {
    fn default() -> Self {
        Box::new(NoopWsTransform)
    }
}
