use crate::lib::*;

use serde::Serialize;
use std::fmt::Debug;
use std::sync::atomic::{self, AtomicUsize};

// #####

#[derive(Debug, PartialEq)]
pub enum OverrideOutput<M: Debug> {
    Suppress,
    Message(M),
}

// #####

#[derive(Debug)]
pub struct OverrideRulesChain<R: Rule> {
    rules: Vec<OverrideRulesChainItem<R>>,
}

impl<R: Rule> OverrideRulesChain<R> {
    fn new() -> Self {
        Self { rules: Vec::new() }
    }

    pub fn apply_rules(
        &self,
        mut message: R::Message,
        context: R::MessageContext,
    ) -> OverrideOutput<R::Message> {
        for rule in &self.rules {
            message = match rule.apply(message, &context) {
                OverrideOutput::Suppress => return OverrideOutput::Suppress,
                OverrideOutput::Message(message) => message,
            };
        }
        OverrideOutput::Message(message)
    }

    pub fn get_unmatched_rules(&self) -> Vec<R> {
        self.rules
            .iter()
            .filter(|item| !item.is_optional() && !item.has_matches())
            .map(|item| item.rule.clone())
            .collect()
    }
}

impl<R: Rule> FromIterator<R> for OverrideRulesChain<R> {
    fn from_iter<T: IntoIterator<Item = R>>(iter: T) -> Self {
        Self {
            rules: iter
                .into_iter()
                .map(|rule| OverrideRulesChainItem::new(rule))
                .collect(),
        }
    }
}

impl<R: Rule> Default for OverrideRulesChain<R> {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug)]
struct OverrideRulesChainItem<R: Rule> {
    rule: R,
    match_count: Arc<AtomicUsize>,
}

impl<R: Rule> OverrideRulesChainItem<R> {
    fn new(rule: R) -> Self {
        Self {
            rule,
            match_count: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn apply(
        &self,
        message: R::Message,
        context: &R::MessageContext,
    ) -> OverrideOutput<R::Message> {
        match self.rule.apply(message, context) {
            RuleResult::SuppressMessage => {
                self.match_count.fetch_add(1, atomic::Ordering::SeqCst);
                OverrideOutput::Suppress
            }
            RuleResult::MessageTransformed(message) => {
                self.match_count.fetch_add(1, atomic::Ordering::SeqCst);
                OverrideOutput::Message(message)
            }
            RuleResult::NoMatch(message) => OverrideOutput::Message(message),
        }
    }

    fn is_optional(&self) -> bool {
        self.rule.is_optional()
    }

    fn has_matches(&self) -> bool {
        self.match_count.load(atomic::Ordering::SeqCst) > 0
    }
}

// ##### Rule

pub trait Rule: Debug + Send + Sync + Clone + Serialize {
    type Message: Debug;
    type MessageContext;

    fn apply(
        &self,
        message: Self::Message,
        context: &Self::MessageContext,
    ) -> RuleResult<Self::Message>;

    fn is_optional(&self) -> bool;
}

#[derive(Debug, PartialEq)]
pub enum RuleResult<M: Debug> {
    SuppressMessage,
    MessageTransformed(M),
    NoMatch(M),
}
