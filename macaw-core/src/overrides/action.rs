use crate::lib::*;

use itertools::Itertools;
use regex::Regex;
use serde::de;
use serde::de::*;
use serde::ser::*;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

pub trait TransformAction: Default + PartialEq {
    type Message: Debug;

    fn apply(&self, message: Self::Message) -> Self::Message;
}

pub trait TryTransformAction: Default + PartialEq {
    type Error;
    type Message: Debug;

    fn try_apply(&self, message: Self::Message) -> Result<Self::Message, Self::Error>;
}

impl<T: TransformAction> TryTransformAction for T {
    type Error = anyhow::Error;
    type Message = T::Message;

    fn try_apply(&self, message: Self::Message) -> Result<Self::Message, Self::Error> {
        Ok(self.apply(message))
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum TransformOrIgnoreAction<T: TransformAction> {
    Ignore,
    Transform(T),
}

impl<T: TransformAction> TransformOrIgnoreAction<T> {
    pub fn apply(&self, message: T::Message) -> RuleResult<T::Message> {
        match self {
            TransformOrIgnoreAction::Ignore => RuleResult::SuppressMessage,
            TransformOrIgnoreAction::Transform(action) => {
                RuleResult::MessageTransformed(action.apply(message))
            }
        }
    }
}

impl<T: TransformAction> Default for TransformOrIgnoreAction<T> {
    fn default() -> Self {
        Self::Transform(T::default())
    }
}

#[derive(Deserialize, PartialEq, Debug, Clone)]
#[serde(try_from = "FieldActionRaw")]
#[derive(Default)]
pub enum FieldAction {
    Replace(Option<String>),
    SearchAndReplace(SearchAndReplaceAction),
    #[default]
    NoOperation,
}

impl FieldAction {
    pub fn transform(&self, content: String) -> Option<String> {
        match self {
            FieldAction::Replace(replacement) => replacement.clone(),
            FieldAction::SearchAndReplace(search_and_replace) => {
                Some(search_and_replace.replace(content))
            }
            FieldAction::NoOperation => Some(content),
        }
    }
}

impl Serialize for FieldAction {
    fn serialize<S>(&self, serializer: S) -> Result<<S as Serializer>::Ok, <S as Serializer>::Error>
    where
        S: Serializer,
    {
        match self {
            FieldAction::Replace(replacement) => replacement.serialize(serializer),
            FieldAction::SearchAndReplace(search_and_replace) => {
                search_and_replace.serialize(serializer)
            }
            FieldAction::NoOperation => "".serialize(serializer),
        }
    }
}

#[derive(Serialize, Debug, PartialEq, Clone)]
#[serde(into = "SearchAndReplaceActionRaw")]
pub struct SearchAndReplaceAction {
    search: RegexComparable,
    replace: SearchAndReplaceReplacer,
}

#[derive(Debug, PartialEq, Clone)]
enum SearchAndReplaceReplacer {
    Captures(BTreeMap<String, String>),
    Match(String),
}

impl SearchAndReplaceAction {
    pub(crate) fn replace(&self, text: String) -> String {
        match &self.replace {
            SearchAndReplaceReplacer::Captures(replacer) => {
                match self.search.captures(&text) {
                    Some(captures) => {
                        // ensure replacement are applied from the last capture to the first
                        replacer
                            .iter()
                            .filter_map(|(key, value)| {
                                captures
                                    .name(key)
                                    .map(|capture| capture.range())
                                    .map(|range| (range, value))
                            })
                            .sorted_by_key(|(range, _)| -(range.end as isize))
                            .fold(text, |mut text, (range, replacement)| {
                                text.replace_range(range, replacement);
                                text
                            })
                    }
                    None => text,
                }
            }
            SearchAndReplaceReplacer::Match(replacer) => {
                self.search.replace(&text, replacer.as_str()).into()
            }
        }
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Default, Clone)]
pub struct MapAction {
    #[serde(flatten)]
    actions: BTreeMap<String, FieldAction>,
}

impl MapAction {
    pub fn transform<I>(&self, map: I) -> impl Iterator<Item = (String, Option<String>)>
    where
        I: Iterator<Item = (String, String)>,
    {
        let mut result = Vec::new();
        for (key, value) in map {
            for (action_key, action) in &self.actions {
                if &key == action_key {
                    result.push((key.clone(), action.transform(value.clone())))
                }
            }
        }
        result.into_iter()
    }
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(untagged)]
enum FieldActionRaw {
    Replace(String),
    SearchAndReplace(SearchAndReplaceActionRaw),
    Empty(()),
}

// Required because using serde(untagged) on FieldAction doesn't show the underlying error (e.g. SearchAndReplaceCaptureError)
impl TryFrom<FieldActionRaw> for FieldAction {
    type Error = anyhow::Error;

    fn try_from(raw: FieldActionRaw) -> Result<Self, Self::Error> {
        let action = match raw {
            FieldActionRaw::Replace(replacement) => FieldAction::Replace(Some(replacement)),
            FieldActionRaw::SearchAndReplace(replace_raw) => {
                FieldAction::SearchAndReplace(replace_raw.try_into()?)
            }
            FieldActionRaw::Empty(()) => FieldAction::Replace(None),
        };
        Ok(action)
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct SearchAndReplaceActionRaw {
    search: RegexComparable,
    replace: SearchAndReplaceReplacerRaw,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(untagged)]
enum SearchAndReplaceReplacerRaw {
    Captures(BTreeMap<String, String>),
    Match(String),
}

impl TryFrom<SearchAndReplaceActionRaw> for SearchAndReplaceAction {
    type Error = SearchAndReplaceCaptureError;

    fn try_from(raw: SearchAndReplaceActionRaw) -> Result<Self, Self::Error> {
        let replace = match raw.replace {
            SearchAndReplaceReplacerRaw::Captures(replacer) => {
                let capture_names: HashSet<String> = raw
                    .search
                    .capture_names()
                    .filter_map(|n| n.map(ToString::to_string))
                    .collect();
                let replacement_keys: HashSet<String> =
                    replacer.keys().map(ToString::to_string).collect();
                let missing_keys = &capture_names - &replacement_keys;
                let extra_keys = &replacement_keys - &capture_names;
                if !missing_keys.is_empty() || !extra_keys.is_empty() {
                    return Err(SearchAndReplaceCaptureError {
                        missing_keys: Vec::from_iter(missing_keys),
                        extra_keys: Vec::from_iter(extra_keys),
                    });
                }
                SearchAndReplaceReplacer::Captures(replacer)
            }
            SearchAndReplaceReplacerRaw::Match(replacer) => {
                SearchAndReplaceReplacer::Match(replacer)
            }
        };
        Ok(Self {
            search: raw.search,
            replace,
        })
    }
}

impl From<SearchAndReplaceAction> for SearchAndReplaceActionRaw {
    fn from(action: SearchAndReplaceAction) -> Self {
        let replace_raw = match action.replace {
            SearchAndReplaceReplacer::Captures(replacers) => {
                SearchAndReplaceReplacerRaw::Captures(replacers)
            }
            SearchAndReplaceReplacer::Match(replacer) => {
                SearchAndReplaceReplacerRaw::Match(replacer)
            }
        };
        SearchAndReplaceActionRaw {
            search: action.search,
            replace: replace_raw,
        }
    }
}

#[derive(thiserror::Error, Debug)]
#[error(
    "Replacement keys differ from regex capture names, missing: {:?}, extra: {:?}.",
    missing_keys,
    extra_keys
)]
pub struct SearchAndReplaceCaptureError {
    pub missing_keys: Vec<String>,
    pub extra_keys: Vec<String>,
}

impl<T> Serialize for TransformOrIgnoreAction<T>
where
    T: Serialize + TransformAction,
{
    fn serialize<S>(&self, serializer: S) -> Result<<S as Serializer>::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            TransformOrIgnoreAction::Ignore => "ignore".serialize(serializer),
            TransformOrIgnoreAction::Transform(value) => value.serialize(serializer),
        }
    }
}

/// Deserialize is implemented manually for `TransformOrIgnoreAction` so that deserialization errors
/// of the `Transform(T)` variant are reported.
impl<'de, T> Deserialize<'de> for TransformOrIgnoreAction<T>
where
    T: Deserialize<'de> + TransformAction,
{
    fn deserialize<D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Self, <D as Deserializer<'de>>::Error> {
        struct TransformOrIgnoreActionVisitor<T>(PhantomData<T>);

        impl<'de, T> Visitor<'de> for TransformOrIgnoreActionVisitor<T>
        where
            T: Deserialize<'de> + TransformAction,
        {
            type Value = TransformOrIgnoreAction<T>;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                write!(formatter, "The string 'ignore'")
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                if v == "ignore" {
                    Ok(TransformOrIgnoreAction::Ignore)
                } else {
                    Err(E::custom("Found a string, but not 'ignore'.".to_string()))
                }
            }

            fn visit_map<M>(self, map: M) -> Result<Self::Value, M::Error>
            where
                M: MapAccess<'de>,
            {
                let data = Deserialize::deserialize(value::MapAccessDeserializer::new(map))?;
                Ok(TransformOrIgnoreAction::Transform(data))
            }
        }

        deserializer.deserialize_any(TransformOrIgnoreActionVisitor(PhantomData))
    }
}

// Conversions

#[derive(Serialize, Deserialize, Debug, Clone)]
pub(crate) struct RegexComparable(#[serde(with = "serde_regex")] Regex);

impl RegexComparable {
    pub(crate) fn is_match(&self, text: &str) -> bool {
        self.0.is_match(text)
    }

    pub(crate) fn replace<'t, R: regex::Replacer>(
        &self,
        text: &'t str,
        rep: R,
    ) -> std::borrow::Cow<'t, str> {
        self.0.replace(text, rep)
    }

    pub(crate) fn captures<'t>(&self, text: &'t str) -> Option<regex::Captures<'t>> {
        self.0.captures(text)
    }

    pub(crate) fn capture_names(&self) -> regex::CaptureNames<'_> {
        self.0.capture_names()
    }
}

impl PartialEq for RegexComparable {
    fn eq(&self, other: &Self) -> bool {
        self.0.to_string() == other.0.to_string()
    }
}
