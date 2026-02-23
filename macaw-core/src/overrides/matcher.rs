use crate::lib::*;

use serde::{Deserialize, Serialize};
use std::fmt::Debug;

#[derive(Serialize, Deserialize, PartialEq, Debug, Default, Clone)]
pub struct RegexMatcher(Option<RegexComparable>);

impl RegexMatcher {
    pub fn is_match(&self, value: &str) -> bool {
        match &self.0 {
            Some(regex) => regex.is_match(value),
            None => true,
        }
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Default, Clone)]
pub struct MapMatcher {
    #[serde(flatten)]
    matches: BTreeMap<String, RegexMatcher>,
}

impl MapMatcher {
    pub fn is_match<'a, I>(&self, map: I) -> bool
    where
        I: Iterator<Item = (&'a String, &'a String)>,
    {
        let mut matches = self.matches.clone();
        for (key, value) in map {
            if let Some(regex) = matches.remove(key)
                && !regex.is_match(value)
            {
                return false;
            }
        }
        matches.is_empty()
    }
}
