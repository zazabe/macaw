use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::path::{Path, PathBuf};
use std::rc::Rc;
use tracing::debug;

use crate::model::{Event, RecordEvent, RecordHeader};
use crate::processor::proxy::ProxyId;

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct RecordFile {
    pub(crate) header: RecordHeader,
    pub(crate) events: Vec<Event<Box<dyn RecordEvent>>>,
}

#[derive(Debug, Clone)]
pub(crate) struct EventStore {
    inner: Rc<RefCell<EventStoreInner>>,
}

impl EventStore {
    pub(crate) fn new() -> Self {
        Self {
            inner: Rc::new(RefCell::new(EventStoreInner::new())),
        }
    }

    /// Load all events from a single YAML document containing a top-level sequence.
    pub(crate) fn from_file<P>(path: P) -> Result<Self, anyhow::Error>
    where
        P: AsRef<Path>,
    {
        let inner = EventStoreInner::from_file(path)?;
        Ok(Self {
            inner: Rc::new(RefCell::new(inner)),
        })
    }

    /// Save header and events as a single YAML document with top-level struct.
    pub(crate) fn save_file<P>(&self, path: P) -> Result<(), anyhow::Error>
    where
        P: AsRef<Path>,
    {
        self.inner.borrow().save_file(path)?;
        Ok(())
    }

    pub(crate) fn push<E: RecordEvent>(&self, id: ProxyId, event: E) {
        self.inner.borrow_mut().push(id, event);
    }
}

#[derive(Debug)]
struct EventStoreInner {
    header: RecordHeader,
    events: Vec<Event<Box<dyn RecordEvent>>>,
}

impl EventStoreInner {
    fn new() -> Self {
        Self {
            header: RecordHeader::new(),
            events: Vec::new(),
        }
    }

    /// Load all events from a single YAML document containing a top-level sequence.
    fn from_file<P>(path: P) -> Result<Self, anyhow::Error>
    where
        P: AsRef<Path>,
    {
        let file = File::open(path.as_ref())?;
        let reader = BufReader::new(file);
        let rf: RecordFile = serde_yaml::from_reader(reader)?;
        debug!(
            "Loaded {} events from file: {}",
            rf.events.len(),
            path.as_ref().display()
        );
        Ok(Self {
            header: rf.header,
            events: rf.events,
        })
    }

    /// Save header and events as a single YAML document with top-level struct.
    fn save_file<P>(&self, path: P) -> Result<(), anyhow::Error>
    where
        P: AsRef<Path>,
    {
        debug!(
            "Saving {} events to file: {:?}",
            self.events.len(),
            path.as_ref().display()
        );
        let file = File::create(path.as_ref())?;
        let writer = BufWriter::new(file);
        let rf = RecordFile {
            header: self.header.clone(),
            events: self.events.clone(),
        };
        serde_yaml::to_writer(writer, &rf)?;
        Ok(())
    }

    fn push<E: RecordEvent>(&mut self, id: ProxyId, event: E) {
        self.events.push(Event::new(id, Box::new(event)));
    }

    fn header(&self) -> &RecordHeader {
        &self.header
    }
}
