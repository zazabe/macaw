use crate::lib::*;
use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::path::{Path, PathBuf};

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct RecordFile {
    pub(crate) header: RecordHeader,
    pub(crate) events: Vec<Event<Box<dyn RecordEvent>>>,
}

#[derive(Debug)]
pub(crate) struct EventStore {
    header: RecordHeader,
    events: Vec<Event<Box<dyn RecordEvent>>>,
}

impl EventStore {
    pub(crate) fn new() -> Self {
        Self {
            header: RecordHeader::new(),
            events: Vec::new(),
        }
    }

    /// Load all events from a single YAML document containing a top-level sequence.
    pub(crate) fn from_file<P>(path: P) -> Result<Self, anyhow::Error>
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
    pub(crate) fn save_file<P>(&self, path: P) -> Result<(), anyhow::Error>
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

    pub(crate) fn push(&mut self, id: ProxyId, event: Box<dyn RecordEvent>) {
        self.events.push(Event::new(id, event));
    }

    pub(crate) fn header(&self) -> &RecordHeader {
        &self.header
    }

    pub(crate) fn header_mut(&mut self) -> &mut RecordHeader {
        &mut self.header
    }
}
