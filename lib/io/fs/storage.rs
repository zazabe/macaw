use crate::lib::*;
use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct RecordFile {
    pub(crate) header: RecordHeader,
    pub(crate) events: Vec<RecordEvent>,
}

#[derive(Debug, Clone)]
pub(crate) struct EventStore {
    header: RecordHeader,
    events: Vec<RecordEvent>,
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

    pub(crate) fn push_http_request(
        &mut self,
        id: ProxyId,
        req: &HttpRequest,
    ) -> Result<(), anyhow::Error> {
        self.events.push(RecordEvent::HttpRequest(Event::new(
            id,
            HttpRequestEvent::from_request(req)?,
        )));
        Ok(())
    }

    pub(crate) fn push_http_response(
        &mut self,
        id: ProxyId,
        res: &HttpResponse,
    ) -> Result<(), anyhow::Error> {
        self.events.push(RecordEvent::HttpResponse(Event::new(
            id,
            HttpResponseEvent::from_response(res)?,
        )));
        Ok(())
    }

    pub(crate) fn header(&self) -> &RecordHeader {
        &self.header
    }

    pub(crate) fn header_mut(&mut self) -> &mut RecordHeader {
        &mut self.header
    }

    pub(crate) fn push(&mut self, event: RecordEvent) {
        self.events.push(event);
    }

    pub(crate) fn extend<I>(&mut self, iter: I)
    where
        I: IntoIterator<Item = RecordEvent>,
    {
        self.events.extend(iter);
    }

    pub(crate) fn clear(&mut self) {
        self.events.clear();
    }

    pub(crate) fn len(&self) -> usize {
        self.events.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    pub(crate) fn events(&self) -> &[RecordEvent] {
        &self.events
    }

    pub(crate) fn events_mut(&mut self) -> &mut [RecordEvent] {
        &mut self.events
    }

    pub(crate) fn into_inner(self) -> Vec<RecordEvent> {
        self.events
    }
}
