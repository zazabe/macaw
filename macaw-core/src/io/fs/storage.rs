use crate::lib::*;
use std::fs::File;
use std::io::{BufReader, BufWriter};
use tokio::fs::File as TokioFile;
use tokio::io::AsyncWriteExt;

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct RecordFile {
    pub(crate) header: RecordHeader,
    pub(crate) events: Vec<Event<Box<dyn RecordEvent>>>,
}

#[derive(Debug)]
pub(crate) struct EventStore {
    header: RecordHeader,
    events: Vec<Event<Box<dyn RecordEvent>>>,
    index: usize,
}

impl EventStore {
    pub(crate) fn new() -> Self {
        Self {
            header: RecordHeader::new(),
            events: Vec::new(),
            index: 0,
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
            index: 0,
        })
    }

    /// Save header and events as a single YAML document with top-level struct asynchronously.
    pub(crate) fn save_file<P>(
        &self,
        path: P,
    ) -> impl Future<Output = Result<(), anyhow::Error>> + Send
    where
        P: AsRef<Path> + Send,
    {
        debug!(
            "Saving {} events to file: {:?}",
            self.events.len(),
            path.as_ref().display()
        );
        let rf = RecordFile {
            header: self.header.clone(),
            events: self.events.clone(),
        };

        async move {
            let ser = serde_yaml::to_string(&rf)?;
            let mut file = TokioFile::create(path.as_ref()).await?;
            file.write_all(ser.as_bytes()).await?;
            file.flush().await?;
            Ok(())
        }
    }

    pub(crate) fn push(&mut self, id: ProxyId, event: Box<dyn RecordEvent>) {
        self.events.push(Event::new(id, event));
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = Event<Box<dyn RecordEvent>>> {
        self.events.iter().cloned()
    }
}
