use crate::lib::*;
use std::fs::File;
use std::io::BufReader;
use tokio::fs::File as TokioFile;
use tokio::io::AsyncWriteExt;

#[derive(Debug, Serialize, Deserialize)]
pub struct RecordFile {
    pub(crate) header: RecordHeader,
    pub(crate) events: Vec<RecordedEvent>,
}

impl RecordFile {
    pub fn events_count(&self) -> usize {
        self.events.len()
    }
}

#[derive(Debug)]
pub(crate) struct EventStore {
    header: RecordHeader,
    events: Vec<RecordedEvent>,
}

impl EventStore {
    pub(crate) fn new() -> Self {
        Self {
            header: RecordHeader::new(),
            events: Vec::new(),
        }
    }

    pub(crate) fn events_count(&self) -> usize {
        self.events.len()
    }

    pub(crate) fn duration(&self) -> Option<Duration> {
        let start = self.events.first()?;
        let end = self.events.last()?;
        Some(
            end.timestamp
                .signed_duration_since(start.timestamp)
                .to_std()
                .unwrap(),
        )
    }

    /// Load all events from a single JSON document containing a top-level struct.
    pub(crate) fn from_file<P>(path: P) -> Result<Self, anyhow::Error>
    where
        P: AsRef<Path>,
    {
        let file = File::open(path.as_ref())?;
        let reader = BufReader::new(file);
        let rf: RecordFile = serde_json::from_reader(reader)?;
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

    /// Save header and events as a single JSON document with top-level struct asynchronously.
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
            let ser = serde_json::to_string_pretty(&rf)?;
            let mut file = TokioFile::create(path.as_ref()).await?;
            file.write_all(ser.as_bytes()).await?;
            file.flush().await?;
            Ok(())
        }
    }

    pub(crate) fn push(&mut self, event: RecordedEvent) {
        self.events.push(event);
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = RecordedEvent> {
        self.events.iter().cloned()
    }
}
