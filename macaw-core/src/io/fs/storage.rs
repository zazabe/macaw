use crate::lib::*;
use std::fs::File;
use std::io::BufReader;
use tokio::fs::File as TokioFile;
use tokio::io::AsyncWriteExt;

pub const RECORD_FORMAT_VERSION: u32 = 2;

/// Versioned, stable DTO for a causal recording.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecordFile {
    pub(crate) format_version: u32,
    pub(crate) header: RecordHeader,
    pub(crate) events: Vec<StoredEvent>,
}

impl RecordFile {
    pub fn events_count(&self) -> usize {
        self.events.len()
    }

    pub fn format_version(&self) -> u32 {
        self.format_version
    }

    fn validate(&self) -> Result<(), anyhow::Error> {
        if self.format_version != RECORD_FORMAT_VERSION {
            anyhow::bail!(
                "Unsupported recording format version {}, expected {}",
                self.format_version,
                RECORD_FORMAT_VERSION
            );
        }

        for (index, event) in self.events.iter().enumerate() {
            let expected = u64::try_from(index)?;
            if event.sequence.get() != expected {
                anyhow::bail!(
                    "Invalid event sequence {}, expected {}",
                    event.sequence.get(),
                    expected
                );
            }
            if event.stream_id.protocol().is_empty() || event.stream_id.id().is_empty() {
                anyhow::bail!(
                    "Event {} has an empty protocol or logical stream ID",
                    event.sequence.get()
                );
            }
            serde_json::from_value::<Box<dyn RecordEvent>>(event.event.clone()).map_err(
                |error| {
                    anyhow::anyhow!(
                        "Event {} has an invalid protocol payload: {}",
                        event.sequence.get(),
                        error
                    )
                },
            )?;
        }

        Ok(())
    }
}

#[derive(Debug)]
pub(crate) struct EventStore {
    header: RecordHeader,
    events: Vec<StoredEvent>,
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
        rf.validate()?;
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
            format_version: RECORD_FORMAT_VERSION,
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

    pub(crate) fn push(&mut self, event: RecordedEvent) -> Result<(), anyhow::Error> {
        let sequence = EventSequence::new(self.events.len() as u64);
        self.events.push(StoredEvent::new(sequence, event)?);
        Ok(())
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = StoredEvent> {
        self.events.iter().cloned()
    }
}
