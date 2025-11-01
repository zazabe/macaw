#![allow(
    dead_code,
    unused_variables,
    unused_imports,
    clippy::match_single_binding
)]

pub(crate) mod io;
pub(crate) mod macaw;
pub(crate) mod model;
pub(crate) mod parsing;
pub(crate) mod processor;
pub(crate) mod proxy;
pub(crate) mod support;

pub(crate) mod lib {
    pub(crate) use crate::io::*;
    pub(crate) use crate::macaw::*;
    pub(crate) use crate::model::*;
    pub(crate) use crate::parsing::http::*;
    pub(crate) use crate::processor::*;
    pub(crate) use crate::proxy::*;
    pub(crate) use crate::support::*;
    pub(crate) use anyhow::{Result, anyhow};
    pub(crate) use bytes::Bytes;
    pub(crate) use chrono::{DateTime, Utc};
    pub(crate) use itertools::Itertools;
    pub(crate) use serde::{Deserialize, Serialize};
    pub(crate) use std::collections::HashMap;
    pub(crate) use std::fmt;
    pub(crate) use std::net::SocketAddr;
    pub(crate) use std::path::PathBuf;
    pub(crate) use std::pin::Pin;
    pub(crate) use tokio::net::{TcpListener, TcpStream};
    pub(crate) use tokio::sync::{mpsc, oneshot};
    pub(crate) use tracing::{debug, error, info, trace, warn};
    pub(crate) use uuid::Uuid;
}

pub mod prelude {
    pub use crate::macaw::*;
    pub use crate::model::*;
    pub use crate::processor::*;
    pub use crate::proxy::*;
    pub use crate::support::*;
}
