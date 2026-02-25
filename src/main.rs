//! Macaw CLI - record and replay network traffic.

mod config;
mod debug;
mod record;
mod replay;

use anyhow::Result;
use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "macaw")]
#[command(about = "Record and replay network traffic")]
struct Cli {
    #[arg(short, long, default_value = "config/macaw.toml")]
    config: PathBuf,

    #[arg(short, long)]
    debug: bool,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Start recording (proxies from config)
    Record {
        /// Recording file path
        output_file: PathBuf,
    },
    /// Start replay from recording file
    Replay {
        /// Path to recording file
        recording_file: PathBuf,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .init();

    let cli = Cli::parse();

    let config = &cli.config;
    let debug = cli.debug;

    match cli.command {
        Commands::Record { output_file } => {
            record::run(config, &output_file, debug).await?;
        }
        Commands::Replay { recording_file } => {
            replay::run(config, &recording_file, debug).await?;
        }
    }

    Ok(())
}
