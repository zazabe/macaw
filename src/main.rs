//! Macaw CLI - record and replay network traffic.

mod client;
mod serve;

use anyhow::Result;
use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "macaw")]
#[command(about = "Record and replay network traffic")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Run the versioned HTTP control server
    Serve(serve::ServeArgs),
    /// Control a running Macaw server
    #[command(alias = "ctl")]
    Client(client::ClientArgs),
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Serve(args) => {
            serve::run(args).await?;
        }
        Commands::Client(args) => {
            client::run(args).await?;
        }
    }

    Ok(())
}
