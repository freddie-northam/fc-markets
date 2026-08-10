mod api;
mod archive;
mod config;
mod db;
mod domain;
mod ids;
mod ingest;
mod valuation;

use anyhow::Result;
use clap::{Parser, Subcommand};
use config::Config;
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(name = "fc-market", about = "FC Market ledger and terminal")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Apply pending migrations and stop.
    Migrate,
    /// Run one ingestion pass and stop.
    Ingest,
    /// Serve the API and run ingestion on an interval.
    Serve,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();

    let cli = Cli::parse();
    let config = Config::from_env()?;
    let pool = db::connect(&config.database_url).await?;

    match cli.command {
        Command::Migrate => {
            db::migrate(&pool).await?;
            tracing::info!("migrations applied");
        }
        Command::Ingest => {
            let reaped = db::reap_abandoned_runs(&pool).await?;
            if reaped > 0 {
                tracing::warn!(reaped, "cleared runs left open by a killed process");
            }
            // Stop rather than wait. The next scheduled tick is the retry.
            match db::try_lock(&pool).await? {
                None => tracing::warn!("another ingest run holds the lock, stopping"),
                Some(_guard) => tracing::info!("ingest lock acquired"),
            }
        }
        Command::Serve => {
            let app = api::router(pool.clone(), config.clone());
            let listener = tokio::net::TcpListener::bind(&config.bind_address).await?;
            tracing::info!(address = %config.bind_address, "listening");
            axum::serve(listener, app).await?;
        }
    }

    Ok(())
}
