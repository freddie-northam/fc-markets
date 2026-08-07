use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use fc_market::archive::S3Archive;
use fc_market::config::Config;
use fc_market::ids::RunId;
use fc_market::ingest::{RunOutcome, Runner, fixture::FixtureSource};
use fc_market::{api, backup, db, heartbeat, ingest};
use sqlx::PgPool;
use std::sync::Arc;
use std::time::Duration;
use tracing_subscriber::EnvFilter;

/// One day between dumps. Section 4.8 keeps the last 30 of them.
const BACKUP_INTERVAL: Duration = Duration::from_secs(24 * 60 * 60);

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
    /// Write one database dump to the archive bucket and stop.
    Backup,
    /// Re-parse one run's archived payloads under the current parser.
    ///
    /// This is the one deliberate exception to the append-only rule, and it never
    /// runs automatically.
    Replay {
        /// The run whose archived payloads are re-parsed.
        run_id: uuid::Uuid,
    },
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
            let archive = connect_archive(&config).await?;
            ingest_once(&pool, &archive, &config).await?;
        }
        Command::Backup => {
            let archive = connect_archive(&config).await?;
            backup::run(
                &archive,
                &config.database_url,
                &config.pg_dump_path,
                config.backup_keep,
                chrono::Utc::now(),
            )
            .await?;
        }
        Command::Replay { run_id } => {
            let archive = connect_archive(&config).await?;
            let source = FixtureSource::new(&config.fixture_dir);
            let runner = Runner {
                pool: &pool,
                archive: &archive,
                source: &source,
                config: &config,
            };
            let report = ingest::replay(&runner, RunId(run_id)).await?;
            tracing::info!(
                deleted = report.deleted,
                rewritten = report.rewritten,
                "replay finished"
            );
        }
        Command::Serve => serve(pool, config).await?,
    }

    Ok(())
}

async fn connect_archive(config: &Config) -> Result<S3Archive> {
    S3Archive::connect(
        &config.object_store_endpoint,
        &config.object_store_bucket,
        &config.object_store_access_key,
        &config.object_store_secret_key,
    )
    .await
    .context("cannot reach the archive; ingestion will not start without it")
}

/// One pass, shared by the `ingest` command and the interval task.
///
/// The heartbeat fires here, at the one place a run closes, so neither entry
/// point can forget it.
async fn ingest_once(pool: &PgPool, archive: &S3Archive, config: &Config) -> Result<()> {
    let source = FixtureSource::new(&config.fixture_dir);
    let runner = Runner {
        pool,
        archive,
        source: &source,
        config,
    };

    if let RunOutcome::Completed { status, .. } = ingest::run(&runner).await? {
        heartbeat::send(config.heartbeat_url.as_deref(), status).await;
    }
    Ok(())
}

async fn serve(pool: PgPool, config: Config) -> Result<()> {
    let archive = Arc::new(connect_archive(&config).await?);
    let config = Arc::new(config);

    let ingest_loop = tokio::spawn(ingest_interval(
        pool.clone(),
        archive.clone(),
        config.clone(),
    ));
    let backup_loop = tokio::spawn(backup_interval(archive.clone(), config.clone()));

    let app = api::router(pool, (*config).clone());
    let listener = tokio::net::TcpListener::bind(&config.bind_address).await?;
    tracing::info!(address = %config.bind_address, "listening");

    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
            tracing::info!("shutting down");
        })
        .await?;

    ingest_loop.abort();
    backup_loop.abort();
    Ok(())
}

/// The interval task awaits each run rather than firing and forgetting.
///
/// A panic inside a run becomes a logged failure here. Without this the process
/// would keep serving 200 on every endpoint while ingestion was dead, and the
/// ledger would stop with nothing saying so.
async fn ingest_interval(pool: PgPool, archive: Arc<S3Archive>, config: Arc<Config>) {
    let mut ticker = tokio::time::interval(Duration::from_secs(config.ingest_interval_seconds));
    // A late tick must not make the next ones fire back to back.
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        ticker.tick().await;
        let (pool, archive, config) = (pool.clone(), archive.clone(), config.clone());
        let handle = tokio::spawn(async move { ingest_once(&pool, &archive, &config).await });

        match handle.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => tracing::error!(error = %e, "the scheduled run failed"),
            Err(e) => tracing::error!(error = %e, "the scheduled run panicked"),
        }
    }
}

/// `pg_dump` opens its own connection, so this task needs no pool.
async fn backup_interval(archive: Arc<S3Archive>, config: Arc<Config>) {
    // Starts one interval from now, NOT immediately. A tokio interval fires its
    // first tick straight away, and with restart: unless-stopped a crash loop
    // would then dump on every start: thirty restarts would push the whole
    // thirty day history out through prune and leave thirty copies of one
    // moment. The ingest loop wants its immediate first tick; this one must not
    // have it.
    let mut ticker = tokio::time::interval_at(
        tokio::time::Instant::now() + BACKUP_INTERVAL,
        BACKUP_INTERVAL,
    );
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        ticker.tick().await;
        let result = backup::run(
            &archive,
            &config.database_url,
            &config.pg_dump_path,
            config.backup_keep,
            chrono::Utc::now(),
        )
        .await;
        if let Err(e) = result {
            tracing::error!(error = %e, "the nightly dump failed");
        }
    }
}
