use std::io::{Read as _, Seek, SeekFrom};
use std::path::PathBuf;

use clap::{Parser, Subcommand};
use tokio_util::sync::CancellationToken;

use cdcflow::config::{Config, OffsetConfig, SinkConfig, SinkMode, SourceConfig};
use cdcflow::error::CdcError;
use cdcflow::metrics::PipelineMetrics;
use cdcflow::offset::memory::MemoryOffsetStore;
use cdcflow::offset::sqlite::SqliteOffsetStore;
use cdcflow::pipeline::Pipeline;
use cdcflow::sink::iceberg::IcebergSink;
use cdcflow::sink::kafka::KafkaSink;
use cdcflow::sink::postgres::PostgresSink;
use cdcflow::sink::stdout::StdoutSink;
use cdcflow::sink::Sink;
use cdcflow::source::mysql::MySqlSource;
use cdcflow::source::postgres::PostgresSource;
use cdcflow::worker_http::registration::AdminClient;
use cdcflow::worker_http::WorkerHttpServer;

#[derive(Parser, Debug)]
#[command(name = "cdcflow", about = "Change Data Capture agent for PostgreSQL and MySQL")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Start the admin dashboard server
    Admin(AdminArgs),
    /// Manage and run jobs
    Job(JobArgs),
}

#[derive(Parser, Debug)]
pub struct RunArgs {
    /// Run in standalone mode with a local config file
    #[arg(long, conflicts_with_all = ["admin_url", "job"])]
    standalone: bool,

    /// Path to the JSON configuration file (required with --standalone)
    #[arg(short, long)]
    config: Option<PathBuf>,

    /// Admin server URL for managed mode
    #[arg(long, conflicts_with = "standalone", requires = "job")]
    admin_url: Option<String>,

    /// Job name to pull config from admin server
    #[arg(long, conflicts_with = "standalone", requires = "admin_url")]
    job: Option<String>,

    /// Port for the worker HTTP server (0 = disabled)
    #[arg(long, default_value = "0")]
    http_port: u16,

    /// Address to advertise to admin server
    #[arg(long)]
    advertise_address: Option<String>,

    /// Heartbeat interval in seconds
    #[arg(long, default_value = "10")]
    heartbeat_interval: u64,
}

#[derive(Parser, Debug)]
pub struct AdminArgs {
    #[command(subcommand)]
    action: AdminAction,
}

#[derive(Subcommand, Debug)]
pub enum AdminAction {
    /// Start the admin server
    Start(AdminStartArgs),
    /// Stop a running admin server
    Stop,
    /// View admin server logs
    Logs(AdminLogsArgs),
}

#[derive(Parser, Debug)]
pub struct AdminStartArgs {
    /// Port to listen on
    #[arg(long, default_value = "8080")]
    port: u16,

    /// SQLite database path
    #[arg(long, default_value = "cdc-admin.db")]
    db_path: String,

    /// Bind address
    #[arg(long, default_value = "0.0.0.0")]
    bind: String,

    /// Heartbeat timeout in seconds
    #[arg(long, default_value = "30")]
    heartbeat_timeout: u64,

    /// Metrics retention in days
    #[arg(long, default_value = "7")]
    metrics_retention_days: u64,
}

#[derive(Parser, Debug)]
pub struct AdminLogsArgs {
    /// List all available log files
    #[arg(long, conflicts_with = "file")]
    list: bool,

    /// Specific log file to view (must be a filename within the logs directory)
    file: Option<String>,
}

#[derive(Parser, Debug)]
pub struct JobArgs {
    #[command(subcommand)]
    action: JobAction,
}

#[derive(Subcommand, Debug)]
pub enum JobAction {
    /// Run a CDC pipeline
    Run(RunArgs),
    /// Create a new job
    Create {
        /// Admin server URL
        #[arg(long)]
        admin_url: String,
        /// Job name
        #[arg(long)]
        name: String,
        /// Path to the JSON pipeline config file
        #[arg(long)]
        config: PathBuf,
        /// Optional description
        #[arg(long, default_value = "")]
        description: String,
    },
    /// List all jobs
    List {
        /// Admin server URL
        #[arg(long)]
        admin_url: String,
    },
    /// Get a job by name
    Get {
        /// Admin server URL
        #[arg(long)]
        admin_url: String,
        /// Job name
        #[arg(long)]
        name: String,
    },
    /// Delete a job by name
    Delete {
        /// Admin server URL
        #[arg(long)]
        admin_url: String,
        /// Job name
        #[arg(long)]
        name: String,
    },
}

fn load_config(path: &std::path::Path) -> Result<Config, CdcError> {
    let content =
        std::fs::read_to_string(path).map_err(|e| CdcError::Config(format!("{path:?}: {e}")))?;
    serde_json::from_str(&content).map_err(|e| CdcError::Config(format!("{path:?}: {e}")))
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    // For `admin start`, write logs to both stdout and a log file under ~/.cdcflow/logs/.
    // For all other commands, log to stdout only.
    let is_admin_start = matches!(
        cli.command,
        Commands::Admin(AdminArgs { action: AdminAction::Start(_) })
    );

    if is_admin_start {
        let log_file = std::fs::File::create(new_log_path())
            .expect("failed to create log file");

        use tracing_subscriber::layer::SubscriberExt;
        use tracing_subscriber::util::SubscriberInitExt;

        let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));

        let stdout_layer = tracing_subscriber::fmt::layer();
        let file_layer = tracing_subscriber::fmt::layer()
            .with_ansi(false)
            .with_writer(std::sync::Mutex::new(log_file));

        tracing_subscriber::registry()
            .with(env_filter)
            .with(stdout_layer)
            .with(file_layer)
            .init();
    } else {
        tracing_subscriber::fmt()
            .with_env_filter(
                tracing_subscriber::EnvFilter::try_from_default_env()
                    .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
            )
            .init();
    }

    // Set up graceful shutdown on Ctrl+C
    let shutdown = CancellationToken::new();
    let shutdown_clone = shutdown.clone();
    tokio::spawn(async move {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to listen for Ctrl+C");
        tracing::info!("received Ctrl+C, shutting down");
        shutdown_clone.cancel();
    });

    let result = match cli.command {
        Commands::Admin(args) => match args.action {
            AdminAction::Start(start_args) => admin_start(start_args, shutdown).await,
            AdminAction::Stop => admin_stop(),
            AdminAction::Logs(logs_args) => admin_logs(logs_args, shutdown).await,
        },
        Commands::Job(args) => job_command(args, shutdown).await,
    };

    match &result {
        Ok(()) => tracing::info!("exited cleanly"),
        Err(e) => {
            eprintln!("fatal error: {e}");
            std::process::exit(1);
        }
    }
}

async fn run_command(args: RunArgs, shutdown: CancellationToken) -> Result<(), CdcError> {
    // Validate: must provide exactly one of --standalone or --admin-url
    if !args.standalone && args.admin_url.is_none() {
        return Err(CdcError::Config(
            "must provide either --standalone or --admin-url (with --job)".into(),
        ));
    }

    // Resolve config: pull from admin or load from file.
    let (config, admin_client, job_id) = if !args.standalone {
        let admin_url = args.admin_url.as_ref().unwrap();
        let job_name = args.job.as_ref().unwrap();
        let worker_id = uuid::Uuid::new_v4().to_string();
        let client = AdminClient::new(admin_url, &worker_id);

        tracing::info!("pulling config for job '{}' from {}", job_name, admin_url);
        let config = client.pull_config(job_name).await?;
        tracing::info!("config pulled successfully");

        // Resolve job name to UUID for worker registration
        let job_client = cdcflow::admin::client::AdminJobClient::new(admin_url);
        let job = job_client.get_job(job_name).await?;
        (config, Some(client), Some(job.id))
    } else {
        let config_path = args.config.ok_or_else(|| {
            CdcError::Config("--config is required with --standalone".into())
        })?;
        let config = load_config(&config_path)?;
        tracing::info!("config loaded from file");
        (config, None, None)
    };

    let metrics = PipelineMetrics::new();

    let worker_addr = if args.http_port > 0 {
        let server =
            WorkerHttpServer::bind(args.http_port, metrics.clone(), shutdown.clone()).await?;
        let addr = server.addr().to_string();
        tracing::info!("worker HTTP server on {}", addr);
        tokio::spawn(async move {
            if let Err(e) = server.start().await {
                tracing::error!("worker HTTP server error: {e}");
            }
        });
        Some(args.advertise_address.unwrap_or(addr))
    } else {
        None
    };

    let _heartbeat_handle = if let Some(ref client) = admin_client {
        let addr = worker_addr.clone().unwrap_or_else(|| "unknown".into());
        let resolved_job_id = job_id.as_ref().unwrap();
        client.register(resolved_job_id, &addr).await?;
        tracing::info!("registered with admin as worker {}", client.worker_id());

        let handle = client.spawn_heartbeat(
            metrics.clone(),
            std::time::Duration::from_secs(args.heartbeat_interval),
            shutdown.clone(),
        );
        Some(handle)
    } else {
        None
    };

    let result = run_pipeline(config, shutdown, Some(metrics)).await;

    if let Some(client) = admin_client {
        if let Err(e) = client.deregister().await {
            tracing::warn!("failed to deregister: {e}");
        }
    }

    result
}

fn cdcflow_dir() -> PathBuf {
    let dir = dirs::home_dir()
        .expect("could not determine home directory")
        .join(".cdcflow");
    std::fs::create_dir_all(&dir).expect("failed to create ~/.cdcflow");
    dir
}

fn pid_file_path() -> PathBuf {
    cdcflow_dir().join("admin.pid")
}

fn logs_dir() -> PathBuf {
    let dir = cdcflow_dir().join("logs");
    std::fs::create_dir_all(&dir).expect("failed to create ~/.cdcflow/logs");
    dir
}

fn new_log_path() -> PathBuf {
    let ts = chrono::Local::now().format("%Y-%m-%dT%H%M%S%.3f");
    logs_dir().join(format!("admin-{ts}.log"))
}

async fn admin_start(args: AdminStartArgs, shutdown: CancellationToken) -> Result<(), CdcError> {
    use cdcflow::admin::{AdminServer, AdminServerConfig};

    // Write PID file so `admin stop` can find us
    let pid_path = pid_file_path();
    let pid = std::process::id();
    std::fs::write(&pid_path, pid.to_string())
        .map_err(|e| CdcError::Config(format!("failed to write pid file: {e}")))?;

    let log_path = new_log_path();
    tracing::info!("PID file: {}", pid_path.display());
    tracing::info!("Logs: {}", log_path.display());

    let server = AdminServer::new(AdminServerConfig {
        bind: args.bind,
        port: args.port,
        db_path: args.db_path,
        heartbeat_timeout_secs: args.heartbeat_timeout,
        metrics_retention_days: args.metrics_retention_days,
    })?;

    // Also handle SIGTERM for `admin stop`
    let shutdown_term = shutdown.clone();
    tokio::spawn(async move {
        let mut sigterm = tokio::signal::unix::signal(
            tokio::signal::unix::SignalKind::terminate(),
        )
        .expect("failed to register SIGTERM handler");
        sigterm.recv().await;
        tracing::info!("received SIGTERM, shutting down");
        shutdown_term.cancel();
    });

    let result = server.start(shutdown).await;

    let _ = std::fs::remove_file(&pid_path);

    result
}

fn admin_stop() -> Result<(), CdcError> {
    let pid_path = pid_file_path();

    if !pid_path.exists() {
        return Err(CdcError::Config(
            "no admin server running (pid file not found)".into(),
        ));
    }

    let contents = std::fs::read_to_string(&pid_path)
        .map_err(|e| CdcError::Config(format!("failed to read pid file: {e}")))?;
    let raw_pid: i32 = contents
        .trim()
        .parse()
        .map_err(|e| CdcError::Config(format!("invalid pid in pid file: {e}")))?;
    let pid = nix::unistd::Pid::from_raw(raw_pid);

    if nix::sys::signal::kill(pid, None).is_err() {
        let _ = std::fs::remove_file(&pid_path);
        tracing::info!("No admin server running (stale pid file cleaned up)");
        return Ok(());
    }

    nix::sys::signal::kill(pid, nix::sys::signal::Signal::SIGTERM)
        .map_err(|e| CdcError::Config(format!("failed to send SIGTERM to pid {raw_pid}: {e}")))?;

    for _ in 0..50 {
        std::thread::sleep(std::time::Duration::from_millis(100));
        if nix::sys::signal::kill(pid, None).is_err() {
            let _ = std::fs::remove_file(&pid_path);
            tracing::info!("Admin server stopped (pid: {raw_pid})");
            return Ok(());
        }
    }

    // Process didn't exit in time
    Err(CdcError::Config(format!(
        "admin server (pid: {raw_pid}) did not exit within 5 seconds"
    )))
}

async fn admin_logs(args: AdminLogsArgs, shutdown: CancellationToken) -> Result<(), CdcError> {
    let logs = logs_dir();

    if args.list {
        return admin_logs_list(&logs);
    }

    let log_path = if let Some(ref file) = args.file {
        // Validate filename has no path separators to prevent path traversal
        if file.contains('/') || file.contains('\\') {
            return Err(CdcError::Config(
                "file argument must be a filename, not a path".into(),
            ));
        }
        let path = logs.join(file);
        if !path.exists() {
            return Err(CdcError::Config(format!(
                "log file not found: {}",
                path.display()
            )));
        }
        path
    } else {
        find_latest_log(&logs)?
    };

    tracing::info!("Tailing {} (Ctrl+C to stop)", log_path.display());
    tail_file(&log_path, shutdown).await
}

fn admin_logs_list(logs_dir: &std::path::Path) -> Result<(), CdcError> {
    let mut entries: Vec<_> = std::fs::read_dir(logs_dir)
        .map_err(|e| CdcError::Config(format!("failed to read logs dir: {e}")))?
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.file_name()
                .to_str()
                .is_some_and(|n| n.starts_with("admin-") && n.ends_with(".log"))
        })
        .collect();

    if entries.is_empty() {
        tracing::info!("No log files found in {}", logs_dir.display());
        return Ok(());
    }

    // Sort by modification time (newest last)
    entries.sort_by_key(|e| e.metadata().and_then(|m| m.modified()).unwrap_or(std::time::SystemTime::UNIX_EPOCH));

    let latest_name = entries.last().map(|e| e.file_name());

    for entry in &entries {
        let name = entry.file_name();
        let suffix = if Some(&name) == latest_name.as_ref() {
            " (latest)"
        } else {
            ""
        };
        println!("{}{}", name.to_string_lossy(), suffix);
    }

    Ok(())
}

fn find_latest_log(logs_dir: &std::path::Path) -> Result<PathBuf, CdcError> {
    let latest = std::fs::read_dir(logs_dir)
        .map_err(|e| CdcError::Config(format!("failed to read logs dir: {e}")))?
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.file_name()
                .to_str()
                .is_some_and(|n| n.starts_with("admin-") && n.ends_with(".log"))
        })
        .max_by_key(|e| {
            e.metadata()
                .and_then(|m| m.modified())
                .unwrap_or(std::time::SystemTime::UNIX_EPOCH)
        });

    match latest {
        Some(entry) => Ok(entry.path()),
        None => Err(CdcError::Config(format!(
            "no log files found in {}",
            logs_dir.display()
        ))),
    }
}

async fn tail_file(path: &std::path::Path, shutdown: CancellationToken) -> Result<(), CdcError> {
    let mut file = std::fs::File::open(path)
        .map_err(|e| CdcError::Config(format!("failed to open {}: {e}", path.display())))?;

    // Seek to end and then tail
    file.seek(SeekFrom::End(0))
        .map_err(|e| CdcError::Config(format!("failed to seek: {e}")))?;

    let mut buf = vec![0u8; 4096];
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => return Ok(()),
            _ = tokio::time::sleep(std::time::Duration::from_millis(100)) => {
                match file.read(&mut buf) {
                    Ok(0) => {} // No new data
                    Ok(n) => {
                        print!("{}", String::from_utf8_lossy(&buf[..n]));
                    }
                    Err(e) => {
                        return Err(CdcError::Config(format!("error reading log: {e}")));
                    }
                }
            }
        }
    }
}

async fn job_command(args: JobArgs, shutdown: CancellationToken) -> Result<(), CdcError> {
    use cdcflow::admin::client::AdminJobClient;

    match args.action {
        JobAction::Run(run_args) => run_command(run_args, shutdown).await?,
        JobAction::Create {
            admin_url,
            name,
            config,
            description,
        } => {
            let client = AdminJobClient::new(&admin_url);
            let config_json = std::fs::read_to_string(&config)
                .map_err(|e| CdcError::Config(format!("{config:?}: {e}")))?;
            // Validate that it's valid JSON
            serde_json::from_str::<serde_json::Value>(&config_json)
                .map_err(|e| CdcError::Config(format!("{config:?}: {e}")))?;
            let job = client.create_job(&name, &config_json, &description).await?;
            println!(
                "{}",
                serde_json::to_string_pretty(&job)
                    .map_err(|e| CdcError::Config(format!("json error: {e}")))?
            );
        }
        JobAction::List { admin_url } => {
            let client = AdminJobClient::new(&admin_url);
            let jobs = client.list_jobs().await?;
            println!(
                "{}",
                serde_json::to_string_pretty(&jobs)
                    .map_err(|e| CdcError::Config(format!("json error: {e}")))?
            );
        }
        JobAction::Get { admin_url, name } => {
            let client = AdminJobClient::new(&admin_url);
            let job = client.get_job(&name).await?;
            println!(
                "{}",
                serde_json::to_string_pretty(&job)
                    .map_err(|e| CdcError::Config(format!("json error: {e}")))?
            );
        }
        JobAction::Delete { admin_url, name } => {
            let client = AdminJobClient::new(&admin_url);
            client.delete_job(&name).await?;
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({"status": "deleted", "name": name}))
                    .map_err(|e| CdcError::Config(format!("json error: {e}")))?
            );
        }
    }

    Ok(())
}

async fn run_with_sink<S: Sink>(
    source_config: SourceConfig,
    offset_config: OffsetConfig,
    sink: S,
    shutdown: CancellationToken,
    metrics: Option<std::sync::Arc<PipelineMetrics>>,
) -> Result<(), CdcError> {
    match source_config {
        SourceConfig::Postgres(pg_config) => match offset_config {
            OffsetConfig::Sqlite { path, key } => {
                let os = SqliteOffsetStore::new(&path, &key)?;
                let mut pipeline = Pipeline::new(PostgresSource::new(pg_config, os.clone()), sink, os);
                if let Some(m) = metrics { pipeline = pipeline.with_metrics(m); }
                pipeline.run(shutdown).await
            }
            OffsetConfig::Memory => {
                let os = MemoryOffsetStore::new();
                let mut pipeline = Pipeline::new(PostgresSource::new(pg_config, os.clone()), sink, os);
                if let Some(m) = metrics { pipeline = pipeline.with_metrics(m); }
                pipeline.run(shutdown).await
            }
        },
        SourceConfig::Mysql(my_config) => match offset_config {
            OffsetConfig::Sqlite { path, key } => {
                let os = SqliteOffsetStore::new(&path, &key)?;
                let mut pipeline = Pipeline::new(MySqlSource::new(my_config, os.clone()), sink, os);
                if let Some(m) = metrics { pipeline = pipeline.with_metrics(m); }
                pipeline.run(shutdown).await
            }
            OffsetConfig::Memory => {
                let os = MemoryOffsetStore::new();
                let mut pipeline = Pipeline::new(MySqlSource::new(my_config, os.clone()), sink, os);
                if let Some(m) = metrics { pipeline = pipeline.with_metrics(m); }
                pipeline.run(shutdown).await
            }
        },
    }
}

async fn run_pipeline(config: Config, shutdown: CancellationToken, metrics: Option<std::sync::Arc<PipelineMetrics>>) -> Result<(), CdcError> {
    if config.mode == SinkMode::Replication {
        match &config.sink {
            SinkConfig::Iceberg(_) | SinkConfig::Postgres(_) => {} // OK
            _ => {
                return Err(CdcError::Config(
                    "replication mode is only supported for the iceberg and postgres sinks".into(),
                ));
            }
        }
    }

    // Derive source connection config for sinks that need schema inference.
    let source_conn = config.source.to_connection_config();

    match config.sink {
        SinkConfig::Stdout => {
            run_with_sink(config.source, config.offset, StdoutSink::new(), shutdown, metrics).await
        }
        SinkConfig::Iceberg(iceberg_config) => {
            let sink = IcebergSink::new(iceberg_config, config.mode, source_conn).await?;
            run_with_sink(config.source, config.offset, sink, shutdown, metrics).await
        }
        SinkConfig::Kafka(kafka_config) => {
            let sink = KafkaSink::new(kafka_config)?;
            run_with_sink(config.source, config.offset, sink, shutdown, metrics).await
        }
        SinkConfig::Postgres(pg_config) => {
            let sink = PostgresSink::new(pg_config, config.mode, Some(source_conn)).await?;
            run_with_sink(config.source, config.offset, sink, shutdown, metrics).await
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;
    use std::io::Write;

    /// Helper to extract RunArgs from a parsed Cli
    fn parse_run_args(args: &[&str]) -> RunArgs {
        let cli = Cli::parse_from(args);
        match cli.command {
            Commands::Job(job_args) => match job_args.action {
                JobAction::Run(run_args) => run_args,
                _ => panic!("expected Run action"),
            },
            _ => panic!("expected Job command"),
        }
    }

    #[test]
    fn test_cli_job_run_standalone() {
        let args = parse_run_args(&[
            "cdcflow", "job", "run", "--standalone", "-c", "config.json",
        ]);
        assert!(args.standalone);
        assert_eq!(args.config.unwrap().to_str().unwrap(), "config.json");
        assert!(args.admin_url.is_none());
        assert!(args.job.is_none());
        assert_eq!(args.http_port, 0);
        assert_eq!(args.heartbeat_interval, 10);
    }

    #[test]
    fn test_cli_job_run_managed() {
        let args = parse_run_args(&[
            "cdcflow", "job", "run",
            "--admin-url", "http://admin:8080",
            "--job", "my-job",
            "--http-port", "9090",
        ]);
        assert!(!args.standalone);
        assert_eq!(args.admin_url.as_deref(), Some("http://admin:8080"));
        assert_eq!(args.job.as_deref(), Some("my-job"));
        assert!(args.config.is_none());
        assert_eq!(args.http_port, 9090);
    }

    #[test]
    fn test_cli_job_run_standalone_conflicts_with_admin_url() {
        let result = Cli::try_parse_from([
            "cdcflow", "job", "run",
            "--standalone",
            "--admin-url", "http://admin:8080",
        ]);
        assert!(result.is_err());
    }

    #[test]
    fn test_cli_job_run_standalone_conflicts_with_job() {
        let result = Cli::try_parse_from([
            "cdcflow", "job", "run",
            "--standalone",
            "--job", "my-job",
        ]);
        assert!(result.is_err());
    }

    #[test]
    fn test_cli_job_run_admin_url_requires_job() {
        let result = Cli::try_parse_from([
            "cdcflow", "job", "run",
            "--admin-url", "http://admin:8080",
        ]);
        assert!(result.is_err());
    }

    #[test]
    fn test_cli_job_run_job_requires_admin_url() {
        let result = Cli::try_parse_from([
            "cdcflow", "job", "run",
            "--job", "my-job",
        ]);
        assert!(result.is_err());
    }

    /// Helper to extract AdminStartArgs from a parsed Cli
    fn parse_admin_start_args(args: &[&str]) -> AdminStartArgs {
        let cli = Cli::parse_from(args);
        match cli.command {
            Commands::Admin(admin_args) => match admin_args.action {
                AdminAction::Start(start_args) => start_args,
                _ => panic!("expected Start action"),
            },
            _ => panic!("expected Admin command"),
        }
    }

    #[test]
    fn test_cli_admin_start_with_options() {
        let args = parse_admin_start_args(&[
            "cdcflow", "admin", "start", "--port", "9090", "--db-path", "test.db",
        ]);
        assert_eq!(args.port, 9090);
        assert_eq!(args.db_path, "test.db");
        assert_eq!(args.bind, "0.0.0.0");
        assert_eq!(args.heartbeat_timeout, 30);
        assert_eq!(args.metrics_retention_days, 7);
    }

    #[test]
    fn test_cli_admin_start_defaults() {
        let args = parse_admin_start_args(&["cdcflow", "admin", "start"]);
        assert_eq!(args.port, 8080);
        assert_eq!(args.db_path, "cdc-admin.db");
        assert_eq!(args.bind, "0.0.0.0");
        assert_eq!(args.heartbeat_timeout, 30);
        assert_eq!(args.metrics_retention_days, 7);
    }

    #[test]
    fn test_cli_admin_logs_default() {
        let cli = Cli::parse_from(["cdcflow", "admin", "logs"]);
        match cli.command {
            Commands::Admin(args) => match args.action {
                AdminAction::Logs(logs_args) => {
                    assert!(!logs_args.list);
                    assert!(logs_args.file.is_none());
                }
                _ => panic!("expected Logs action"),
            },
            _ => panic!("expected Admin command"),
        }
    }

    #[test]
    fn test_find_latest_log_empty_dir() {
        let dir = tempfile::tempdir().unwrap();
        let result = find_latest_log(dir.path());
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("no log files found"));
    }

    #[test]
    fn test_find_latest_log_picks_newest() {
        let dir = tempfile::tempdir().unwrap();
        // Create two log files with a small delay to ensure different mtime
        std::fs::write(dir.path().join("admin-2026-01-01T000000.log"), "old").unwrap();
        std::thread::sleep(std::time::Duration::from_millis(10));
        std::fs::write(dir.path().join("admin-2026-02-01T000000.log"), "new").unwrap();

        let latest = find_latest_log(dir.path()).unwrap();
        assert!(latest.to_string_lossy().contains("2026-02-01"));
    }

    #[test]
    fn test_find_latest_log_ignores_non_admin_files() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("other.log"), "nope").unwrap();
        std::fs::write(dir.path().join("admin-2026-01-01T000000.log"), "yes").unwrap();

        let latest = find_latest_log(dir.path()).unwrap();
        assert!(latest.to_string_lossy().contains("admin-"));
    }

    #[test]
    fn test_new_log_path_format() {
        let path = new_log_path();
        let name = path.file_name().unwrap().to_string_lossy();
        assert!(name.starts_with("admin-"), "log name should start with 'admin-': {name}");
        assert!(name.ends_with(".log"), "log name should end with '.log': {name}");
        // Should contain a timestamp with sub-second precision (dot separator)
        let stem = name.trim_start_matches("admin-").trim_end_matches(".log");
        assert!(stem.contains('.'), "timestamp should have sub-second precision: {stem}");
    }

    #[test]
    fn test_admin_logs_list_empty_dir() {
        let dir = tempfile::tempdir().unwrap();
        // Should not error — just prints "No log files found"
        let result = admin_logs_list(dir.path());
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_admin_logs_path_traversal_rejected() {
        let args = AdminLogsArgs {
            list: false,
            file: Some("../../etc/passwd".to_string()),
        };
        let token = tokio_util::sync::CancellationToken::new();
        let result = admin_logs(args, token).await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("filename, not a path"),
            "expected path traversal error, got: {err}"
        );
    }

    #[test]
    fn test_cli_admin_logs_list_conflicts_with_file() {
        let result = Cli::try_parse_from([
            "cdcflow", "admin", "logs", "--list", "some-file.log",
        ]);
        assert!(result.is_err());
    }

    #[test]
    fn test_cli_legacy_config_flag_errors() {
        // Legacy `cdcflow -c config.json` should no longer work
        let result = Cli::try_parse_from(["cdcflow", "-c", "config.json"]);
        assert!(result.is_err());
    }

    #[test]
    fn test_load_config_valid() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.json");
        let mut f = std::fs::File::create(&path).unwrap();
        write!(
            f,
            r#"{{
            "source": {{
                "type": "postgres",
                "host": "localhost",
                "user": "test",
                "database": "testdb",
                "slot_name": "slot",
                "publication_name": "pub"
            }},
            "sink": {{ "type": "stdout" }},
            "offset": {{ "type": "memory" }}
        }}"#
        )
        .unwrap();

        let config = load_config(&path).unwrap();
        match &config.source {
            SourceConfig::Postgres(pg) => {
                assert_eq!(pg.host, "localhost");
            }
            #[allow(unreachable_patterns)]
            _ => panic!("expected Postgres source config"),
        }
    }

    #[test]
    fn test_load_config_missing_file() {
        let result = load_config(std::path::Path::new("/nonexistent/config.json"));
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), CdcError::Config(_)));
    }

    #[test]
    fn test_load_config_invalid_json() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bad.json");
        std::fs::write(&path, "not json").unwrap();

        let result = load_config(&path);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), CdcError::Config(_)));
    }

    #[test]
    fn test_cli_job_create() {
        let cli = Cli::parse_from([
            "cdcflow", "job",
            "create",
            "--admin-url", "http://localhost:8090",
            "--name", "pg-stdout",
            "--config", "example/configs/pg-to-stdout.json",
            "--description", "test job",
        ]);
        match cli.command {
            Commands::Job(args) => match args.action {
                JobAction::Create { admin_url, name, config, description } => {
                    assert_eq!(admin_url, "http://localhost:8090");
                    assert_eq!(name, "pg-stdout");
                    assert_eq!(config.to_str().unwrap(), "example/configs/pg-to-stdout.json");
                    assert_eq!(description, "test job");
                }
                _ => panic!("expected Create action"),
            },
            _ => panic!("expected Job command"),
        }
    }

    #[test]
    fn test_cli_job_create_default_description() {
        let cli = Cli::parse_from([
            "cdcflow", "job",
            "create",
            "--admin-url", "http://localhost:8090",
            "--name", "test",
            "--config", "config.json",
        ]);
        match cli.command {
            Commands::Job(args) => match args.action {
                JobAction::Create { description, .. } => {
                    assert_eq!(description, "");
                }
                _ => panic!("expected Create action"),
            },
            _ => panic!("expected Job command"),
        }
    }

    #[tokio::test]
    async fn test_run_command_requires_standalone_or_admin_url() {
        let args = RunArgs {
            standalone: false,
            config: Some(PathBuf::from("config.json")),
            admin_url: None,
            job: None,
            http_port: 0,
            advertise_address: None,
            heartbeat_interval: 10,
        };
        let shutdown = CancellationToken::new();
        let result = run_command(args, shutdown).await;
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("--standalone or --admin-url"));
    }

    #[tokio::test]
    async fn test_run_command_standalone_requires_config() {
        let args = RunArgs {
            standalone: true,
            config: None,
            admin_url: None,
            job: None,
            http_port: 0,
            advertise_address: None,
            heartbeat_interval: 10,
        };
        let shutdown = CancellationToken::new();
        let result = run_command(args, shutdown).await;
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("--config is required with --standalone"));
    }

    #[tokio::test]
    async fn test_replication_mode_requires_iceberg_sink() {
        let config = Config {
            source: SourceConfig::Postgres(cdcflow::config::PostgresSourceConfig {
                host: "localhost".into(),
                port: 5432,
                user: "replicator".into(),
                password: None,
                database: "mydb".into(),
                slot_name: "slot".into(),
                publication_name: "pub".into(),
                create_publication: true,
                tables: vec![],
            }),
            sink: SinkConfig::Stdout,
            offset: OffsetConfig::Memory,
            mode: SinkMode::Replication,
        };

        let shutdown = CancellationToken::new();
        let result = run_pipeline(config, shutdown, None).await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, CdcError::Config(_)));
        assert!(err.to_string().contains("replication mode"));
    }

    #[tokio::test]
    async fn test_replication_mode_with_kafka_errors() {
        let config = Config {
            source: SourceConfig::Postgres(cdcflow::config::PostgresSourceConfig {
                host: "localhost".into(),
                port: 5432,
                user: "replicator".into(),
                password: None,
                database: "mydb".into(),
                slot_name: "slot".into(),
                publication_name: "pub".into(),
                create_publication: true,
                tables: vec![],
            }),
            sink: SinkConfig::Kafka(cdcflow::config::KafkaSinkConfig {
                brokers: "localhost:9092".into(),
                topic_prefix: "cdc".into(),
                properties: std::collections::HashMap::new(),
            }),
            offset: OffsetConfig::Memory,
            mode: SinkMode::Replication,
        };

        let shutdown = CancellationToken::new();
        let result = run_pipeline(config, shutdown, None).await;
        assert!(result.is_err());
        assert!(
            result.unwrap_err().to_string().contains("replication mode"),
        );
    }
}
