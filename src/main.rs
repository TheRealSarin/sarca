use std::io::{Read, Write};
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use clap::{Args, Parser, Subcommand};
use sarca::auth::hash_token;
use sarca::client::{Client, SendOptions};
use sarca::config::Config;
use sarca::server::Server;
use sarca::wire::Retry;
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(
    name = "sarca",
    version,
    about = "Serialised Arbitrary Remote Control Autonomy"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Args)]
struct Connection {
    #[arg(long, env = "SARCA_URL", default_value = "tcp://127.0.0.1:7878")]
    url: String,
    #[arg(long, env = "SARCA_TOKEN", hide_env_values = true)]
    token: Option<String>,
}

#[derive(Subcommand)]
enum Command {
    Serve {
        #[arg(long, short)]
        config: Option<PathBuf>,
    },
    Send {
        #[command(flatten)]
        conn: Connection,
        target: String,
        action: String,
        #[arg(help = "Payload text; use - to read raw bytes from stdin")]
        payload: Option<String>,
        #[arg(long, conflicts_with = "stream")]
        fire: bool,
        #[arg(long)]
        stream: bool,
        #[arg(long = "cap")]
        caps: Vec<String>,
        #[arg(long, help = "Time to live in milliseconds")]
        ttl: Option<u64>,
        #[arg(long, default_value_t = 1)]
        attempts: u32,
        #[arg(
            long,
            default_value_t = 0,
            help = "Initial retry backoff in milliseconds"
        )]
        backoff: u64,
    },
    Subscribe {
        #[command(flatten)]
        conn: Connection,
        target: String,
        #[arg(default_value = "*")]
        event: String,
    },
    Targets {
        #[command(flatten)]
        conn: Connection,
    },
    HashToken,
}

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_writer(std::io::stderr)
        .init();
    match run(Cli::parse().command).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("error: {err}");
            ExitCode::FAILURE
        }
    }
}

type BoxError = Box<dyn std::error::Error + Send + Sync>;

async fn run(command: Command) -> Result<(), BoxError> {
    match command {
        Command::Serve { config } => serve(config).await,
        Command::Send {
            conn,
            target,
            action,
            payload,
            fire,
            stream,
            caps,
            ttl,
            attempts,
            backoff,
        } => {
            let payload = read_payload(payload)?;
            let opts = SendOptions {
                caps,
                ttl: ttl.map(Duration::from_millis),
                retry: Retry {
                    max_attempts: attempts,
                    backoff_ms: backoff,
                },
            };
            let client = Client::connect_as(&conn.url, conn.token.as_deref(), "sarca-cli").await?;
            let mut stdout = std::io::stdout().lock();
            if fire {
                client.fire(&target, &action, payload, &opts).await?;
            } else if stream {
                let mut items = client.stream(&target, &action, payload, &opts).await?;
                while let Some(item) = items.recv().await {
                    stdout.write_all(&item?)?;
                    stdout.flush()?;
                }
            } else {
                stdout.write_all(&client.request(&target, &action, payload, &opts).await?)?;
            }
            Ok(())
        }
        Command::Subscribe {
            conn,
            target,
            event,
        } => {
            let client = Client::connect_as(&conn.url, conn.token.as_deref(), "sarca-cli").await?;
            let mut events = client.events().ok_or("event stream unavailable")?;
            client.subscribe(&target, &event).await?;
            while let Some(event) = events.recv().await {
                println!(
                    "{}\t{}\t{}",
                    event.target,
                    event.event,
                    String::from_utf8_lossy(&event.payload)
                );
            }
            Err("disconnected from bus".into())
        }
        Command::Targets { conn } => {
            let client = Client::connect_as(&conn.url, conn.token.as_deref(), "sarca-cli").await?;
            for target in client.targets().await? {
                let actions: Vec<&str> = target.actions.iter().map(|a| a.name.as_str()).collect();
                println!(
                    "{}\t{}\tqueued={}\tgroups={}\tactions={}",
                    target.name,
                    if target.online { "online" } else { "offline" },
                    target.queued,
                    target.groups.join(","),
                    actions.join(",")
                );
            }
            Ok(())
        }
        Command::HashToken => {
            let mut token = String::new();
            std::io::stdin().read_to_string(&mut token)?;
            let token = token.trim_end_matches(['\r', '\n']);
            if token.is_empty() {
                return Err("expected a token on stdin".into());
            }
            println!("{}", hash_token(token));
            Ok(())
        }
    }
}

fn read_payload(payload: Option<String>) -> Result<Vec<u8>, BoxError> {
    match payload.as_deref() {
        None => Ok(Vec::new()),
        Some("-") => {
            let mut bytes = Vec::new();
            std::io::stdin().read_to_end(&mut bytes)?;
            Ok(bytes)
        }
        Some(text) => Ok(text.as_bytes().to_vec()),
    }
}

async fn serve(path: Option<PathBuf>) -> Result<(), BoxError> {
    let config = match &path {
        Some(path) => Config::load(path)?,
        None => {
            tracing::warn!(
                "no config given: listening on 127.0.0.1:7878 with anonymous full access"
            );
            Config::default()
        }
    };
    let server = Server::bind(&config, vec![]).await?;
    server.run(shutdown_signal()).await;
    Ok(())
}

async fn shutdown_signal() {
    let mut terminate =
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(signal) => signal,
            Err(err) => {
                tracing::warn!(%err, "cannot listen for SIGTERM");
                let _ = tokio::signal::ctrl_c().await;
                return;
            }
        };
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = terminate.recv() => {}
    }
}
