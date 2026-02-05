use clap::{Args, Parser, Subcommand};
use std::path::PathBuf;
use tracing_subscriber;
use tracing_appender::rolling::{RollingFileAppender, Rotation};
use anyhow::Result;

use music_file_playlist_online_sync as lib;
use lib::config::Config;

#[derive(Parser)]
#[command(name = "music-file-playlist-online-sync", version)]
struct Cli {
    /// Path to config TOML
    #[arg(long, value_name = "FILE", default_value = "config/example-config.toml")]
    config: PathBuf,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Run the watcher (long-running)
    Watcher,
    /// Run the worker once (one-shot)
    Worker,
    /// Validate config file and exit
    ConfigValidate,
    /// Auth helpers
    Auth {
        #[command(subcommand)]
        sub: AuthCommands,
    },
}

#[derive(Subcommand)]
enum AuthCommands {
    /// Authorize Spotify and store tokens in DB. Requires client_id & client_secret.
    Spotify(SpotifyAuthArgs),
}

#[derive(Args)]
struct SpotifyAuthArgs {
    #[arg(long)]
    client_id: String,
    #[arg(long)]
    client_secret: String,
    /// Redirect URI you registered with Spotify (for manual flow, set to something like http://localhost/)
    #[arg(long, default_value = "http://localhost/")]
    redirect_uri: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let cfg = Config::from_path(&cli.config)?;

    // Initialize structured logging to a daily rolling file in the configured log dir.
    // Keep the _guard alive for the duration of the process so the non-blocking writer flushes on drop.
    let file_appender: RollingFileAppender = tracing_appender::rolling::daily(&cfg.log_dir, "music-sync.log");
    let (non_blocking, _guard) = tracing_appender::non_blocking(file_appender);
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_writer(non_blocking)
        .init();

    match cli.command {
        Commands::Watcher => {
            // blocking run
            lib::watcher::run_watcher(&cfg)?;
        }
        Commands::Worker => {
            lib::worker::run_worker_once(&cfg).await?;
        }
        Commands::ConfigValidate => {
            // parse and validate config
            match lib::config::Config::from_path(&cli.config.as_path()) {
                Ok(_) => println!("OK"),
                Err(e) => {
                    eprintln!("Config validation failed: {}", e);
                    std::process::exit(2);
                }
            }
        }
        Commands::Auth { sub } => match sub {
            AuthCommands::Spotify(args) => {
                lib::api::spotify_auth::run_spotify_auth(&args.client_id, &args.client_secret, &args.redirect_uri, &cfg).await?;
            }
        },
    }

    Ok(())
}