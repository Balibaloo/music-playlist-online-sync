use clap::{Args, Parser, Subcommand};
use std::path::PathBuf;
use tracing_subscriber;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_log::LogTracer;
use tracing_appender::rolling::RollingFileAppender;
use anyhow::Result;
use music_file_playlist_online_sync as lib;
use lib::api::Provider;
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
    /// Auth test helpers
    AuthTest {
        #[command(subcommand)]
        sub: AuthTestCommands,
    },
    /// Show the status of the event queue
    QueueStatus,
    /// Clear all unsynced events from the event queue
    QueueClear,
}

#[derive(Subcommand)]
enum AuthCommands {
    /// Authorize Spotify and store tokens in DB. Requires client_id & client_secret.
    Spotify(SpotifyAuthArgs),
    /// Authorize Tidal and store tokens in DB (interactive)
    Tidal,
}

#[derive(Subcommand)]
enum AuthTestCommands {
    /// Test Spotify authentication and playlist operations
    Spotify,
    /// Test Tidal authentication and playlist operations
    Tidal,
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
        // Initialize log->tracing bridge and structured logging to a daily rolling file in the configured log dir.
        // Keep the _guard alive for the duration of the process so the non-blocking writer flushes on drop.
        use tracing_subscriber::fmt::writer::MakeWriterExt;
        let file_appender: RollingFileAppender = tracing_appender::rolling::daily(&cfg.log_dir, "music-sync.log");
        let (non_blocking, _guard) = tracing_appender::non_blocking(file_appender);
        let stdout = std::io::stdout.with_max_level(tracing::Level::INFO);
        let writer = non_blocking.and(stdout);
        tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .with_writer(writer)
            .finish()
            .try_init()
            .expect("Failed to set tracing subscriber");
        LogTracer::init().ok();

    match cli.command {
        Commands::Watcher => {
            lib::watcher::run_watcher(&cfg)?;
        }
        Commands::Worker => {
            lib::worker::run_worker_once(&cfg).await?;
        }
        Commands::ConfigValidate => {
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
            AuthCommands::Tidal => {
                lib::api::tidal_auth::run_tidal_auth(&cfg).await?;
            }
        },
        Commands::AuthTest { sub } => {
            use std::sync::Arc;
            use futures::future::BoxFuture;
            async fn run_auth_test(
                provider_name: &str,
                list_playlists: BoxFuture<'static, anyhow::Result<Vec<(String, String)>>>,
                ensure_playlist: Box<dyn Fn(String, String) -> BoxFuture<'static, anyhow::Result<String>> + Send + Sync>,
                rename_playlist: Box<dyn Fn(String, String) -> BoxFuture<'static, anyhow::Result<()>> + Send + Sync>,
            ) -> anyhow::Result<()> {
                println!("Listing all user playlists:");
                let playlists = list_playlists.await?;
                for (id, name) in &playlists {
                    println!("- {}: {}", id, name);
                }
                let test_prefix = "Test MFPOS ";
                let config_path = std::env::var("TEST_DB_PATH").ok().unwrap_or_else(|| "etc/music-sync/config.toml".to_string());
                let db_path = if config_path.ends_with(".toml") {
                    match Config::from_path(std::path::Path::new(&config_path)) {
                        Ok(cfg) => cfg.db_path.to_string_lossy().to_string(),
                        Err(_) => config_path.clone(),
                    }
                } else {
                    config_path.clone()
                };
                println!("[DEBUG] Using DB path: {}", db_path);

                // Find the max test playlist number in remote
                let mut max_num = 0;
                for (_id, name) in &playlists {
                    if let Some(rest) = name.strip_prefix(test_prefix) {
                        if let Ok(n) = rest.trim().parse::<u32>() {
                            if n > max_num { max_num = n; }
                        }
                    }
                }


                // Scan mapping DB for all mapped Test MFPOS N playlists, find highest N
                let (mapped_name, mapped_id, mapped_num) = rusqlite::Connection::open(&db_path)
                    .ok()
                    .and_then(|conn| {
                        let mut highest_n = 0;
                        let mut best_name = String::new();
                        let mut best_id = String::new();
                        for n in 1..=max_num.max(1) {
                            let name = format!("{}{}", test_prefix, n);
                            if let Ok(Some(id)) = music_file_playlist_online_sync::db::get_remote_playlist_id(&conn, &name) {
                                if n > highest_n {
                                    highest_n = n;
                                    best_name = name;
                                    best_id = id;
                                }
                            }
                        }
                        if highest_n > 0 {
                            Some((best_name, best_id, highest_n))
                        } else {
                            None
                        }
                    })
                    .unwrap_or((String::new(), String::new(), 0));

                if !mapped_id.is_empty() {
                    // Increment, rename, update mapping
                    let new_num = mapped_num + 1;
                    let new_name = format!("{}{}", test_prefix, new_num);
                    println!("Renaming playlist '{}' to '{}'", mapped_name, new_name);
                    rename_playlist(mapped_id.clone(), new_name.clone()).await?;
                    if let Ok(conn) = rusqlite::Connection::open(&db_path) {
                        let _ = music_file_playlist_online_sync::db::upsert_playlist_map(&conn, &new_name, &mapped_id);
                        println!("[DEBUG] Mapping updated in DB.");
                    }
                    println!("{} auth test complete.", provider_name);
                    return Ok(());
                }

                // If no mapping, create Test MFPOS 1
                let test_name = format!("{}1", test_prefix);
                let desc = format!("Test playlist created by auth test #1");
                println!("Creating playlist: {}", test_name);
                let pid = ensure_playlist(test_name.clone(), desc).await?;
                println!("Created playlist with id: {}", pid);
                if let Ok(conn) = rusqlite::Connection::open(&db_path) {
                    let _ = music_file_playlist_online_sync::db::upsert_playlist_map(&conn, &test_name, &pid);
                    println!("[DEBUG] Mapping created in DB.");
                }
                println!("{} auth test complete.", provider_name);
                Ok(())
            }

            match sub {
                AuthTestCommands::Spotify => {
                    use lib::api::spotify::SpotifyProvider;
                    let db_path = cfg.db_path.clone();
                    // Pass empty strings to load from DB
                    let spotify = Arc::new(SpotifyProvider::new(String::new(), String::new(), db_path));
                    let list_playlists = {
                        let spotify = spotify.clone();
                        Box::pin(async move { spotify.list_user_playlists().await })
                    };
                    let ensure_playlist = {
                        let spotify = spotify.clone();
                        Box::new(move |n: String, d: String| {
                            let spotify = spotify.clone();
                            Box::pin(async move { spotify.ensure_playlist(&n, &d).await }) as BoxFuture<'static, anyhow::Result<String>>
                        })
                    };
                    let rename_playlist = {
                        let spotify = spotify.clone();
                        Box::new(move |id: String, n: String| {
                            let spotify = spotify.clone();
                            Box::pin(async move { spotify.rename_playlist(&id, &n).await }) as BoxFuture<'static, anyhow::Result<()>>
                        })
                    };
                    run_auth_test("Spotify", list_playlists, ensure_playlist, rename_playlist).await?;
                }
                AuthTestCommands::Tidal => {
                    use lib::api::tidal::TidalProvider;
                    let db_path = cfg.db_path.clone();
                    // Pass empty strings to load from DB
                    let tidal = Arc::new(TidalProvider::new(String::new(), String::new(), db_path));
                    let list_playlists = {
                        let tidal = tidal.clone();
                        Box::pin(async move { tidal.list_user_playlists().await })
                    };
                    let ensure_playlist = {
                        let tidal = tidal.clone();
                        Box::new(move |n: String, d: String| {
                            let tidal = tidal.clone();
                            Box::pin(async move { tidal.ensure_playlist(&n, &d).await }) as BoxFuture<'static, anyhow::Result<String>>
                        })
                    };
                    let rename_playlist = {
                        let tidal = tidal.clone();
                        Box::new(move |id: String, n: String| {
                            let tidal = tidal.clone();
                            Box::pin(async move { tidal.rename_playlist(&id, &n).await }) as BoxFuture<'static, anyhow::Result<()>>
                        })
                    };
                    run_auth_test("Tidal", list_playlists, ensure_playlist, rename_playlist).await?;
                }
            }
        }
        Commands::QueueStatus => {
            let db_path = cfg.db_path.clone();
            match rusqlite::Connection::open(&db_path) {
                Ok(conn) => {
                    match music_file_playlist_online_sync::db::fetch_unsynced_events(&conn) {
                        Ok(events) => {
                            println!("Queue contains {} unsynced event(s):", events.len());
                            for event in events {
                                println!("- id: {} | playlist: {} | action: {:?} | track: {:?} | extra: {:?} | synced: {} | ts: {}",
                                    event.id, event.playlist_name, event.action, event.track_path, event.extra, event.is_synced, event.timestamp_ms);
                            }
                        }
                        Err(e) => {
                            eprintln!("Failed to fetch queue events: {}", e);
                            std::process::exit(1);
                        }
                    }
                }
                Err(e) => {
                    eprintln!("Failed to open DB: {}", e);
                    std::process::exit(1);
                }
            }
        }
        Commands::QueueClear => {
            let db_path = cfg.db_path.clone();
            match rusqlite::Connection::open(&db_path) {
                Ok(mut conn) => {
                    match music_file_playlist_online_sync::db::clear_unsynced_events(&mut conn) {
                        Ok(removed) => {
                            println!("Cleared {} unsynced event(s) from the queue.", removed);
                        }
                        Err(e) => {
                            eprintln!("Failed to clear queue events: {}", e);
                            std::process::exit(1);
                        }
                    }
                }
                Err(e) => {
                    eprintln!("Failed to open DB: {}", e);
                    std::process::exit(1);
                }
            }
        }
    }

    Ok(())
}