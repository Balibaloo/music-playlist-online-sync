use clap::{Parser, Subcommand};
use std::path::{Path, PathBuf};
use tracing_subscriber;
use tracing_subscriber::{fmt, EnvFilter};
use tracing_subscriber::prelude::*;
use tracing_appender::rolling::RollingFileAppender;
use tracing_log::LogTracer;
use tracing::subscriber as tracing_subscriber_global;
use anyhow::{Result, Context};
use music_file_playlist_online_sync as lib;
use lib::api::Provider;
use lib::config::Config;

#[derive(Parser)]
#[command(name = "music-file-playlist-online-sync", version)]
struct Cli {
    /// Path to config TOML
    #[arg(long, value_name = "FILE")]
    config: Option<PathBuf>,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Run the watcher (long-running)
    Watcher,
    /// Run the worker once (one-shot)
    Worker,
    /// Run a full reconciliation scan of the root folder
    Reconcile,
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
    /// Delete remote playlists for a provider whose names match a regex
    DeletePlaylists {
        /// Provider to operate on (e.g. "spotify" or "tidal")
        #[arg(long)]
        provider: String,

        /// Regex used to match playlist names on the provider
        #[arg(long)]
        name_regex: String,

        /// Dry run: list matching playlists but do not delete anything
        #[arg(long)]
        dry_run: bool,
    },
}

#[derive(Subcommand)]
enum AuthCommands {
    /// Authorize Spotify and store tokens in DB (interactive)
    Spotify,
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

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    // Resolve config path: explicit --config overrides; otherwise prefer
    // system-wide /etc/music-sync/config.toml and fall back to the
    // repository example config for local/dev usage.
    let resolved_config_path: PathBuf = match &cli.config {
        Some(p) => p.clone(),
        None => {
            let etc_path = Path::new("/etc/music-sync/config.toml");
            if etc_path.exists() {
                etc_path.to_path_buf()
            } else {
                PathBuf::from("config/example-config.toml")
            }
        }
    };

    let cfg = Config::from_path(&resolved_config_path)
        .with_context(|| format!("loading config from {}", resolved_config_path.display()))?;

    // Initialize log->tracing bridge and structured logging.
    // Logs go to both stdout and a daily-rotated file in cfg.log_dir.
    let _ = LogTracer::init();
    let file_appender: RollingFileAppender = tracing_appender::rolling::daily(&cfg.log_dir, "music-sync.log");
    let (non_blocking, _guard) = tracing_appender::non_blocking(file_appender);

    // Honor RUST_LOG if set, otherwise default to info.
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    let file_layer = fmt::layer().with_writer(non_blocking);
    let stdout_layer = fmt::layer().with_writer(std::io::stdout);

    let subscriber = tracing_subscriber::registry()
        .with(env_filter)
        .with(file_layer)
        .with(stdout_layer);

    // Install as global default tracing subscriber without triggering
    // tracing-subscriber’s internal log bridge (we already call LogTracer).
    tracing_subscriber_global::set_global_default(subscriber)
        .expect("failed to set global tracing subscriber");

    match cli.command {
        Commands::Watcher => {
            lib::watcher::run_watcher(&cfg)
                .with_context(|| "running watcher".to_string())?;
        }
        Commands::Worker => {
            lib::worker::run_worker_once(&cfg).await
                .with_context(|| "running worker".to_string())?;
        }
        Commands::Reconcile => {
            // Nightly reconciliation is synchronous and does not require Tokio.
            if let Err(e) = lib::worker::run_nightly_reconcile(&cfg) {
                eprintln!("Reconcile failed: {}", e);
                std::process::exit(1);
            }
        }
        Commands::ConfigValidate => {
            match lib::config::Config::from_path(&resolved_config_path.as_path()) {
                Ok(_) => println!("OK"),
                Err(e) => {
                    eprintln!("Config validation failed: {}", e);
                    std::process::exit(2);
                }
            }
        }
        Commands::Auth { sub } => match sub {
            AuthCommands::Spotify => {
                lib::api::spotify_auth::run_spotify_auth(&cfg).await?;
            }
            AuthCommands::Tidal => {
                lib::api::tidal_auth::run_tidal_auth(&cfg).await?;
            }
        },
        Commands::AuthTest { sub } => {
            use futures::future::BoxFuture;
            use std::sync::Arc;

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
                            if let Ok(Some(id)) = music_file_playlist_online_sync::db::get_remote_playlist_id(&conn, provider_name, &name) {
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
                    match rename_playlist(mapped_id.clone(), new_name.clone()).await {
                        Ok(()) => {
                            if let Ok(conn) = rusqlite::Connection::open(&db_path) {
                                let _ = music_file_playlist_online_sync::db::upsert_playlist_map(&conn, provider_name, &new_name, &mapped_id);
                                println!("[DEBUG] Mapping updated in DB.");
                            }
                            println!("{} auth test complete.", provider_name);
                            return Ok(());
                        }
                        Err(e) => {
                            eprintln!("[WARN] Failed to rename existing {} test playlist (it may have been deleted): {}", provider_name, e);
                            // Fall through to creating a fresh test playlist and mapping
                        }
                    }
                }

                // If no mapping, create Test MFPOS 1
                let test_name = format!("{}1", test_prefix);
                let desc = format!("Test playlist created by auth test #1");
                println!("Creating playlist: {}", test_name);
                let pid = ensure_playlist(test_name.clone(), desc).await?;
                println!("Created playlist with id: {}", pid);
                if let Ok(conn) = rusqlite::Connection::open(&db_path) {
                    let _ = music_file_playlist_online_sync::db::upsert_playlist_map(&conn, provider_name, &test_name, &pid);
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
                    // Pass empty strings to load from DB; propagate any configured
                    // online_root_playlist so auth tests reflect real behavior.
                    let tidal = Arc::new(TidalProvider::new(
                        String::new(),
                        String::new(),
                        db_path,
                        if cfg.online_root_playlist.trim().is_empty() {
                            None
                        } else {
                            Some(cfg.online_root_playlist.clone())
                        },
                    ));

                    // First, explicitly test token refresh so users can see
                    // whether their client_id/client_secret and pasted
                    // token JSON support refresh.
                    println!("Testing Tidal token refresh...");
                    match tidal.test_refresh_token().await {
                        Ok(()) => {
                            println!("Tidal token refresh succeeded. Proceeding with playlist tests.\n");
                        }
                        Err(e) => {
                            eprintln!("Tidal token refresh FAILED: {}", e);
                            return Err(e);
                        }
                    }

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
                Ok(conn) => match music_file_playlist_online_sync::db::fetch_unsynced_events(&conn) {
                    Ok(events) => {
                        println!("Queue contains {} unsynced event(s):", events.len());
                        for event in events {
                            println!(
                                "- id: {} | playlist: {} | action: {:?} | track: {:?} | extra: {:?} | synced: {} | ts: {}",
                                event.id,
                                event.playlist_name,
                                event.action,
                                event.track_path,
                                event.extra,
                                event.is_synced,
                                event.timestamp_ms
                            );
                        }
                    }
                    Err(e) => {
                        eprintln!("Failed to fetch queue events: {}", e);
                        std::process::exit(1);
                    }
                },
                Err(e) => {
                    eprintln!("Failed to open DB: {}", e);
                    std::process::exit(1);
                }
            }
        }
        Commands::QueueClear => {
            let db_path = cfg.db_path.clone();
            match rusqlite::Connection::open(&db_path) {
                Ok(mut conn) => match music_file_playlist_online_sync::db::clear_unsynced_events(&mut conn) {
                    Ok(removed) => {
                        println!("Cleared {} unsynced event(s) from the queue.", removed);
                    }
                    Err(e) => {
                        eprintln!("Failed to clear queue events: {}", e);
                        std::process::exit(1);
                    }
                },
                Err(e) => {
                    eprintln!("Failed to open DB: {}", e);
                    std::process::exit(1);
                }
            }
        }
        Commands::DeletePlaylists { provider, name_regex, dry_run } => {
            use regex::Regex;
            use std::sync::Arc;

            let re = match Regex::new(&name_regex) {
                Ok(r) => r,
                Err(e) => {
                    eprintln!("Invalid regex '{}': {}", name_regex, e);
                    std::process::exit(1);
                }
            };

            let provider_lc = provider.to_ascii_lowercase();

            match provider_lc.as_str() {
                "spotify" => {
                    use lib::api::spotify::SpotifyProvider;

                    let db_path = cfg.db_path.clone();
                    let prov = Arc::new(SpotifyProvider::new(String::new(), String::new(), db_path));
                    if !prov.is_authenticated() {
                        eprintln!("Spotify provider is not authenticated. Run auth first.");
                        std::process::exit(1);
                    }

                    let playlists = match prov.list_user_playlists().await {
                        Ok(p) => p,
                        Err(e) => {
                            eprintln!("Failed to list Spotify playlists: {}", e);
                            std::process::exit(1);
                        }
                    };

                    let mut matches: Vec<(String, String)> = playlists
                        .into_iter()
                        .filter(|(_id, name)| re.is_match(name))
                        .collect();

                    if matches.is_empty() {
                        println!("No Spotify playlists matched regex '{}'.", name_regex);
                        return Ok(());
                    }

                    println!("Matched {} Spotify playlist(s):", matches.len());
                    for (id, name) in &matches {
                        println!("- {} ({})", name, id);
                    }

                    if dry_run {
                        println!("Dry run: no playlists were deleted.");
                        return Ok(());
                    }

                    let mut failed = 0usize;
                    for (id, name) in matches.drain(..) {
                        println!("Deleting playlist '{}' ({})...", name, id);
                        if let Err(e) = prov.delete_playlist(&id).await {
                            eprintln!("Failed to delete Spotify playlist '{}' ({}): {}", name, id, e);
                            failed += 1;
                        }
                    }

                    if failed > 0 {
                        eprintln!(
                            "Completed with {} failure(s) while deleting Spotify playlists.",
                            failed
                        );
                        std::process::exit(1);
                    }

                    println!("All matched Spotify playlists deleted successfully.");
                }
                "tidal" => {
                    use lib::api::tidal::TidalProvider;

                    let db_path = cfg.db_path.clone();
                    let root = if cfg.online_root_playlist.trim().is_empty() {
                        None
                    } else {
                        Some(cfg.online_root_playlist.clone())
                    };
                    let prov = Arc::new(TidalProvider::new(String::new(), String::new(), db_path, root));
                    if !prov.is_authenticated() {
                        eprintln!("Tidal provider is not authenticated. Run auth first.");
                        std::process::exit(1);
                    }

                    let playlists = match prov.list_user_playlists().await {
                        Ok(p) => p,
                        Err(e) => {
                            eprintln!("Failed to list Tidal playlists: {}", e);
                            std::process::exit(1);
                        }
                    };

                    let mut matches: Vec<(String, String)> = playlists
                        .into_iter()
                        .filter(|(_id, name)| re.is_match(name))
                        .collect();

                    if matches.is_empty() {
                        println!("No Tidal playlists matched regex '{}'.", name_regex);
                        return Ok(());
                    }

                    println!("Matched {} Tidal playlist(s):", matches.len());
                    for (id, name) in &matches {
                        println!("- {} ({})", name, id);
                    }

                    if dry_run {
                        println!("Dry run: no playlists were deleted.");
                        return Ok(());
                    }

                    let mut failed = 0usize;
                    for (id, name) in matches.drain(..) {
                        println!("Deleting playlist '{}' ({})...", name, id);
                        if let Err(e) = prov.delete_playlist(&id).await {
                            eprintln!("Failed to delete Tidal playlist '{}' ({}): {}", name, id, e);
                            failed += 1;
                        }
                    }

                    if failed > 0 {
                        eprintln!(
                            "Completed with {} failure(s) while deleting Tidal playlists.",
                            failed
                        );
                        std::process::exit(1);
                    }

                    println!("All matched Tidal playlists deleted successfully.");
                }
                other => {
                    eprintln!(
                        "Unknown provider '{}'. Expected 'spotify' or 'tidal'.",
                        other
                    );
                    std::process::exit(1);
                }
            }
        }
    }

    Ok(())
}