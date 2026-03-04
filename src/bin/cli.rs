use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use lib::api::Provider;
use lib::config::Config;
use music_file_playlist_online_sync as lib;
use lib::troubleshoot;
use std::path::{Path, PathBuf};
use tracing::subscriber as tracing_subscriber_global;
use tracing_log::LogTracer;
use tracing_subscriber;
use tracing_subscriber::prelude::*;
use tracing_subscriber::{fmt, EnvFilter};

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
    Worker {
        /// Trust the track/playlist cache even if the files have changed on disk.
        /// Useful when running Create events and you know the cache is still valid.
        #[arg(long)]
        trust_cache: bool,
    },
    /// Run a full reconciliation scan of the root folder
    Reconcile,
    /// Reconcile a single playlist folder, optionally restricted to one provider
    ReconcilePlaylist {
        /// Absolute (or root-relative) path to the playlist folder to reconcile
        #[arg(long, value_name = "PATH")]
        path: PathBuf,

        /// Provider to sync to (e.g. "spotify" or "tidal"); omit to sync all configured providers
        #[arg(long, value_name = "PROVIDER")]
        provider: Option<String>,

        /// Trust the track/playlist cache even if local files have changed on disk.
        #[arg(long)]
        trust_cache: bool,
    },
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

        /// Only delete playlists that are NOT referenced in the local playlist_map DB
        #[arg(long)]
        orphan_only: bool,
    },
    /// Troubleshooting helpers
    Troubleshoot {
        #[command(subcommand)]
        sub: TroubleshootCommands,
    },
    /// Database inspection and maintenance helpers
    Db {
        #[command(subcommand)]
        sub: DbCommands,
    },
}

#[derive(Subcommand)]
enum TroubleshootCommands {
    /// File‑related troubleshooting helpers
    File {
        #[command(subcommand)]
        sub: TroubleshootFileCommands,
    },
}

#[derive(Subcommand)]
enum DbCommands {
    /// Inspect and manage the local playlist_map table
    PlaylistMap {
        #[command(subcommand)]
        sub: DbPlaylistMapCommands,
    },
}

#[derive(Subcommand)]
enum DbPlaylistMapCommands {
    /// List all playlist_map entries (optionally filtered by provider)
    List {
        /// Filter to a single provider (e.g. "spotify" or "tidal")
        #[arg(long, value_name = "PROVIDER")]
        provider: Option<String>,
    },
    /// Remove a stale playlist_map entry by provider + playlist key
    Remove {
        /// Provider the entry belongs to
        #[arg(long, value_name = "PROVIDER")]
        provider: String,
        /// Logical playlist name (the key stored in the DB, e.g. "_rooty tooty 3")
        #[arg(long, value_name = "PLAYLIST")]
        playlist: String,
    },
}

#[derive(Subcommand)]
enum TroubleshootFileCommands {
    /// Show detailed information about a local media file (track) or playlist
    Info {
        /// Path to the file to inspect
        #[arg(value_name = "PATH")]
        path: PathBuf,
    },
    /// Lookup the given file with a remote provider and refresh track cache
    Lookup {
        /// Provider name (spotify or tidal)
        #[arg(value_name = "PROVIDER")]
        provider: String,
        /// Path to the local file to resolve
        #[arg(value_name = "PATH")]
        path: PathBuf,
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

    // Initialize log->tracing bridge and structured logging.  We try to
    // create the directory specified in the config so that the rolling file
    // appender doesn't panic on startup; if the directory is unwritable we
    // fall back to stdout-only logging and emit a warning on stderr.
    let _ = LogTracer::init();

    // attempt to make the log directory if it doesn't exist; ignore errors
    // when the directory is not writeable and silently fall back to a no-op
    // layer that writes to `/dev/null`. this keeps the subscriber type
    // consistent regardless of whether we could successfully create the
    // rolling file appender.
    // compute a file logging layer, falling back to a plain stdout-based
    // NonBlocking writer when the rolling appender cannot be created.  we call
    // `non_blocking(std::io::stdout())` each time so that the associated
    // `WorkerGuard` is owned and dropped appropriately.
    // if we have a writable log directory we will create the rolling appender
    // normally; if we can't open the file ourselves we skip calling `daily` to
    // avoid the internal panic that was observed earlier.
    let file_layer = if let Ok(()) = std::fs::create_dir_all(&cfg.log_dir) {
        let probe = cfg.log_dir.join("music-sync.log");
        match std::fs::OpenOptions::new().create(true).append(true).open(&probe) {
            Ok(_) => {
                // since we were able to touch the file, it's reasonably safe to
                // ask the appender to manage it as well.
                let app = tracing_appender::rolling::daily(&cfg.log_dir, "music-sync.log");
                let (non_blocking, _guard) = tracing_appender::non_blocking(app);
                fmt::layer().with_writer(non_blocking)
            }
            Err(e) => {
                eprintln!(
                    "warning: cannot open log file {}: {}; logging to stdout only",
                    probe.display(), e
                );
                let (nb, _guard) = tracing_appender::non_blocking(std::io::stdout());
                fmt::layer().with_writer(nb)
            }
        }
    } else {
        eprintln!(
            "warning: could not create log directory {}: {}; logging to stdout only",
            cfg.log_dir.display(), std::io::Error::new(std::io::ErrorKind::PermissionDenied, "create_dir_all failed")
        );
        let (nb, _guard) = tracing_appender::non_blocking(std::io::stdout());
        fmt::layer().with_writer(nb)
    };

    // Honor RUST_LOG if set, otherwise default to info.
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    let stdout_layer = fmt::layer().with_writer(std::io::stdout);

    let subscriber = tracing_subscriber::registry()
        .with(env_filter)
        .with(stdout_layer)
        .with(file_layer);

    // Install as global default tracing subscriber without triggering
    // tracing-subscriber’s internal log bridge (we already call LogTracer).
    tracing_subscriber_global::set_global_default(subscriber)
        .expect("failed to set global tracing subscriber");

    match cli.command {
        Commands::Watcher => {
            lib::watcher::run_watcher(&cfg).with_context(|| "running watcher".to_string())?;
        }
        Commands::Worker { trust_cache } => {
            lib::worker::run_worker_once(&cfg, None, trust_cache)
                .await
                .with_context(|| "running worker".to_string())?;
        }
        Commands::Reconcile => {
            // 1. Purge any DB-tracked playlists whose local folder is gone.
            lib::worker::purge_deleted_playlists(&cfg)
                .await
                .with_context(|| "purge deleted playlists failed".to_string())?;
            // 2. Scan the folder tree, write local .m3u files, and enqueue Create
            //    events for every playlist.
            lib::worker::run_nightly_reconcile(&cfg)
                .with_context(|| "reconcile scan failed".to_string())?;
            // 3. Drain the event queue so the remote is synced in the same run.
            lib::worker::run_worker_once(&cfg, None, false)
                .await
                .with_context(|| "worker run after reconcile failed".to_string())?;
        }
        Commands::ReconcilePlaylist { path, provider, trust_cache } => {
            lib::worker::reconcile_single_playlist(&cfg, &path, provider.as_deref(), trust_cache)
                .await
                .with_context(|| format!("reconcile-playlist {:?} provider={}", path, provider.as_deref().unwrap_or("<all>")))?;
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
                ensure_playlist: Box<
                    dyn Fn(String, String) -> BoxFuture<'static, anyhow::Result<String>>
                        + Send
                        + Sync,
                >,
                rename_playlist: Box<
                    dyn Fn(String, String) -> BoxFuture<'static, anyhow::Result<()>> + Send + Sync,
                >,
            ) -> anyhow::Result<()> {
                println!("Listing all user playlists:");
                let playlists = list_playlists.await?;
                for (id, name) in &playlists {
                    println!("- {}: {}", id, name);
                }

                let test_prefix = "Test MFPOS ";
                let config_path = std::env::var("TEST_DB_PATH")
                    .ok()
                    .unwrap_or_else(|| "etc/music-sync/config.toml".to_string());
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
                            if n > max_num {
                                max_num = n;
                            }
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
                            if let Ok(Some(id)) =
                                music_file_playlist_online_sync::db::get_remote_playlist_id(
                                    &conn,
                                    provider_name,
                                    &name,
                                )
                            {
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
                                // Migrate the existing mapping from the old logical
                                // name to the new one so the old key is removed and
                                // future runs find the correct highest index.
                                let _ = music_file_playlist_online_sync::db::migrate_playlist_map(
                                    &conn,
                                    provider_name,
                                    &mapped_name,
                                    &new_name,
                                );
                                println!("[DEBUG] Mapping migrated in DB.");
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
                    let _ = music_file_playlist_online_sync::db::upsert_playlist_map(
                        &conn,
                        provider_name,
                        &test_name,
                        &pid,
                    );
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
                    let spotify =
                        Arc::new(SpotifyProvider::new(String::new(), String::new(), db_path, cfg.clone()));
                    let list_playlists = {
                        let spotify = spotify.clone();
                        Box::pin(async move { spotify.list_user_playlists().await })
                    };
                    let ensure_playlist = {
                        let spotify = spotify.clone();
                        Box::new(move |n: String, d: String| {
                            let spotify = spotify.clone();
                            Box::pin(async move { spotify.ensure_playlist(&n, &d).await })
                                as BoxFuture<'static, anyhow::Result<String>>
                        })
                    };
                    let rename_playlist = {
                        let spotify = spotify.clone();
                        Box::new(move |id: String, n: String| {
                            let spotify = spotify.clone();
                            Box::pin(async move { spotify.rename_playlist(&id, &n).await })
                                as BoxFuture<'static, anyhow::Result<()>>
                        })
                    };
                    run_auth_test("Spotify", list_playlists, ensure_playlist, rename_playlist)
                        .await?;
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
                        cfg.clone(),
                    ));

                    // First, explicitly test token refresh so users can see
                    // whether their client_id/client_secret and pasted
                    // token JSON support refresh.
                    println!("Testing Tidal token refresh...");
                    match tidal.test_refresh_token().await {
                        Ok(()) => {
                            println!(
                                "Tidal token refresh succeeded. Proceeding with playlist tests.\n"
                            );
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
                            Box::pin(async move { tidal.ensure_playlist(&n, &d).await })
                                as BoxFuture<'static, anyhow::Result<String>>
                        })
                    };
                    let rename_playlist = {
                        let tidal = tidal.clone();
                        Box::new(move |id: String, n: String| {
                            let tidal = tidal.clone();
                            Box::pin(async move { tidal.rename_playlist(&id, &n).await })
                                as BoxFuture<'static, anyhow::Result<()>>
                        })
                    };
                    run_auth_test("Tidal", list_playlists, ensure_playlist, rename_playlist)
                        .await?;
                }
            }
        }
        Commands::Troubleshoot { sub } => match sub {
            TroubleshootCommands::File { sub } => match sub {
                TroubleshootFileCommands::Info { path } => {
                    troubleshoot::file_info(&cfg, &path)?;
                }
                TroubleshootFileCommands::Lookup { provider, path } => {
                    troubleshoot::file_lookup(&cfg, &provider, &path).await?;
                }
            },
        },
        Commands::QueueStatus => {
            let db_path = cfg.db_path.clone();
            match rusqlite::Connection::open(&db_path) {
                Ok(conn) => match music_file_playlist_online_sync::db::fetch_unsynced_events(&conn)
                {
                    Ok(events) => {
                        for event in &events {
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
                        println!("Queue contains {} unsynced event(s).", events.len());
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
        Commands::DeletePlaylists {
            provider,
            name_regex,
            dry_run,
            orphan_only,
        } => {
            use regex::Regex;
            use std::collections::HashSet;
            use std::sync::Arc;

            let re = match Regex::new(&name_regex) {
                Ok(r) => r,
                Err(e) => {
                    eprintln!("Invalid regex '{}': {}", name_regex, e);
                    std::process::exit(1);
                }
            };

            let provider_lc = provider.to_ascii_lowercase();

            // When --orphan-only is set, load all remote_ids tracked locally
            // so we can exclude them from deletion.
            let tracked_ids: HashSet<String> = if orphan_only {
                match rusqlite::Connection::open(&cfg.db_path) {
                    Ok(conn) => {
                        match music_file_playlist_online_sync::db::get_all_remote_ids_for_provider(&conn, &provider_lc) {
                            Ok(ids) => {
                                println!("Loaded {} locally-tracked remote ID(s) for '{}' (will be excluded).", ids.len(), provider_lc);
                                ids
                            }
                            Err(e) => {
                                eprintln!("Failed to query local playlist_map: {}", e);
                                std::process::exit(1);
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("Failed to open DB: {}", e);
                        std::process::exit(1);
                    }
                }
            } else {
                HashSet::new()
            };

            match provider_lc.as_str() {
                "spotify" => {
                    use lib::api::spotify::SpotifyProvider;

                    let db_path = cfg.db_path.clone();
                    let prov =
                        Arc::new(SpotifyProvider::new(String::new(), String::new(), db_path, cfg.clone()));
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

                    if orphan_only {
                        let before = matches.len();
                        matches.retain(|(id, _name)| !tracked_ids.contains(id));
                        let skipped = before - matches.len();
                        if skipped > 0 {
                            println!("Skipped {} locally-tracked playlist(s).", skipped);
                        }
                    }

                    if matches.is_empty() {
                        println!("No Spotify playlists matched regex '{}'{}", name_regex, if orphan_only { " (after excluding locally-tracked)." } else { "." });
                        return Ok(());
                    }

                    println!("Matched {} Spotify orphan playlist(s):", matches.len());
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
                            eprintln!(
                                "Failed to delete Spotify playlist '{}' ({}): {}",
                                name, id, e
                            );
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
                    let prov = Arc::new(TidalProvider::new(
                        String::new(),
                        String::new(),
                        db_path,
                        root,
                        cfg.clone(),
                    ));
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

                    if orphan_only {
                        let before = matches.len();
                        matches.retain(|(id, _name)| !tracked_ids.contains(id));
                        let skipped = before - matches.len();
                        if skipped > 0 {
                            println!("Skipped {} locally-tracked playlist(s).", skipped);
                        }
                    }

                    if matches.is_empty() {
                        println!("No Tidal playlists matched regex '{}'{}", name_regex, if orphan_only { " (after excluding locally-tracked)." } else { "." });
                        return Ok(());
                    }

                    println!("Matched {} Tidal orphan playlist(s):", matches.len());
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
        Commands::Db { sub } => match sub {
            DbCommands::PlaylistMap { sub } => match sub {
                DbPlaylistMapCommands::List { provider } => {
                    match rusqlite::Connection::open(&cfg.db_path) {
                        Ok(conn) => {
                            match music_file_playlist_online_sync::db::list_playlist_map_entries(
                                &conn,
                                provider.as_deref(),
                            ) {
                                Ok(entries) => {
                                    if entries.is_empty() {
                                        println!("No playlist_map entries found.");
                                    } else {
                                        println!("{:<10} {:<45} {:<40} {}",
                                            "PROVIDER", "PLAYLIST KEY", "REMOTE ID", "DISPLAY NAME");
                                        println!("{}", "-".repeat(130));
                                        for (prov, pl, rid, dn, _synced) in &entries {
                                            println!("{:<10} {:<45} {:<40} {}",
                                                prov,
                                                pl,
                                                rid.as_deref().unwrap_or("(none)"),
                                                dn.as_deref().unwrap_or(""),
                                            );
                                        }
                                        println!("({} row(s))", entries.len());
                                    }
                                }
                                Err(e) => {
                                    eprintln!("Failed to list playlist_map: {}", e);
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
                DbPlaylistMapCommands::Remove { provider, playlist } => {
                    match rusqlite::Connection::open(&cfg.db_path) {
                        Ok(conn) => {
                            match music_file_playlist_online_sync::db::delete_playlist_map(
                                &conn,
                                &provider,
                                &playlist,
                            ) {
                                Ok(()) => {
                                    println!(
                                        "Removed playlist_map entry: provider={} playlist={}",
                                        provider, playlist
                                    );
                                }
                                Err(e) => {
                                    eprintln!("Failed to remove playlist_map entry: {}", e);
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
            },
        },
    }

    Ok(())
}
