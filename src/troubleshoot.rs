use crate::config::Config;
use crate::db;
use crate::util;
use anyhow::{Context, Result};
use chrono::Utc;
use std::path::Path;

/// TTL used for negative track cache entries (same constant as in worker.rs)
const NEGATIVE_CACHE_TTL_SECS: i64 = 30 * 24 * 3600;

/// Print a bunch of information about a local file that may be relevant when
/// troubleshooting sync problems.  The current implementation focuses on the
/// track cache: it will show extracted ISRC, any cache entries for the hard-
/// coded providers (spotify/tidal), and whether the file exists on disk.  If
/// no cache entry exists the output makes that clear.  We avoid performing any
/// network lookups so this command can be run even when offline.

pub fn file_info(cfg: &Config, path: &Path) -> Result<()> {
    println!("=== file info for {} ===", path.display());

    if path.exists() {
        println!("exists: yes");
        if let Ok(meta) = path.metadata() {
            println!("  size: {}", meta.len());
            if let Ok(mtime) = meta.modified() {
                println!("  mtime: {:?}", mtime);
            }
        }
        if let Some(isrc) = util::extract_isrc_from_path(path) {
            println!("  extracted isrc: {isrc}");
        } else {
            println!("  extracted isrc: <none>");
        }
    } else {
        println!("exists: no");
    }

    // open database once
    let conn = rusqlite::Connection::open(&cfg.db_path)
        .with_context(|| format!("opening database at {}", cfg.db_path.display()))?;

    // examine track cache for each supported provider
    for provider in ["spotify", "tidal"].iter() {
        match db::get_track_cache_by_local(&conn, provider, &path.display().to_string()) {
            Ok(Some((isrc_opt, remote_opt, resolved_at))) => {
                println!("\ntrack cache (provider={})", provider);
                println!("  isrc: {:?}", isrc_opt);
                println!("  remote_id/url: {:?}", remote_opt);
                let now = Utc::now().timestamp();
                let age = now - resolved_at;
                println!("  resolved_at: {} ({age}s ago)", resolved_at);
                if remote_opt.is_none() {
                    if age < NEGATIVE_CACHE_TTL_SECS {
                        println!(
                            "  negative-cache TTL remaining: {}s",
                            NEGATIVE_CACHE_TTL_SECS - age
                        );
                    } else {
                        println!("  negative-cache entry expired");
                    }
                }
            }
            Ok(None) => {
                println!("\nno track-cache entry for provider {}", provider);
            }
            Err(e) => {
                println!(
                    "\nfailed to query track cache for provider {}: {}",
                    provider, e
                );
            }
        }
    }

    // If the path looks like a playlist file, try to map it back to a logical
    // playlist name and dump any cached playlist metadata.
    if path
        .extension()
        .and_then(|s| s.to_str())
        .map_or(false, |ext| ext.eq_ignore_ascii_case("m3u"))
    {
        if let Some(rel) = path
            .parent()
            .and_then(|p| p.strip_prefix(&cfg.root_folder).ok())
        {
            let playlist_name = rel.display().to_string();
            // Show playlist cache entries for all providers.
            for provider in &["spotify", "tidal"] {
                if let Ok(Some((mtime, size, hash, uris_json))) =
                    db::get_playlist_cache(&conn, &playlist_name, provider)
                {
                    println!("\nplaylist cache for '{}' (provider={}):", playlist_name, provider);
                    println!("  file_mtime: {}", mtime);
                    println!("  file_size: {}", size);
                    println!("  file_hash: {}", hash);
                    println!("  cached uris: {}", (uris_json));
                }
            }
            // If no entries found for any provider, say so.
            let any_found = ["spotify", "tidal"]
                .iter()
                .any(|p| db::get_playlist_cache(&conn, &playlist_name, p).ok().flatten().is_some());
            if !any_found {
                println!("\nno playlist cache entry for '{}'", playlist_name);
            }
        }
    }

    Ok(())
}

/// Lookup a single media file using a remote provider and update the track cache
/// accordingly.  This mirrors the logic used by the worker when resolving
/// playlist entries but is exposed as a CLI helper for manual debugging.
pub async fn file_lookup(cfg: &Config, provider_name: &str, path: &Path) -> Result<()> {
    println!(
        "=== lookup '{}' via provider '{}' ===",
        path.display(), provider_name
    );

    if !path.exists() {
        println!("file does not exist");
        return Ok(());
    }

    // build provider instance
    let prov: std::sync::Arc<dyn crate::api::Provider> = match provider_name.to_ascii_lowercase().as_str() {
        "spotify" => {
            use crate::api::spotify::SpotifyProvider;
            let db_path = cfg.db_path.clone();
            std::sync::Arc::new(SpotifyProvider::new(String::new(), String::new(), db_path, cfg.clone()))
        }
        "tidal" => {
            use crate::api::tidal::TidalProvider;
            let db_path = cfg.db_path.clone();
            let root = if cfg.online_root_playlist.trim().is_empty() {
                None
            } else {
                Some(cfg.online_root_playlist.clone())
            };
            std::sync::Arc::new(TidalProvider::new(String::new(), String::new(), db_path, root, cfg.clone()))
        }
        "mock" => {
            use crate::api::mock::MockProvider;
            std::sync::Arc::new(MockProvider::new())
        }
        other => {
            return Err(anyhow::anyhow!("unknown provider '{}'", other));
        }
    };

    if !prov.is_authenticated() {
        println!("provider not authenticated (skipping lookup)");
        // continue anyway: mock isn’t authenticated but still works
    }

    let path_str = path.display().to_string();
    // show existing cache entry if any
    if let Ok(Some((isrc, remote, resolved))) = {
        let conn = rusqlite::Connection::open(&cfg.db_path)?;
        db::get_track_cache_by_local(&conn, provider_name, &path_str)
    } {
        println!(
            "existing cache -> isrc={:?} remote={:?} resolved_at={}",
            isrc, remote, resolved
        );
    } else {
        println!("no existing cache entry");
    }

    // try resolution
    let isrc_opt = util::extract_isrc_from_path(path);
    let mut uri_opt: Option<String> = None;

    if let Some(ref isrc) = isrc_opt {
        match prov.search_track_uri_by_isrc(isrc).await {
            Ok(Some(u)) => {
                println!("found via ISRC {} -> {}", isrc, u);
                uri_opt = Some(u.clone());
            }
            Ok(None) => {
                println!("ISRC {} not found on provider", isrc);
            }
            Err(e) => {
                println!("ISRC lookup failed: {}", e);
            }
        }
    } else {
        println!("no ISRC extracted");
    }

    // fallback metadata search
    if uri_opt.is_none() {
        let fname = path.file_name().and_then(|s| s.to_str()).unwrap_or("");
        let stem = if let Some((base, _ext)) = fname.rsplit_once('.') {
            base
        } else {
            fname
        };
        let mut candidates: Vec<(&str, &str)> = Vec::new();
        if let Some((left, right)) = stem.split_once(" - ") {
            let left = left.trim();
            let right = right.trim();
            candidates.push((left, right));
            candidates.push((right, left));
        } else {
            candidates.push(("", stem));
        }
        for (artist, title) in candidates {
            match prov.search_track_uri(title, artist).await {
                Ok(Some(u)) => {
                    println!(
                        "found via metadata search '{}' - '{}' -> {}",
                        title, artist, u
                    );
                    uri_opt = Some(u.clone());
                    break;
                }
                Ok(None) => {}
                Err(e) => {
                    println!("metadata search failed: {}", e);
                }
            }
        }
    }

    // update cache
    {
        let db_path = cfg.db_path.clone();
        let provider_c = provider_name.to_string();
        let path_c = path_str.clone();
        let isrc_c = isrc_opt.clone();
        let uri_c = uri_opt.clone();
        tokio::task::spawn_blocking(move || -> Result<(), anyhow::Error> {
            let conn = rusqlite::Connection::open(db_path)?;
            db::upsert_track_cache(&conn, &provider_c, &path_c, isrc_c.as_deref(), uri_c.as_deref())?;
            Ok(())
        })
        .await??;
    }

    println!("cache refreshed");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn file_lookup_mock_creates_cache() -> Result<()> {
        let td = tempdir()?;
        let root = td.path().to_path_buf();
        let db_file = td.path().join("test_lookup.db");
        // create database with proper schema
        let _conn = db::open_or_create(&db_file)?;

        let cfg = Config {
            root_folder: root.clone(),
            whitelist: String::new(),
            local_whitelist: String::new(),
            remote_whitelist: String::new(),
            local_playlist_template: "${folder_name}.m3u".into(),
            remote_playlist_template: "${relative_path}".into(),
            remote_playlist_template_flat: String::new(),
            remote_playlist_template_folders: String::new(),
            playlist_description_template: String::new(),
            playlist_order_mode: "append".into(),
            playlist_mode: "flat".into(),
            linked_reference_format: "relative".into(),
            debounce_ms: 0,
            log_dir: td.path().join("log"),
            token_refresh_interval: 0,
            worker_interval_sec: 0,
            nightly_reconcile_cron: String::new(),
            queue_length_stop_cloud_sync_threshold: None,
            max_retries_on_error: 0,
            max_batch_size_spotify: 0,
            max_batch_size_tidal: 0,
            db_path: db_file.clone(),
            file_extensions: vec!["*.mp3".into()],
            online_root_playlist: String::new(),
            online_playlist_structure: "flat".into(),
            online_folder_flattening_delimiter: String::new(),
        };

        let test_file = cfg.root_folder.join("song.mp3");
        // create the file so lookup doesn't bail
        std::fs::write(&test_file, b"dummy")?;

        file_lookup(&cfg, "mock", &test_file).await?;

        // verify cache entry created
        let conn = rusqlite::Connection::open(&db_file)?;
        let entry = db::get_track_cache_by_local(&conn, "mock", &test_file.display().to_string())?;
        assert!(entry.is_some());
        let (_isrc, remote, _ts) = entry.unwrap();
        assert!(remote.is_some());
        Ok(())
    }

    #[test]
    fn file_info_nonexistent_file() -> Result<()> {
        let td = tempdir()?;
        let root = td.path().to_path_buf();
        let db_file = td.path().join("test.db");
        // ensure schema is applied
        let _conn = db::open_or_create(&db_file)?;

        let cfg = Config {
            root_folder: root.clone(),
            whitelist: String::new(),
            local_whitelist: String::new(),
            remote_whitelist: String::new(),
            local_playlist_template: "${folder_name}.m3u".into(),
            remote_playlist_template: "${relative_path}".into(),
            remote_playlist_template_flat: String::new(),
            remote_playlist_template_folders: String::new(),
            playlist_description_template: String::new(),
            playlist_order_mode: "append".into(),
            playlist_mode: "flat".into(),
            linked_reference_format: "relative".into(),
            debounce_ms: 0,
            log_dir: td.path().join("log"),
            token_refresh_interval: 0,
            worker_interval_sec: 0,
            nightly_reconcile_cron: String::new(),
            queue_length_stop_cloud_sync_threshold: None,
            max_retries_on_error: 0,
            max_batch_size_spotify: 0,
            max_batch_size_tidal: 0,
            db_path: db_file.clone(),
            file_extensions: vec!["*.mp3".into()],
            online_root_playlist: String::new(),
            online_playlist_structure: "flat".into(),
            online_folder_flattening_delimiter: String::new(),
        };

        let test_path = cfg.root_folder.join("doesnotexist.mp3");
        file_info(&cfg, &test_path)?;
        Ok(())
    }
}

