use crate::api::{spotify::SpotifyProvider, tidal::TidalProvider, Provider};
use crate::config::Config;
use crate::db;
use crate::collapse::collapse_events;
use crate::models::{Event, EventAction};
use anyhow::Result;

use std::sync::Arc;
use uuid::Uuid;

/// Worker orchestration: read unsynced events, group by playlist, collapse, apply rename then track adds/removes.
/// Adds per-playlist processing lease to avoid concurrent workers processing the same playlist.
pub async fn run_worker_once(cfg: &Config) -> Result<()> {
    // Ensure DB migrations are run (blocking)
    let _conn = tokio::task::spawn_blocking({
        let db_path = cfg.db_path.clone();
        move || -> Result<(), anyhow::Error> {
            let c = rusqlite::Connection::open(db_path)?;
            db::run_migrations(&c)?;
            Ok(())
        }
    })
    .await??;

    // Fetch unsynced events (blocking) by opening a fresh connection in the blocking task
    let events: Vec<Event> = tokio::task::spawn_blocking({
        let db_path = cfg.db_path.clone();
        move || -> Result<Vec<Event>, anyhow::Error> {
            let conn = rusqlite::Connection::open(db_path)?;
            db::fetch_unsynced_events(&conn).map_err(|e| e.into())
        }
    })
    .await??;

    if events.is_empty() {
        log::info!("No pending events");
        return Ok(());
    }

    // Backpressure
    if let Some(thresh) = cfg.queue_length_stop_cloud_sync_threshold {
        if events.len() as u64 > thresh {
            log::warn!("Queue length {} > threshold {}; stopping worker processing", events.len(), thresh);
            return Ok(());
        }
    }

    // Collect all authenticated providers
    let mut providers: Vec<(String, Arc<dyn Provider>)> = Vec::new();
    // Spotify
    let db_path = cfg.db_path.clone();
    let has_spotify = tokio::task::spawn_blocking(move || -> Result<bool, anyhow::Error> {
        let conn = rusqlite::Connection::open(db_path)?;
        Ok(db::load_credential_with_client(&conn, "spotify")?.is_some())
    })
    .await??;
    if has_spotify {
        log::info!("Using Spotify provider");
        providers.push(("spotify".to_string(), Arc::new(SpotifyProvider::new(String::new(), String::new(), cfg.db_path.clone()))));
    }
    // Tidal
    let db_path = cfg.db_path.clone();
    let has_tidal = tokio::task::spawn_blocking(move || -> Result<bool, anyhow::Error> {
        let conn = rusqlite::Connection::open(db_path)?;
        Ok(db::load_credential_with_client(&conn, "tidal")?.is_some())
    })
    .await??;
    if has_tidal {
        log::info!("Using Tidal provider");
        providers.push(("tidal".to_string(), Arc::new(TidalProvider::new(String::new(), String::new(), cfg.db_path.clone()))));
    }
    // If no real providers, do not consume the queue
    if providers.is_empty() {
        log::warn!("No valid provider credentials configured. Queue will not be consumed.");
        return Ok(());
    }

    // Group events per playlist_name
    use std::collections::HashMap;
    let mut groups: HashMap<String, Vec<Event>> = HashMap::new();
    for ev in events.into_iter() {
        groups.entry(ev.playlist_name.clone()).or_default().push(ev);
    }

    let worker_id = Uuid::new_v4().to_string();

    // For each provider, process all playlists
    for (provider_name, provider) in &providers {
        log::info!("Processing events with provider: {}", provider_name);
        // Process playlists sequentially (safety). Could be parallelized with locks.
        for (playlist_name, evs) in &groups {
            log::info!("Attempting to process playlist {} with provider {}", playlist_name, provider_name);

            // Try acquire lock (TTL = 10 minutes default)
            let lock_acquired = tokio::task::spawn_blocking({
                let db_path = cfg.db_path.clone();
                let pl = playlist_name.clone();
                let wid = worker_id.clone();
                move || -> Result<bool, anyhow::Error> {
                    let mut conn = rusqlite::Connection::open(db_path)?;
                    Ok(db::try_acquire_playlist_lock(&mut conn, &pl, &wid, 600)?)
                }
            })
            .await??;

            if !lock_acquired {
                log::info!("Skipped {} because lock could not be acquired", playlist_name);
                continue;
            }

            // Ensure we release lock at end
            let release_on_exit = (cfg.db_path.clone(), playlist_name.clone(), worker_id.clone());

            // Collapse events
            let collapsed = collapse_events(evs);

        // Find rename/delete and collect track ops
        let mut rename_opt: Option<(String, String)> = None;
        let mut has_delete: bool = false;
        let mut track_ops: Vec<(EventAction, Option<String>)> = Vec::new();
        let mut original_ids: Vec<i64> = Vec::new();
        for op in &collapsed {
            match &op.action {
                EventAction::Rename { from, to } => rename_opt = Some((from.clone(), to.clone())),
                EventAction::Delete => has_delete = true,
                EventAction::Add => track_ops.push((EventAction::Add, op.track_path.clone())),
                EventAction::Remove => track_ops.push((EventAction::Remove, op.track_path.clone())),
                _ => {}
            }
        }
        for e in evs {
            original_ids.push(e.id);
        }

        // Resolve remote playlist id from playlist_map (or create via provider.ensure_playlist if needed)
        let remote_id_opt = tokio::task::spawn_blocking({
            let db_path = cfg.db_path.clone();
            let pl = playlist_name.clone();
            move || -> Result<Option<String>, anyhow::Error> {
                let conn = rusqlite::Connection::open(db_path)?;
                db::get_remote_playlist_id(&conn, &pl).map_err(|e| e.into())
            }
        })
        .await??;

        // If this playlist is being deleted, attempt to delete remotely and clean up local state,
        // then skip any add/remove operations.
        if has_delete {
            if let Some(remote_id) = remote_id_opt.clone() {
                let mut attempt = 0u32;
                loop {
                    attempt += 1;
                    let res = provider.delete_playlist(&remote_id).await;
                    match res {
                        Ok(_) => {
                            log::info!("Deleted remote playlist {} ({})", playlist_name, remote_id);
                            break;
                        }
                        Err(e) => {
                            if attempt >= cfg.max_retries_on_error {
                                log::error!("Delete failed after {} attempts: {}", attempt, e);
                                break;
                            } else {
                                log::warn!("Delete attempt {} failed: {}. Retrying...", attempt, e);
                                tokio::time::sleep(std::time::Duration::from_secs(1 << attempt)).await;
                                continue;
                            }
                        }
                    }
                }
            } else {
                log::info!("No remote playlist id for {}; skipping remote delete", playlist_name);
            }

            // Remove local playlist_map entry regardless of whether remote deletion succeeded
            let pl = playlist_name.clone();
            let db_path = cfg.db_path.clone();
            tokio::task::spawn_blocking(move || -> Result<(), anyhow::Error> {
                let conn = rusqlite::Connection::open(db_path)?;
                let _ = db::delete_playlist_map(&conn, &pl)?;
                Ok(())
            })
            .await??;

            // Mark original events as synced (so they don't get retried forever)
            let ids_clone = original_ids.clone();
            let db_path = cfg.db_path.clone();
            tokio::task::spawn_blocking(move || -> Result<(), anyhow::Error> {
                let mut conn = rusqlite::Connection::open(db_path)?;
                if !ids_clone.is_empty() {
                    db::mark_events_synced(&mut conn, &ids_clone)?;
                }
                Ok(())
            })
            .await??;

            // release lock
            let (dbp, pln, wid) = release_on_exit.clone();
            let _ = tokio::task::spawn_blocking(move || -> Result<(), anyhow::Error> {
                let mut conn = rusqlite::Connection::open(dbp)?;
                db::release_playlist_lock(&mut conn, &pln, &wid)?;
                Ok(())
            })
            .await?;

            // Move to next playlist/provider
            continue;
        }

        let remote_id = if let Some(rid) = remote_id_opt {
            rid
        } else {
            // create via provider.ensure_playlist
            match provider.ensure_playlist(&playlist_name, "").await {
                Ok(rid) => {
                    // persist
                    let pl = playlist_name.clone();
                    let db_path = cfg.db_path.clone();
                    let rid_clone = rid.clone();
                    tokio::task::spawn_blocking(move || -> Result<(), anyhow::Error> {
                        let conn = rusqlite::Connection::open(db_path)?;
                        db::upsert_playlist_map(&conn, &pl, &rid_clone)?;
                        Ok(())
                    })
                    .await??;
                    rid
                }
                Err(e) => {
                    log::error!("Failed to create remote playlist for {}: {}", playlist_name, e);
                    // release lock and continue
                    let (dbp, pln, wid) = release_on_exit.clone();
                    let _ = tokio::task::spawn_blocking(move || -> Result<(), anyhow::Error> {
                        let mut conn = rusqlite::Connection::open(dbp)?;
                        let _ = db::release_playlist_lock(&mut conn, &pln, &wid)?;
                        Ok(())
                    })
                    .await;
                    continue;
                }
            }
        };

        // Apply rename first
        if let Some((_from, to)) = rename_opt {
            let mut attempt = 0u32;
            loop {
                attempt += 1;
                let res = provider.rename_playlist(&remote_id, &to).await;
                match res {
                    Ok(_) => {
                        log::info!("Renamed remote playlist {} -> {}", remote_id, to);
                        // update playlist_map name? Here we keep playlist_name as local key; remote_id unchanged.
                        break;
                    }
                    Err(e) => {
                        if attempt >= cfg.max_retries_on_error {
                            log::error!("Rename failed after {} attempts: {}", attempt, e);
                            break;
                        } else {
                            log::warn!("Rename attempt {} failed: {}. Retrying...", attempt, e);
                            tokio::time::sleep(std::time::Duration::from_secs(1 << attempt)).await;
                            continue;
                        }
                    }
                }
            }
        }

        // Resolve track URIs and build add/remove lists (prefer cache/ISRC when possible, then fallback to metadata search)
        let mut add_uris: Vec<String> = Vec::new();
        let mut remove_uris: Vec<String> = Vec::new();
        for (act, track_path_opt) in track_ops.into_iter() {
            if let Some(tp) = track_path_opt {
                if tp.starts_with("uri::") {
                    let uri = tp.trim_start_matches("uri::").to_string();
                    match act {
                        EventAction::Add => add_uris.push(uri),
                        EventAction::Remove => remove_uris.push(uri),
                        _ => {}
                    }
                    continue;
                }

                // Try track cache first: reuse previously resolved URI and/or ISRC for this local path
                let cached: Option<(Option<String>, Option<String>)> = tokio::task::spawn_blocking({
                    let db_path = cfg.db_path.clone();
                    let local_path = tp.clone();
                    move || -> Result<Option<(Option<String>, Option<String>)>, anyhow::Error> {
                        let conn = rusqlite::Connection::open(db_path)?;
                        Ok(db::get_track_cache_by_local(&conn, &local_path)?)
                    }
                })
                .await??;

                if let Some((_cached_isrc, cached_remote_id)) = &cached {
                    if let Some(uri) = cached_remote_id {
                        match act {
                            EventAction::Add => add_uris.push(uri.clone()),
                            EventAction::Remove => remove_uris.push(uri.clone()),
                            _ => {}
                        }
                        continue;
                    }
                }

                // For Spotify, try to extract ISRC from local file metadata and perform an ISRC-based search
                let mut isrc_for_lookup: Option<String> = cached.as_ref().and_then(|(i, _)| i.clone());
                if isrc_for_lookup.is_none() && provider.name() == "spotify" {
                    let p = std::path::Path::new(&tp).to_path_buf();
                    let extracted = match tokio::task::spawn_blocking(move || crate::util::extract_isrc_from_path(&p)).await {
                        Ok(v) => v,
                        Err(e) => {
                            log::warn!("ISRC extraction task failed for {}: {}", tp, e);
                            None
                        }
                    };
                    if let Some(code) = extracted {
                        isrc_for_lookup = Some(code.clone());
                        // persist locally extracted ISRC in cache (without remote id yet)
                        let db_path = cfg.db_path.clone();
                        let local_path = tp.clone();
                        let code_for_cache = code.clone();
                        tokio::task::spawn_blocking(move || -> Result<(), anyhow::Error> {
                            let conn = rusqlite::Connection::open(db_path)?;
                            let _ = crate::db::upsert_track_cache(&conn, &local_path, Some(code_for_cache.as_str()), None)?;
                            Ok(())
                        })
                        .await??;
                    }
                }

                if let Some(isrc) = isrc_for_lookup.clone() {
                    match provider.search_track_uri_by_isrc(&isrc).await {
                        Ok(Some(uri)) => {
                            match act {
                                EventAction::Add => add_uris.push(uri.clone()),
                                EventAction::Remove => remove_uris.push(uri.clone()),
                                _ => {}
                            }
                            // Persist cache with ISRC + resolved URI
                            let db_path = cfg.db_path.clone();
                            let local_path = tp.clone();
                            let uri_clone = uri.clone();
                            let isrc_for_cache = isrc.clone();
                            tokio::task::spawn_blocking(move || -> Result<(), anyhow::Error> {
                                let conn = rusqlite::Connection::open(db_path)?;
                                let _ = crate::db::upsert_track_cache(&conn, &local_path, Some(isrc_for_cache.as_str()), Some(&uri_clone))?;
                                Ok(())
                            })
                            .await??;
                            continue;
                        }
                        #[allow(non_snake_case)]
                        Ok(None) => {
                            // fall through to metadata-based search
                        }
                        Err(e) => {
                            log::warn!("Error searching by ISRC for {}: {}", tp, e);
                            // fall through to metadata-based search
                        }
                    }
                }

                // Fallback: derive artist/title from filename and do provider metadata search
                let fname = std::path::Path::new(&tp).file_name().and_then(|s| s.to_str()).unwrap_or("");
                let (artist, title) = if let Some((a,t)) = fname.split_once(" - ") {
                    (a.trim(), t.trim().trim_end_matches(".mp3"))
                } else {
                    ("", fname)
                };
                match provider.search_track_uri(title, artist).await {
                    Ok(Some(uri)) => {
                        match act {
                            EventAction::Add => add_uris.push(uri.clone()),
                            EventAction::Remove => remove_uris.push(uri.clone()),
                            _ => {}
                        }
                        // attempt to lookup ISRC from provider; persist resolved uri + isrc into track_cache
                        let db_path = cfg.db_path.clone();
                        let local_path = tp.clone();
                        let uri_clone = uri.clone();
                        let provider_clone = provider.clone();
                        let maybe_isrc = provider_clone.lookup_track_isrc(&uri_clone).await.unwrap_or(None);
                        tokio::task::spawn_blocking(move || -> Result<(), anyhow::Error> {
                            let conn = rusqlite::Connection::open(db_path)?;
                            let _ = crate::db::upsert_track_cache(&conn, &local_path, maybe_isrc.as_deref(), Some(&uri_clone))?;
                            Ok(())
                        })
                        .await??;
                    }
                    #[allow(non_snake_case)]
                    Ok(None) => {
                        log::warn!("Could not resolve track {} to remote URI", tp);
                    }
                    Err(e) => {
                        log::warn!("Error searching track {}: {}", tp, e);
                    }
                }
            }
        }

        // Helper to apply batches with retry/backoff and 429 handling
        async fn apply_in_batches(provider: Arc<dyn Provider>, playlist_id: &str, uris: Vec<String>, is_add: bool, cfg: &Config) -> Result<()> {
            if uris.is_empty() {
                return Ok(());
            }
            let batch_size = cfg.max_batch_size_spotify;
                for chunk in uris.chunks(batch_size) {
                    let mut attempt = 0u32;
                    loop {
                        attempt += 1;
                        let res = if is_add {
                            provider.add_tracks(playlist_id, chunk).await
                        } else {
                            provider.remove_tracks(playlist_id, chunk).await
                        };
                        match res {
                            Ok(_) => {
                                log::info!("Applied {} {} tracks to {}", if is_add { "add" } else { "remove" }, chunk.len(), playlist_id);
                                break;
                            }
                            Err(e) => {
                                let s = format!("{}", e);
                                // Parse retry_after if provider included it in the error string like `retry_after=Some(5)`
                                let retry_after_secs = s.split("retry_after=").nth(1).and_then(|rest| {
                                    // rest might be like "Some(5)" or "None" or "5"
                                    let token = rest.trim();
                                    if token.starts_with("Some(") {
                                        token.trim_start_matches("Some(").split(')').next()
                                    } else if token.starts_with("None") {
                                        None
                                    } else {
                                        // take digits prefix
                                        Some(token.split(|c: char| !c.is_digit(10)).next().unwrap_or("").trim())
                                    }
                                }).and_then(|s| s.parse::<u64>().ok());

                                if s.contains("rate_limited") || retry_after_secs.is_some() {
                                    let wait = retry_after_secs.unwrap_or_else(|| {
                                        // exponential backoff cap 60s
                                        let exp = 2u64.saturating_pow(std::cmp::min(attempt, 6));
                                        std::cmp::min(exp, 60)
                                    });
                                    log::warn!("Rate limited: {}. Sleeping {}s before retry.", e, wait);
                                    tokio::time::sleep(std::time::Duration::from_secs(wait + 1)).await;
                                    // continue retrying until max_retries_on_error
                                    if attempt >= cfg.max_retries_on_error {
                                        log::error!("Giving up after {} rate-limit attempts: {}", attempt, e);
                                        break;
                                    }
                                    continue;
                                } else {
                                    if attempt >= cfg.max_retries_on_error {
                                        log::error!("Giving up after {} attempts: {}", attempt, e);
                                        break;
                                    } else {
                                        let exp = std::cmp::min(1u64 << attempt, 60);
                                        log::warn!("Error applying batch (attempt {}): {}. Retrying in {}s...", attempt, e, exp);
                                        tokio::time::sleep(std::time::Duration::from_secs(exp)).await;
                                        continue;
                                    }
                                }
                            }
                        }
                    }
                }
            Ok(())
        }

        let provider_arc = provider.clone();
        if let Err(e) = apply_in_batches(provider_arc.clone(), &remote_id, remove_uris, false, cfg).await {
                log::error!("Error applying removes for {}: {}", playlist_name, e);
        }
        if let Err(e) = apply_in_batches(provider_arc.clone(), &remote_id, add_uris, true, cfg).await {
                log::error!("Error applying adds for {}: {}", playlist_name, e);
        }

        // Mark original events as synced (blocking)
        let ids_clone = original_ids.clone();
        let db_path = cfg.db_path.clone();
        tokio::task::spawn_blocking(move || -> Result<(), anyhow::Error> {
            let mut conn = rusqlite::Connection::open(db_path)?;
            if !ids_clone.is_empty() {
                db::mark_events_synced(&mut conn, &ids_clone)?;
            }
            Ok(())
        })
        .await??;

        // release lock
        let (dbp, pln, wid) = release_on_exit.clone();
        let _ = tokio::task::spawn_blocking(move || -> Result<(), anyhow::Error> {
            let mut conn = rusqlite::Connection::open(dbp)?;
            db::release_playlist_lock(&mut conn, &pln, &wid)?;
            Ok(())
        })
        .await?;
        }
    }
    Ok(())
}

/// Nightly reconciliation: scan the root folder, write playlists for every folder,
/// and enqueue a `Create` event so the worker will reconcile remote playlists later.
pub fn run_nightly_reconcile(cfg: &Config) -> Result<()> {
    let tree = crate::watcher::InMemoryTree::build(
        &cfg.root_folder,
        if cfg.whitelist.is_empty() { None } else { Some(&cfg.whitelist) },
        Some(&cfg.file_extensions),
    )?;
    // collect thread handles for the enqueue operations so we can join before returning
    let mut handles: Vec<std::thread::JoinHandle<()>> = Vec::new();
    for (folder, _node) in tree.nodes.iter() {
        let folder_name = folder.file_name().and_then(|s| s.to_str()).unwrap_or("");
        let rel = folder.strip_prefix(&cfg.root_folder).unwrap_or(folder).display().to_string();
        let playlist_name = crate::util::expand_template(&cfg.local_playlist_template, folder_name, &rel);
        let playlist_path = folder.join(&playlist_name);

        if cfg.playlist_mode == "flat" {
            if let Err(e) = crate::playlist::write_flat_playlist(folder, &playlist_path, &cfg.playlist_order_mode, &cfg.file_extensions) {
                log::warn!("Failed to write playlist {:?}: {}", playlist_path, e);
            }
        } else {
            if let Err(e) = crate::playlist::write_linked_playlist(folder, &playlist_path, &cfg.linked_reference_format, &cfg.local_playlist_template) {
                log::warn!("Failed to write linked playlist {:?}: {}", playlist_path, e);
            }
        }

        // enqueue Create event in a background thread but keep the handle to join
        let pname = playlist_name.clone();
        let db_path = cfg.db_path.clone();
        let h = std::thread::spawn(move || {
            if let Ok(conn) = crate::db::open_or_create(std::path::Path::new(&db_path)) {
                if let Err(e) = crate::db::enqueue_event(&conn, &pname, &crate::models::EventAction::Create, None, None) {
                    log::warn!("Failed to enqueue nightly create event for {}: {}", pname, e);
                }
            } else {
                log::warn!("Failed to open DB to enqueue nightly event");
            }
        });
        handles.push(h);
    }

    // join all enqueue threads to ensure events are persisted before returning
    for h in handles {
        let _ = h.join();
    }

    Ok(())
}