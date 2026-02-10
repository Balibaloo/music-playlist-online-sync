use crate::api::{spotify::SpotifyProvider, tidal::TidalProvider, Provider};
use crate::config::Config;
use crate::db;
use crate::collapse::collapse_events;
use crate::models::{Event, EventAction};
use anyhow::{Result, Context};

use std::sync::Arc;
use uuid::Uuid;

/// Compute the set of remote track URIs that should be present for a playlist
/// based on the current local playlist file contents.
///
/// This helper reads the local .m3u for the logical playlist key, resolves
/// each referenced file to a remote URI using the same cache/lookup logic as
/// the event-driven worker (track_cache first, then ISRC/metadata lookup),
/// and returns the resulting URI set.
async fn desired_remote_uris_for_playlist(cfg: &Config, playlist_name: &str, provider: Arc<dyn Provider>) -> Result<Vec<String>> {
    use std::io::BufRead;

    // Map logical playlist key back to on-disk .m3u path using the same
    // template as reconcile/watcher: playlist_name is the relative folder
    // path under root_folder.
    let rel = std::path::Path::new(playlist_name);
    let folder = cfg.root_folder.join(rel);
    let folder_name = folder.file_name().and_then(|s| s.to_str()).unwrap_or("");
    let path_to_parent = rel.parent().map(|p| p.to_path_buf()).unwrap_or_else(|| std::path::PathBuf::new());
    let path_to_parent_str = if path_to_parent.as_os_str().is_empty() {
        String::new()
    } else {
        let mut s = path_to_parent.display().to_string();
        if !s.ends_with(std::path::MAIN_SEPARATOR) {
            s.push(std::path::MAIN_SEPARATOR);
        }
        s
    };
    let playlist_file_name = crate::util::expand_template(&cfg.local_playlist_template, folder_name, &path_to_parent_str);
    let playlist_path = folder.join(playlist_file_name);

    if !playlist_path.exists() {
        return Ok(Vec::new());
    }

    let file = std::fs::File::open(&playlist_path)?;
    let reader = std::io::BufReader::new(file);
    let mut uris: Vec<String> = Vec::new();
    let provider_name = provider.name().to_string();

    for line in reader.lines() {
        let line = line.unwrap_or_default();
        if line.starts_with('#') || line.trim().is_empty() {
            continue;
        }
        let local_path = folder.join(&line);
        if !local_path.exists() {
            continue;
        }

        let local_path_str = local_path.display().to_string();

        // First, try the track cache by local path.
        let db_path = cfg.db_path.clone();
        let provider_name_for_lookup = provider_name.clone();
        let local_path_for_lookup = local_path_str.clone();
        let cached: Option<(Option<String>, Option<String>)> = tokio::task::spawn_blocking(move || -> Result<Option<(Option<String>, Option<String>)>, anyhow::Error> {
            let conn = rusqlite::Connection::open(db_path)?;
            Ok(db::get_track_cache_by_local(&conn, &provider_name_for_lookup, &local_path_for_lookup)?)
        })
        .await??;

        if let Some((_cached_isrc, cached_remote_id)) = &cached {
            if let Some(uri) = cached_remote_id {
                uris.push(uri.clone());
                continue;
            }
        }

        // Try to extract ISRC from local file metadata and perform an ISRC-based search.
        let p = local_path.clone();
        let extracted = tokio::task::spawn_blocking(move || crate::util::extract_isrc_from_path(&p))
            .await
            .unwrap_or(None);

        let mut uri_opt: Option<String> = None;

        if let Some(isrc) = extracted.clone() {
            if let Ok(Some(u)) = provider.search_track_uri_by_isrc(&isrc).await {
                uri_opt = Some(u.clone());

                // Persist into track_cache for future lookups.
                let db_path = cfg.db_path.clone();
                let local_path_for_cache = local_path_str.clone();
                let provider_name_for_cache = provider.name().to_string();
                tokio::task::spawn_blocking(move || -> Result<(), anyhow::Error> {
                    let conn = rusqlite::Connection::open(db_path)?;
                    let _ = db::upsert_track_cache(&conn, &provider_name_for_cache, &local_path_for_cache, Some(isrc.as_str()), Some(&u));
                    Ok(())
                })
                .await??;
            }
        }

        // Fallback: derive artist/title from filename and search.
        if uri_opt.is_none() {
            let fname = local_path.file_name().and_then(|s| s.to_str()).unwrap_or("");
            let stem = if let Some((base, _ext)) = fname.rsplit_once('.') { base } else { fname };
            let mut candidates: Vec<(&str, &str)> = Vec::new();
            if let Some((left, right)) = stem.split_once(" - ") {
                let left = left.trim();
                let right = right.trim();
                candidates.push((left, right));
                candidates.push((right, left));
            } else {
                candidates.push(("", stem));
            }
            for (artist, title) in candidates.into_iter() {
                if let Ok(Some(u)) = provider.search_track_uri(title, artist).await {
                    uri_opt = Some(u.clone());

                    // Persist into track_cache.
                    let db_path = cfg.db_path.clone();
                    let local_path_for_cache = local_path_str.clone();
                    let provider_name_for_cache = provider.name().to_string();
                    let isrc_clone = extracted.clone();
                    tokio::task::spawn_blocking(move || -> Result<(), anyhow::Error> {
                        let conn = rusqlite::Connection::open(db_path)?;
                        let _ = db::upsert_track_cache(&conn, &provider_name_for_cache, &local_path_for_cache, isrc_clone.as_deref(), Some(&u));
                        Ok(())
                    })
                    .await??;

                    break;
                }
            }
        }

        if let Some(u) = uri_opt {
            uris.push(u);
        } else {
            log::warn!(
                "Reconcile: could not resolve local track {:?} to remote URI for provider {}",
                local_path,
                provider.name()
            );
        }
    }

    // Deduplicate while preserving order.
    let mut seen = std::collections::HashSet::new();
    uris.retain(|u| seen.insert(u.clone()));
    Ok(uris)
}

/// Return true if the given provider supports hierarchical playlist folders.
/// Some providers (e.g. Tidal) do not expose folder nesting via their APIs
/// and must be treated as flat for naming purposes.
fn provider_supports_folder_nesting(provider_name: &str) -> bool {
    match provider_name {
        "tidal" => false,
        _ => true,
    }
}

/// Compute the display name to use for a remote playlist on a given provider,
/// based on the logical playlist key and configuration.
///
/// This function is responsible for:
/// - Applying an optional `online_root_playlist` under which all remote
///   playlists are logically nested.
/// - Respecting `online_playlist_structure` ("flat" or "folders").
/// - Using `online_folder_flattening_delimiter` when flattening hierarchies
///   into a single playlist name.
/// - Applying the appropriate remote playlist template based on structure:
///   `remote_playlist_template_flat` or `remote_playlist_template_folders`.
///   When these are empty, it falls back to `remote_playlist_template` and
///   finally to the raw logical name.
///
/// Placeholders inside the templates are expanded via `util::expand_template`:
/// - "${folder_name}"    -> the final path segment of the logical playlist
///                            key (e.g. "Album1").
/// - "${path_to_parent}" -> the logical path to the playlist folder's
///                            parent, including `online_root_playlist` when
///                            set. For flat mode, when
///                            `online_folder_flattening_delimiter` is non-
///                            empty, filesystem separators in this path are
///                            replaced by that delimiter.
/// - "${relative_path}"  -> legacy alias, expanded as
///                            `path_to_parent + folder_name` so that existing
///                            configs continue to work.
fn compute_remote_playlist_name(cfg: &Config, provider_name: &str, playlist_key: &str) -> String {
    let root = cfg.online_root_playlist.trim();
    let structure = cfg.online_playlist_structure.as_str();
    let delim_cfg = cfg.online_folder_flattening_delimiter.as_str();
    let supports_folders = provider_supports_folder_nesting(provider_name);

    // Normalize the logical playlist key to use "/" as a separator so we
    // can split it into parent path and folder_name. Existing databases may
    // still contain old keys like "Album1.m3u"; for those, parent will be
    // empty and folder_name will be the whole string.
    let normalized = playlist_key.replace('\\', "/").trim_matches('/').to_string();
    let normalized_str = normalized.as_str();
    let (parent_rel, folder_name) = if let Some((p, f)) = normalized_str.rsplit_once('/') {
        (p.to_string(), f.to_string())
    } else {
        (String::new(), normalized_str.to_string())
    };

    // Build a filesystem-style logical path to the playlist folder's parent,
    // starting from the optional online_root_playlist.
    let mut path_to_parent_fs = String::new();
    if !root.is_empty() {
        path_to_parent_fs.push_str(root);
        path_to_parent_fs.push('/');
    }
    if !parent_rel.is_empty() {
        path_to_parent_fs.push_str(&parent_rel);
        path_to_parent_fs.push('/');
    }

    // Decide whether this provider will actually use folder-style structure.
    let is_folder_style = structure == "folders" && supports_folders;

    // Choose template based on effective structure, falling back to the
    // legacy `remote_playlist_template` if the structure-specific template
    // is empty.
    let template = if is_folder_style {
        if !cfg.remote_playlist_template_folders.is_empty() {
            cfg.remote_playlist_template_folders.as_str()
        } else {
            cfg.remote_playlist_template.as_str()
        }
    } else {
        if !cfg.remote_playlist_template_flat.is_empty() {
            cfg.remote_playlist_template_flat.as_str()
        } else {
            cfg.remote_playlist_template.as_str()
        }
    };

    if template.is_empty() {
        // No templates configured; fall back to a simple logical path that
        // includes the root (if any) and the key.
        if !root.is_empty() {
            if normalized_str.is_empty() {
                return root.to_string();
            }
            return format!("{}/{}", root, normalized_str);
        }
        return normalized_str.to_string();
    }

    // For flat structure (or providers without folder support), optionally
    // flatten the path_to_parent using the configured delimiter.
    let path_to_parent = if !is_folder_style && !delim_cfg.is_empty() {
        let delim = delim_cfg;
        if path_to_parent_fs.is_empty() {
            String::new()
        } else {
            // Replace "/" separators with the delimiter, preserving
            // trailing position so that `path_to_parent + folder_name`
            // yields the logical path to the playlist folder.
            path_to_parent_fs.replace('/', delim)
        }
    } else {
        path_to_parent_fs
    };

    crate::util::expand_template(template, &folder_name, &path_to_parent)
}

/// Worker orchestration: read unsynced events, group by playlist, collapse, apply rename then track adds/removes.
/// Adds per-playlist processing lease to avoid concurrent workers processing the same playlist.
pub async fn run_worker_once(cfg: &Config) -> Result<()> {
    // Ensure DB migrations are run (blocking)
    let _conn = tokio::task::spawn_blocking({
        let db_path = cfg.db_path.clone();
        move || -> Result<(), anyhow::Error> {
            let path_display = db_path.display().to_string();
            let c = rusqlite::Connection::open(&db_path)
                .with_context(|| format!("opening DB for migrations at {}", path_display))?;
            db::run_migrations(&c)
                .with_context(|| format!("running DB migrations using schema for {}", path_display))?;
            Ok(())
        }
    })
    .await??;

    // Fetch unsynced events (blocking) by opening a fresh connection in the blocking task
    let events: Vec<Event> = tokio::task::spawn_blocking({
        let db_path = cfg.db_path.clone();
        move || -> Result<Vec<Event>, anyhow::Error> {
            let path_display = db_path.display().to_string();
            let conn = rusqlite::Connection::open(&db_path)
                .with_context(|| format!("opening DB for fetching unsynced events at {}", path_display))?;
            db::fetch_unsynced_events(&conn)
                .map_err(|e| e.into())
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
        let path_display = db_path.display().to_string();
        let conn = rusqlite::Connection::open(&db_path)
            .with_context(|| format!("opening DB for loading spotify credentials at {}", path_display))?;
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
        let path_display = db_path.display().to_string();
        let conn = rusqlite::Connection::open(&db_path)
            .with_context(|| format!("opening DB for loading tidal credentials at {}", path_display))?;
        Ok(db::load_credential_with_client(&conn, "tidal")?.is_some())
    })
    .await??;
    if has_tidal {
        log::info!("Using Tidal provider");
        providers.push((
            "tidal".to_string(),
            Arc::new(TidalProvider::new(
                String::new(),
                String::new(),
                cfg.db_path.clone(),
                if cfg.online_root_playlist.trim().is_empty() {
                    None
                } else {
                    Some(cfg.online_root_playlist.clone())
                },
            )),
        ));
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

    // For each playlist, process all providers
    // This ensures that for a given playlist, all configured
    // providers are updated before moving on to the next one.
    for (playlist_name, evs) in &groups {
        // Process providers sequentially (safety). Could be parallelized with locks.
        for (provider_name, provider) in &providers {
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
            let mut collapsed = collapse_events(evs);

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

        // Resolve remote playlist id from playlist_map (or create via provider.ensure_playlist if needed).
        let remote_id_opt = tokio::task::spawn_blocking({
            let db_path = cfg.db_path.clone();
            let pl = playlist_name.clone();
            let prov = provider.name().to_string();
            move || -> Result<Option<String>, anyhow::Error> {
                let conn = rusqlite::Connection::open(db_path)?;
                db::get_remote_playlist_id(&conn, &prov, &pl).map_err(|e| e.into())
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
            let prov = provider.name().to_string();
            tokio::task::spawn_blocking(move || -> Result<(), anyhow::Error> {
                let conn = rusqlite::Connection::open(db_path)?;
                let _ = db::delete_playlist_map(&conn, &prov, &pl)?;
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

        // Precompute desired remote URIs based on the current local playlist
        // so we can reconcile remote contents later once we have a playlist id.
        let mut reconcile_desired: Option<Vec<String>> = None;
        if !has_delete {
            log::info!(
                "Reconcile: computing desired remote URIs for playlist {} on provider {}",
                playlist_name,
                provider.name()
            );
            match desired_remote_uris_for_playlist(cfg, playlist_name, provider.clone()).await {
                Ok(desired) => {
                    log::info!(
                        "Reconcile: computed {} desired tracks for playlist {} on provider {}",
                        desired.len(),
                        playlist_name,
                        provider.name()
                    );
                    if !desired.is_empty() {
                        reconcile_desired = Some(desired);
                    }
                }
                Err(e) => {
                    log::warn!(
                        "Reconcile: failed to compute desired URIs for playlist {} on provider {}: {}",
                        playlist_name,
                        provider.name(),
                        e
                    );
                }
            }
        }

        // Compute the desired remote display name for this playlist on this provider.
        let remote_display_name = compute_remote_playlist_name(cfg, provider.name(), playlist_name);

        let mut remote_id = if let Some(rid) = remote_id_opt {
            rid
        } else {
            // create via provider.ensure_playlist
            match provider.ensure_playlist(&remote_display_name, "").await {
                Ok(rid) => {
                    // persist
                    let pl = playlist_name.clone();
                    let db_path = cfg.db_path.clone();
                    let rid_clone = rid.clone();
                    let prov = provider.name().to_string();
                    tokio::task::spawn_blocking(move || -> Result<(), anyhow::Error> {
                        let conn = rusqlite::Connection::open(db_path)?;
                        db::upsert_playlist_map(&conn, &prov, &pl, &rid_clone)?;
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

        // If we have a stored remote id, verify with the provider that it is
        // still a valid, accessible playlist. This covers the case where the
        // user "deletes" (unfollows) a Spotify playlist in the client while
        // our local mapping still points at the old id.
        match provider.playlist_is_valid(&remote_id).await {
            Ok(false) => {
                log::warn!(
                    "Remote playlist {} (id {}) is no longer accessible on provider {}; recreating and updating mapping",
                    playlist_name,
                    remote_id,
                    provider.name()
                );
                match provider.ensure_playlist(&remote_display_name, "").await {
                    Ok(new_id) => {
                        let db_path = cfg.db_path.clone();
                        let pl = playlist_name.clone();
                        let prov = provider.name().to_string();
                        let new_id_clone = new_id.clone();
                        tokio::task::spawn_blocking(move || -> Result<(), anyhow::Error> {
                            let conn = rusqlite::Connection::open(db_path)?;
                            db::upsert_playlist_map(&conn, &prov, &pl, &new_id_clone)?;
                            Ok(())
                        })
                        .await??;

                        log::info!(
                            "Recreated remote playlist {} with new id {} for provider {} after detecting deletion/unfollow",
                            playlist_name,
                            new_id,
                            provider.name()
                        );
                        remote_id = new_id;
                    }
                    Err(e) => {
                        log::error!(
                            "Failed to recreate inaccessible playlist {} for provider {}: {}",
                            playlist_name,
                            provider.name(),
                            e
                        );

                        // Release lock and skip further processing for this playlist.
                        let (dbp, pln, wid) = release_on_exit.clone();
                        let _ = tokio::task::spawn_blocking(move || -> Result<(), anyhow::Error> {
                            let mut conn = rusqlite::Connection::open(dbp)?;
                            db::release_playlist_lock(&mut conn, &pln, &wid)?;
                            Ok(())
                        })
                        .await;
                        continue;
                    }
                }
            }
            Ok(true) => {
                // normal case, keep existing id
            }
            Err(e) => {
                log::warn!(
                    "playlist_is_valid check failed for playlist {} (id {}) on provider {}: {}",
                    playlist_name,
                    remote_id,
                    provider.name(),
                    e
                );
            }
        }

        // If there was no explicit rename event for this playlist, still
        // ensure that the remote playlist's display name matches what we
        // compute from configuration (e.g. to apply `online_root_playlist`
        // to playlists that were created before this option was enabled).
        //
        // We only attempt this if the desired remote display name differs
        // from the logical local playlist name to avoid unnecessary rename
        // calls when no online naming options are in use.
        if rename_opt.is_none() && remote_display_name != *playlist_name {
            let mut attempt = 0u32;
            loop {
                attempt += 1;
                let res = provider.rename_playlist(&remote_id, &remote_display_name).await;
                match res {
                    Ok(_) => {
                        log::info!(
                            "Ensured remote playlist {} has display name {}",
                            remote_id,
                            remote_display_name
                        );
                        break;
                    }
                    Err(e) => {
                        let s = format!("{}", e);
                        // Special handling: if the provider reports that the
                        // playlist id no longer exists (e.g. TIDAL 404),
                        // recreate it from local state and update mappings.
                        if s.contains("tidal rename failed: 404 Not Found") && s.contains("Playlists with id") {
                            log::warn!(
                                "Remote playlist {} (id {}) not found on provider {}; recreating before config-driven rename...",
                                playlist_name,
                                remote_id,
                                provider.name()
                            );

                            match provider.ensure_playlist(&remote_display_name, "").await {
                                Ok(new_id) => {
                                    let db_path = cfg.db_path.clone();
                                    let pl = playlist_name.clone();
                                    let prov = provider.name().to_string();
                                    let new_id_clone = new_id.clone();
                                    tokio::task::spawn_blocking(move || -> Result<(), anyhow::Error> {
                                        let conn = rusqlite::Connection::open(db_path)?;
                                        db::upsert_playlist_map(&conn, &prov, &pl, &new_id_clone)?;
                                        Ok(())
                                    })
                                    .await??;

                                    remote_id = new_id;
                                    log::info!(
                                        "Recreated remote playlist {} with new id {} for provider {} during config-driven rename",
                                        playlist_name,
                                        remote_id,
                                        provider.name()
                                    );

                                    // Newly created playlist already has the desired
                                    // display name, so we can stop retrying.
                                    break;
                                }
                                Err(err) => {
                                    log::error!(
                                        "Failed to recreate missing playlist {} for provider {} during config-driven rename: {}",
                                        playlist_name,
                                        provider.name(),
                                        err
                                    );
                                    break;
                                }
                            }
                        }

                        if attempt >= cfg.max_retries_on_error {
                            log::error!(
                                "Config-driven rename failed after {} attempts: {}",
                                attempt,
                                e
                            );
                            break;
                        } else {
                            let exp = std::cmp::min(1u64 << attempt, 60);
                            log::warn!(
                                "Config-driven rename attempt {} failed: {}. Retrying in {}s...",
                                attempt,
                                e,
                                exp
                            );
                            tokio::time::sleep(std::time::Duration::from_secs(exp)).await;
                            continue;
                        }
                    }
                }
            }
        }

        // Apply rename first
        if let Some((from, to)) = rename_opt.clone() {
            let new_remote_name = compute_remote_playlist_name(cfg, provider.name(), &to);
            let mut attempt = 0u32;
            loop {
                attempt += 1;
                let res = provider.rename_playlist(&remote_id, &new_remote_name).await;
                match res {
                    Ok(_) => {
                        log::info!("Renamed remote playlist {} -> {}", remote_id, new_remote_name);
                        // On successful explicit rename, migrate the playlist_map
                        // entry so that the logical playlist key follows the
                        // folder rename. This prevents a subsequent run for the
                        // new logical name from calling ensure_playlist and
                        // creating a duplicate remote playlist.
                        let db_path = cfg.db_path.clone();
                        let prov = provider.name().to_string();
                        let pl_from = playlist_name.clone();
                        let pl_to = to.clone();
                        let _ = tokio::task::spawn_blocking(move || -> Result<(), anyhow::Error> {
                            let conn = rusqlite::Connection::open(db_path)?;
                            crate::db::migrate_playlist_map(&conn, &prov, &pl_from, &pl_to)?;
                            Ok(())
                        })
                        .await;
                        // update playlist_map name? Here we keep playlist_name as local key; remote_id unchanged.
                        break;
                    }
                    Err(e) => {
                        let s = format!("{}", e);
                        // Special handling: if the provider reports that the
                        // playlist id no longer exists (e.g. TIDAL 404),
                        // recreate it from local state and update mappings
                        // using the new target name.
                        if s.contains("tidal rename failed: 404 Not Found") && s.contains("Playlists with id") {
                            log::warn!(
                                "Remote playlist {} (id {}) not found on provider {}; recreating before explicit rename...",
                                playlist_name,
                                remote_id,
                                provider.name()
                            );

                            match provider.ensure_playlist(&new_remote_name, "").await {
                                Ok(new_id) => {
                                    let db_path = cfg.db_path.clone();
                                    let pl = playlist_name.clone();
                                    let prov = provider.name().to_string();
                                    let new_id_clone = new_id.clone();
                                    tokio::task::spawn_blocking(move || -> Result<(), anyhow::Error> {
                                        let conn = rusqlite::Connection::open(db_path)?;
                                        db::upsert_playlist_map(&conn, &prov, &pl, &new_id_clone)?;
                                        Ok(())
                                    })
                                    .await??;

                                    remote_id = new_id;
                                    log::info!(
                                        "Recreated remote playlist {} with new id {} for provider {} during explicit rename",
                                        playlist_name,
                                        remote_id,
                                        provider.name()
                                    );

                                    // Newly created playlist already has the desired
                                    // display name (`new_remote_name`), so we can
                                    // stop retrying.
                                    break;
                                }
                                Err(err) => {
                                    log::error!(
                                        "Failed to recreate missing playlist {} for provider {} during explicit rename: {}",
                                        playlist_name,
                                        provider.name(),
                                        err
                                    );
                                    break;
                                }
                            }
                        }

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

        // Seed add/remove lists from reconciliation diff so that remote
        // contents exactly match the local playlist when desired.
        let mut add_uris: Vec<String> = Vec::new();
        let mut remove_uris: Vec<String> = Vec::new();
        if let Some(desired) = reconcile_desired.take() {
            match provider.list_playlist_tracks(&remote_id).await {
                Ok(remote_current) => {
                    use std::collections::HashSet;
                    let desired_set: HashSet<String> = desired.into_iter().collect();
                    let remote_set: HashSet<String> = remote_current.into_iter().collect();

                    let to_add: Vec<String> = desired_set
                        .difference(&remote_set)
                        .cloned()
                        .collect();
                    let to_remove: Vec<String> = remote_set
                        .difference(&desired_set)
                        .cloned()
                        .collect();

                    if !to_add.is_empty() {
                        log::info!(
                            "Reconcile: playlist {} on provider {} is missing {} tracks; scheduling adds",
                            playlist_name,
                            provider.name(),
                            to_add.len()
                        );
                        add_uris.extend(to_add);
                    }

                    if !to_remove.is_empty() {
                        log::info!(
                            "Reconcile: playlist {} on provider {} has {} extra remote tracks; scheduling removes",
                            playlist_name,
                            provider.name(),
                            to_remove.len()
                        );
                        remove_uris.extend(to_remove);
                    }
                }
                Err(e) => {
                    log::warn!(
                        "Reconcile: failed to list remote tracks for playlist {} on provider {}: {}",
                        playlist_name,
                        provider.name(),
                        e
                    );
                }
            }
        }

        // Resolve track URIs and build add/remove lists (prefer cache/ISRC when possible, then fallback to metadata search)
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
                    let provider_name = provider.name().to_string();
                    move || -> Result<Option<(Option<String>, Option<String>)>, anyhow::Error> {
                        let conn = rusqlite::Connection::open(db_path)?;
                        Ok(db::get_track_cache_by_local(&conn, &provider_name, &local_path)?)
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

                // Try to extract ISRC from local file metadata and perform an ISRC-based search
                let mut isrc_for_lookup: Option<String> = cached.as_ref().and_then(|(i, _)| i.clone());
                if isrc_for_lookup.is_none() {
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
                        let provider_name = provider.name().to_string();
                        tokio::task::spawn_blocking(move || -> Result<(), anyhow::Error> {
                            let conn = rusqlite::Connection::open(db_path)?;
                            let _ = crate::db::upsert_track_cache(&conn, &provider_name, &local_path, Some(code_for_cache.as_str()), None)?;
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
                            let provider_name = provider.name().to_string();
                            tokio::task::spawn_blocking(move || -> Result<(), anyhow::Error> {
                                let conn = rusqlite::Connection::open(db_path)?;
                                let _ = crate::db::upsert_track_cache(&conn, &provider_name, &local_path, Some(isrc_for_cache.as_str()), Some(&uri_clone))?;
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

                // Fallback: derive artist/title from filename and do provider metadata search.
                // Strip any extension and try both "Artist - Title" and "Title - Artist" orders.
                let fname = std::path::Path::new(&tp)
                    .file_name()
                    .and_then(|s| s.to_str())
                    .unwrap_or("");
                let stem = if let Some((base, _ext)) = fname.rsplit_once('.') {
                    base
                } else {
                    fname
                };

                let mut candidates: Vec<(&str, &str)> = Vec::new();
                if let Some((left, right)) = stem.split_once(" - ") {
                    let left = left.trim();
                    let right = right.trim();
                    // First assume "Artist - Title"
                    candidates.push((left, right));
                    // Then try "Title - Artist" if the first fails
                    candidates.push((right, left));
                } else {
                    candidates.push(("", stem));
                }

                let mut resolved_uri: Option<String> = None;
                for (artist, raw_title) in candidates.into_iter() {
                    // Normalize common duplicate suffixes like " copy 5"
                    let mut title = raw_title.trim();
                    let lower = title.to_ascii_lowercase();
                    if let Some(idx) = lower.rfind(" copy ") {
                        // Ensure suffix is exactly " copy <digits>"
                        let suffix = &lower[idx + 6..];
                        if !suffix.is_empty() && suffix.chars().all(|c| c.is_ascii_digit()) {
                            title = title[..idx].trim_end();
                        }
                    }

                    if let Ok(result) = provider.search_track_uri(title, artist).await {
                        if let Some(uri) = result {
                            resolved_uri = Some(uri);
                            break;
                        } else {
                            // try next candidate ordering
                        }
                    } else if let Err(e) = provider.search_track_uri(title, artist).await {
                        log::warn!("Error searching track {} with artist='{}' title='{}': {}", tp, artist, title, e);
                    }
                }

                if let Some(uri) = resolved_uri {
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
                        let provider_name = provider.name().to_string();
                        tokio::task::spawn_blocking(move || -> Result<(), anyhow::Error> {
                            let conn = rusqlite::Connection::open(db_path)?;
                            let _ = crate::db::upsert_track_cache(&conn, &provider_name, &local_path, maybe_isrc.as_deref(), Some(&uri_clone))?;
                            Ok(())
                        })
                        .await??;
                } else {
                    log::warn!("Could not resolve track {} to remote URI", tp);
                }
            }
        }

        // Helper to apply batches with retry/backoff and 429 handling.
        // If the remote playlist is reported as missing (e.g. deleted on the
        // provider side), we will recreate it and update the playlist_map,
        // then retry the batch once with the new playlist id.
        async fn apply_in_batches(
            provider: Arc<dyn Provider>,
            playlist_id: &mut String,
            playlist_name: &str,
            remote_display_name: &str,
            uris: Vec<String>,
            is_add: bool,
            cfg: &Config,
        ) -> Result<()> {
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
                                    // Special handling: if the provider reports that the
                                    // playlist id no longer exists, recreate it and retry
                                    // this batch once with the new id.
                                    if s.contains("tidal add tracks failed: 404 Not Found") && s.contains("Playlists with id") {
                                        log::warn!(
                                            "Remote playlist {} (id {}) not found on provider {}; recreating...",
                                            playlist_name,
                                            playlist_id,
                                            provider.name()
                                        );

                                        // Recreate the playlist via ensure_playlist using the
                                        // current display name, then update playlist_map.
                                        match provider.ensure_playlist(remote_display_name, "").await {
                                            Ok(new_id) => {
                                                let db_path = cfg.db_path.clone();
                                                let pl = playlist_name.to_string();
                                                let prov = provider.name().to_string();
                                                let new_id_clone = new_id.clone();
                                                tokio::task::spawn_blocking(move || -> Result<(), anyhow::Error> {
                                                    let conn = rusqlite::Connection::open(db_path)?;
                                                    crate::db::upsert_playlist_map(&conn, &prov, &pl, &new_id_clone)?;
                                                    Ok(())
                                                })
                                                .await??;

                                                *playlist_id = new_id;
                                                log::info!(
                                                    "Recreated remote playlist {} with new id {} for provider {}",
                                                    playlist_name,
                                                    playlist_id,
                                                    provider.name()
                                                );

                                                // Reset attempts and retry the current chunk with
                                                // the fresh playlist id.
                                                attempt = 0;
                                                continue;
                                            }
                                            Err(err) => {
                                                log::error!(
                                                    "Failed to recreate missing playlist {} for provider {}: {}",
                                                    playlist_name,
                                                    provider.name(),
                                                    err
                                                );
                                                return Err(err);
                                            }
                                        }
                                    }
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
        if let Err(e) = apply_in_batches(provider_arc.clone(), &mut remote_id, &playlist_name, &remote_display_name, remove_uris, false, cfg).await {
                log::error!("Error applying removes for {}: {}", playlist_name, e);
        }
        if let Err(e) = apply_in_batches(provider_arc.clone(), &mut remote_id, &playlist_name, &remote_display_name, add_uris, true, cfg).await {
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
    log::info!("Starting nightly reconcile over root folder {:?}", cfg.root_folder);

    let tree = crate::watcher::InMemoryTree::build(
        &cfg.root_folder,
        if cfg.whitelist.is_empty() { None } else { Some(&cfg.whitelist) },
        Some(&cfg.file_extensions),
    )?;
    // collect thread handles for the enqueue operations so we can join before returning
    let mut handles: Vec<std::thread::JoinHandle<()>> = Vec::new();
        for (folder, _node) in tree.nodes.iter() {
        // Extra safety: respect the folder whitelist again here before
        // writing playlists and enqueueing events, so reconciliation
        // never touches non-whitelisted folders even if they slipped
        // into the tree for any reason.
        if let Some(ref wlvec) = tree.whitelist {
            let path_str = folder.to_string_lossy();
            if !wlvec.iter().any(|re| re.is_match(&path_str)) {
                continue;
            }
        }
        let folder_name = folder.file_name().and_then(|s| s.to_str()).unwrap_or("");
        let rel = folder.strip_prefix(&cfg.root_folder).unwrap_or(folder).to_path_buf();

        let path_to_parent = rel.parent().map(|p| p.to_path_buf()).unwrap_or_else(|| std::path::PathBuf::new());
        let path_to_parent_str = if path_to_parent.as_os_str().is_empty() {
            String::new()
        } else {
            let mut s = path_to_parent.display().to_string();
            if !s.ends_with(std::path::MAIN_SEPARATOR) {
                s.push(std::path::MAIN_SEPARATOR);
            }
            s
        };

        let playlist_name = crate::util::expand_template(&cfg.local_playlist_template, folder_name, &path_to_parent_str);
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
        let pname = rel.display().to_string();
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

    log::info!("Nightly reconcile completed for root folder {:?}", cfg.root_folder);

    Ok(())
}