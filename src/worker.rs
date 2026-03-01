use crate::api::{spotify::SpotifyProvider, tidal::TidalProvider, Provider};
use crate::collapse::collapse_events;
use crate::config::Config;
use crate::db;
use crate::models::{Event, EventAction};
use crate::watcher::{compile_whitelist, matches_whitelist};
use anyhow::{Context, Result};
use chrono::Utc;

use std::sync::Arc;
use uuid::Uuid;

const PHASE_WIDTH: usize = 10;

/// negative lookup cache entries are valid for this many seconds (30 days)
const NEGATIVE_CACHE_TTL_SECS: i64 = 30 * 24 * 3600;

fn log_run_tag(run_id: &str) -> String {
    let short = &run_id[..std::cmp::min(8, run_id.len())];
    format!("[RUN:{run}]", run = short)
}

fn log_playlist_tag(playlist: &str, provider: &str) -> String {
    format!("[PL:\"{}\" | {}]", playlist, provider)
}

fn log_phase_tag(phase: &str) -> String {
    format!("[PH:{:<width$}]", phase, width = PHASE_WIDTH)
}

async fn mark_events_synced_async(pool: db::DbPool, ids: Vec<i64>) -> Result<()> {
    if ids.is_empty() {
        return Ok(());
    }
    tokio::task::spawn_blocking(move || -> Result<(), anyhow::Error> {
        let mut conn = pool.get()?;
        db::mark_events_synced(&mut conn, &ids)?;
        Ok(())
    })
    .await??;
    Ok(())
}

/// Release a playlist processing lock in a background blocking task.
/// Ignores errors so callers can use this in error-recovery paths.
async fn release_lock_async(pool: &db::DbPool, playlist_name: &str, worker_id: &str) {
    let pool = pool.clone();
    let pln = playlist_name.to_string();
    let wid = worker_id.to_string();
    let _ = tokio::task::spawn_blocking(move || -> Result<(), anyhow::Error> {
        let mut conn = pool.get()?;
        db::release_playlist_lock(&mut conn, &pln, &wid)?;
        Ok(())
    })
    .await;
}

/// Compute the set of remote track URIs that should be present for a playlist
/// based on the current local playlist file contents.
///
/// This helper reads the local .m3u for the logical playlist key, resolves
/// each referenced file to a remote URI using the same cache/lookup logic as
/// the event-driven worker (track_cache first, then ISRC/metadata lookup),
/// and returns the resulting URI set.
pub async fn desired_remote_uris_for_playlist(
    cfg: &Config,
    playlist_name: &str,
    provider: Arc<dyn Provider>,
    db_pool: &db::DbPool,
) -> Result<Vec<String>> {
    use std::io::BufRead;

    // Map logical playlist key back to on-disk .m3u path using the same
    // template as reconcile/watcher: playlist_name is the relative folder
    // path under root_folder.
    let rel = std::path::Path::new(playlist_name);
    let folder = cfg.root_folder.join(rel);
    let folder_name = folder.file_name().and_then(|s| s.to_str()).unwrap_or("");
    let path_to_parent = rel
        .parent()
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| std::path::PathBuf::new());
    let path_to_parent_str = if path_to_parent.as_os_str().is_empty() {
        String::new()
    } else {
        let mut s = path_to_parent.display().to_string();
        if !s.ends_with(std::path::MAIN_SEPARATOR) {
            s.push(std::path::MAIN_SEPARATOR);
        }
        s
    };
    let playlist_file_name = crate::util::expand_template(
        &cfg.local_playlist_template,
        folder_name,
        &path_to_parent_str,
    );
    let playlist_path = folder.join(playlist_file_name);

    if !playlist_path.exists() {
        return Ok(Vec::new());
    }

    // compute metadata so we can consult the cache
    let meta = std::fs::metadata(&playlist_path)?;
    let file_mtime = meta
        .modified()?
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs() as i64;
    let file_size = meta.len() as i64;
    let file_hash = crate::util::hash_file(&playlist_path)?;

    // check playlist cache; if the file is unchanged we can return early
    if let Some((cached_mtime, cached_size, cached_hash, uris_json)) = {
        let pool = db_pool.clone();
        let playlist_name = playlist_name.to_string();
        tokio::task::spawn_blocking(move || -> Result<Option<(i64, i64, String, String)>, anyhow::Error> {
            let conn = pool.get()?;
            Ok(db::get_playlist_cache(&conn, &playlist_name)?)
        })
        .await??
    } {
        if cached_mtime == file_mtime && cached_size == file_size && cached_hash == file_hash {
            // parse the cached JSON and return
            let uris: Vec<String> = serde_json::from_str(&uris_json).unwrap_or_default();
            return Ok(uris);
        }
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
        // lookup existing cache entry (includes resolved_at timestamp)
        let pool = db_pool.clone();
        let provider_name_for_lookup = provider_name.clone();
        let local_path_for_lookup = local_path_str.clone();
        let cached: Option<(Option<String>, Option<String>, i64)> =
            tokio::task::spawn_blocking(
                move || -> Result<Option<(Option<String>, Option<String>, i64)>, anyhow::Error> {
                    let conn = pool.get()?;
                    Ok(db::get_track_cache_by_local(
                        &conn,
                        &provider_name_for_lookup,
                        &local_path_for_lookup,
                    )?)
                },
            )
            .await??;

        if let Some((_cached_isrc, cached_remote_id, resolved_at)) = &cached {
            if let Some(uri) = cached_remote_id {
                uris.push(uri.clone());
                continue;
            } else {
                // negative cache hit? check TTL
                let now = Utc::now().timestamp();
                if now - *resolved_at < NEGATIVE_CACHE_TTL_SECS {
                    // track previously failed to resolve recently; skip further attempts
                    continue;
                }
                // else fall through and re-query
            }
        }

        // Try to extract ISRC from local file metadata and perform an ISRC-based search.
        let p = local_path.clone();
        let extracted =
            tokio::task::spawn_blocking(move || crate::util::extract_isrc_from_path(&p))
                .await
                .unwrap_or(None);

        let mut uri_opt: Option<String> = None;

        if let Some(isrc) = extracted.clone() {
            if let Ok(Some(u)) = provider.search_track_uri_by_isrc(&isrc).await {
                uri_opt = Some(u.clone());

                // Persist into track_cache for future lookups.
                let pool = db_pool.clone();
                let local_path_for_cache = local_path_str.clone();
                let provider_name_for_cache = provider.name().to_string();
                tokio::task::spawn_blocking(move || -> Result<(), anyhow::Error> {
                    let conn = pool.get()?;
                    let _ = db::upsert_track_cache(
                        &conn,
                        &provider_name_for_cache,
                        &local_path_for_cache,
                        Some(isrc.as_str()),
                        Some(&u),
                    );
                    Ok(())
                })
                .await??;
            }
        }

        // Fallback: derive artist/title from filename and search.
        if uri_opt.is_none() {
            let fname = local_path
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
                candidates.push((left, right));
                candidates.push((right, left));
            } else {
                candidates.push(("", stem));
            }
            for (artist, title) in candidates.into_iter() {
                if let Ok(Some(u)) = provider.search_track_uri(title, artist).await {
                    uri_opt = Some(u.clone());

                    // Persist into track_cache.
                    let pool = db_pool.clone();
                    let local_path_for_cache = local_path_str.clone();
                    let provider_name_for_cache = provider.name().to_string();
                    let isrc_clone = extracted.clone();
                    tokio::task::spawn_blocking(move || -> Result<(), anyhow::Error> {
                        let conn = pool.get()?;
                        let _ = db::upsert_track_cache(
                            &conn,
                            &provider_name_for_cache,
                            &local_path_for_cache,
                            isrc_clone.as_deref(),
                            Some(&u),
                        );
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
            // record negative result to avoid repeated lookups for a while
            let pool = db_pool.clone();
            let local_path_for_cache = local_path_str.clone();
            let provider_name_for_cache = provider.name().to_string();
            let maybe_isrc = extracted.clone();
            tokio::task::spawn_blocking(move || -> Result<(), anyhow::Error> {
                let conn = pool.get()?;
                let _ = db::upsert_track_cache(
                    &conn,
                    &provider_name_for_cache,
                    &local_path_for_cache,
                    maybe_isrc.as_deref(),
                    None,
                );
                Ok(())
            })
            .await??;
        }
    }

    // Deduplicate while preserving order.
    let mut seen = std::collections::HashSet::new();
    uris.retain(|u| seen.insert(u.clone()));

    // Drop any URIs that the provider considers invalid.  This uses the
    // Provider::validate_uri trait method so each backend can enforce its
    // own invariants (e.g. Tidal requires a positive numeric id suffix).
    {
        let mut dropped = 0;
        uris.retain(|u| {
            if provider.validate_uri(u) {
                true
            } else {
                log::warn!(
                    "reconcile: dropping invalid {} uri {:?} generated from local playlist",
                    provider.name(),
                    u
                );
                dropped += 1;
                false
            }
        });
        if dropped > 0 {
            log::warn!(
                "reconcile: removed {} invalid {} uri(s) before caching",
                dropped,
                provider.name()
            );
        }
    }

    // store the result in the cache for next time
    {
        let pool = db_pool.clone();
        let playlist_name = playlist_name.to_string();
        let uris_json = serde_json::to_string(&uris)?;
        let file_hash_clone = file_hash.clone();
        tokio::task::spawn_blocking(move || -> Result<(), anyhow::Error> {
            let conn = pool.get()?;
            db::upsert_playlist_cache(
                &conn,
                &playlist_name,
                file_mtime,
                file_size,
                &file_hash_clone,
                &uris_json,
            )?;
            Ok(())
        })
        .await??;
    }

    Ok(uris)
}



/// Decide whether we should pre‑compute the set of desired remote URIs for a
/// playlist before performing any reconciliation work.
///
/// The worker historically called `desired_remote_uris_for_playlist` early in
/// the processing of each playlist, even when the only action was a rename.  In
/// that case we were resolving a potentially large local playlist for no good
/// reason.  The caller can avoid the expense by invoking this predicate and
/// delaying the actual work until a change is required.
pub fn should_precompute_desired(has_delete: bool, has_track_ops: bool, has_rename: bool) -> bool {
    // We only want to calculate the desired URIs if we're not handling a
    // deletion, and there are no explicit track operations or renames.  A
    // nightly "create" event will pass `(false, false, false)` which is the
    // only case where this returns true during normal operation.
    !has_delete && !has_track_ops && !has_rename
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
fn compute_remote_playlist_name(cfg: &Config, _provider_name: &str, playlist_key: &str, supports_folders: bool) -> String {
    let root = cfg.online_root_playlist.trim();
    let structure = cfg.online_playlist_structure.as_str();
    let delim_cfg = cfg.online_folder_flattening_delimiter.as_str();

    // Normalize the logical playlist key to use "/" as a separator so we
    // can split it into parent path and folder_name. Existing databases may
    // still contain old keys like "Album1.m3u"; for those, parent will be
    // empty and folder_name will be the whole string.
    let normalized = playlist_key
        .replace('\\', "/")
        .trim_matches('/')
        .to_string();
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
    worker_id: &str,
    db_pool: &db::DbPool,
) -> Result<()> {
    if uris.is_empty() {
        return Ok(());
    }
    let prov_name = provider.name().to_string();
    // choose an appropriate batch size for the provider
    // we call.  Spotify tolerates large payloads (configurable),
    // but Tidal is strictly limited to 1–20 items per request
    // according to their API.  Applying the provider-specific
    // cap here prevents 400 errors such as
    // "size must be between 1 and 20" which were flooding the
    // logs.
    let batch_size = provider.max_batch_size(cfg);
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
                    let phase = if is_add { "BATCH_ADD" } else { "BATCH_REM" };
                    log::info!(
                        "{} {} {} batch_applied size={} id={}",
                        log_run_tag(worker_id),
                        log_playlist_tag(playlist_name, &prov_name),
                        log_phase_tag(phase),
                        chunk.len(),
                        playlist_id
                    );
                    break;
                }
                Err(e) => {
                    let s = format!("{}", e);
                    // Parse retry_after if provider included it in the error string like `retry_after=Some(5)`
                    let retry_after_secs = s
                        .split("retry_after=")
                        .nth(1)
                        .and_then(|rest| {
                            // rest might be like "Some(5)" or "None" or "5"
                            let token = rest.trim();
                            if token.starts_with("Some(") {
                                token.trim_start_matches("Some(").split(')').next()
                            } else if token.starts_with("None") {
                                None
                            } else {
                                // take digits prefix
                                Some(
                                    token
                                        .split(|c: char| !c.is_digit(10))
                                        .next()
                                        .unwrap_or("")
                                        .trim(),
                                )
                            }
                        })
                        .and_then(|s| s.parse::<u64>().ok());

                    if s.contains("rate_limited") || retry_after_secs.is_some() {
                        let wait = retry_after_secs.unwrap_or_else(|| {
                            // exponential backoff cap 60s
                            let exp = 2u64.saturating_pow(std::cmp::min(attempt, 6));
                            std::cmp::min(exp, 60)
                        });
                        let phase = if is_add { "BATCH_ADD" } else { "BATCH_REM" };
                        log::warn!(
                            "{} {} {} rate_limited wait_s={} error={}",
                            log_run_tag(worker_id),
                            log_playlist_tag(
                                playlist_name,
                                &prov_name
                            ),
                            log_phase_tag(phase),
                            wait,
                            e
                        );
                        // also log the actual sleep duration so it's easy to spot in
                        // the logs when a 429 triggers backoff
                        log::info!(
                            "{} {} {} Sleeping for {} seconds",
                            log_run_tag(worker_id),
                            log_playlist_tag(
                                playlist_name,
                                &prov_name
                            ),
                            log_phase_tag(phase),
                            wait + 1
                        );
                        tokio::time::sleep(std::time::Duration::from_secs(wait + 1))
                            .await;
                        // continue retrying until max_retries_on_error
                        if attempt >= cfg.max_retries_on_error {
                            let phase = if is_add { "BATCH_ADD" } else { "BATCH_REM" };
                            log::error!(
                                "{} {} {} rate_limit_give_up attempts={} error={}",
                                log_run_tag(worker_id),
                                log_playlist_tag(
                                    playlist_name,
                                    &prov_name
                                ),
                                log_phase_tag(phase),
                                attempt,
                                e
                            );
                            return Err(e);
                        }
                        continue;
                    } else {
                        // Special handling: if the provider reports that the
                        // playlist id no longer exists, recreate it and retry
                        // this batch once with the new id.
                        if s.contains("tidal add tracks failed: 404 Not Found")
                            && s.contains("Playlists with id")
                        {
                            let phase = if is_add { "BATCH_ADD" } else { "BATCH_REM" };
                            log::warn!(
                                "{} {} {} playlist_missing_for_batch id={}",
                                log_run_tag(worker_id),
                                log_playlist_tag(
                                    playlist_name,
                                    &prov_name
                                ),
                                log_phase_tag(phase),
                                playlist_id
                            );

                            // Recreate the playlist via ensure_playlist using the
                            // current display name, then update playlist_map.
                            match provider
                                .ensure_playlist(remote_display_name, "")
                                .await
                            {
                                Ok(new_id) => {
                                    let pool = db_pool.clone();
                                    let pl = playlist_name.to_string();
                                    let prov = prov_name.clone();
                                    let new_id_clone = new_id.clone();
                                    let dn = remote_display_name.to_string();
                                    tokio::task::spawn_blocking(
                                        move || -> Result<(), anyhow::Error> {
                                            let conn =
                                                pool.get()?;
                                            crate::db::upsert_playlist_map(
                                                &conn,
                                                &prov,
                                                &pl,
                                                &new_id_clone,
                                            )?;
                                            crate::db::set_remote_display_name(
                                                &conn,
                                                &prov,
                                                &pl,
                                                &dn,
                                            )?;
                                            Ok(())
                                        },
                                    )
                                    .await??;

                                    *playlist_id = new_id;
                                    let phase =
                                        if is_add { "BATCH_ADD" } else { "BATCH_REM" };
                                    log::info!(
                                        "{} {} {} playlist_recreated_for_batch new_id={}",
                                        log_run_tag(worker_id),
                                        log_playlist_tag(playlist_name, &prov_name),
                                        log_phase_tag(phase),
                                        playlist_id
                                    );

                                    // Reset attempts and retry the current chunk with
                                    // the fresh playlist id.
                                    attempt = 0;
                                    continue;
                                }
                                Err(err) => {
                                    let phase =
                                        if is_add { "BATCH_ADD" } else { "BATCH_REM" };
                                    log::error!(
                                        "{} {} {} playlist_recreate_for_batch_failed error={}",
                                        log_run_tag(worker_id),
                                        log_playlist_tag(playlist_name, &prov_name),
                                        log_phase_tag(phase),
                                        err
                                    );
                                    return Err(err);
                                }
                            }
                        }
                        if attempt >= cfg.max_retries_on_error {
                            let phase = if is_add { "BATCH_ADD" } else { "BATCH_REM" };
                            log::error!(
                                "{} {} {} batch_give_up attempts={} error={}",
                                log_run_tag(worker_id),
                                log_playlist_tag(
                                    playlist_name,
                                    &prov_name
                                ),
                                log_phase_tag(phase),
                                attempt,
                                e
                            );
                            return Err(e);
                        } else {
                            let exp = std::cmp::min(1u64 << attempt, 60);
                            let phase = if is_add { "BATCH_ADD" } else { "BATCH_REM" };
                            log::warn!(
                                "{} {} {} batch_retry attempt={} error={} backoff_s={}",
                                log_run_tag(worker_id),
                                log_playlist_tag(
                                    playlist_name,
                                    &prov_name
                                ),
                                log_phase_tag(phase),
                                attempt,
                                e,
                                exp
                            );
                            log::info!(
                                "{} {} {} Sleeping for {} seconds",
                                log_run_tag(worker_id),
                                log_playlist_tag(
                                    playlist_name,
                                    &prov_name
                                ),
                                log_phase_tag(phase),
                                exp
                            );
                            tokio::time::sleep(std::time::Duration::from_secs(exp))
                                .await;
                            continue;
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

pub async fn run_worker_once(cfg: &Config) -> Result<()> {
    let worker_id = Uuid::new_v4().to_string();

    // Create a connection pool and run migrations once.
    let db_pool = db::create_pool(&cfg.db_path)?;

    // Fetch unsynced events (blocking)
    let events: Vec<Event> = tokio::task::spawn_blocking({
        let pool = db_pool.clone();
        move || -> Result<Vec<Event>, anyhow::Error> {
            let conn = pool.get().context("pool: fetch unsynced events")?;
            db::fetch_unsynced_events(&conn).map_err(|e| e.into())
        }
    })
    .await??;

    if events.is_empty() {
        log::info!("{} No pending events", log_run_tag(&worker_id));
        return Ok(());
    }

    // Backpressure
    if let Some(thresh) = cfg.queue_length_stop_cloud_sync_threshold {
        if events.len() as u64 > thresh {
            log::warn!(
                "{} Queue length {} > threshold {}; stopping worker processing",
                log_run_tag(&worker_id),
                events.len(),
                thresh
            );
            return Ok(());
        }
    }

    // Collect all authenticated providers
    let mut providers: Vec<(String, Arc<dyn Provider>)> = Vec::new();
    // Spotify
    let has_spotify = tokio::task::spawn_blocking({
        let pool = db_pool.clone();
        move || -> Result<bool, anyhow::Error> {
            let conn = pool.get().context("pool: load spotify credentials")?;
            Ok(db::load_credential_with_client(&conn, "spotify")?.is_some())
        }
    })
    .await??;
    if has_spotify {
        log::info!("{} Using Spotify provider", log_run_tag(&worker_id));
        providers.push((
            "spotify".to_string(),
            Arc::new(SpotifyProvider::new(
                String::new(),
                String::new(),
                cfg.db_path.clone(),
            )),
        ));
    }
    // Tidal
    let has_tidal = tokio::task::spawn_blocking({
        let pool = db_pool.clone();
        move || -> Result<bool, anyhow::Error> {
            let conn = pool.get().context("pool: load tidal credentials")?;
            Ok(db::load_credential_with_client(&conn, "tidal")?.is_some())
        }
    })
    .await??;
    if has_tidal {
        log::info!("{} Using Tidal provider", log_run_tag(&worker_id));
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
        log::warn!(
            "{} No valid provider credentials configured. Queue will not be consumed.",
            log_run_tag(&worker_id)
        );
        return Ok(());
    }

    let remote_whitelist = compile_whitelist(Some(cfg.effective_remote_whitelist()));

    // Group events per playlist_name
    use std::collections::HashMap;
    let mut groups: HashMap<String, Vec<Event>> = HashMap::new();
    for ev in events.into_iter() {
        groups.entry(ev.playlist_name.clone()).or_default().push(ev);
    }

    // For each playlist, process all providers
    // This ensures that for a given playlist, all configured
    // providers are updated before moving on to the next one.
    for (playlist_name, evs) in &groups {
        let playlist_folder = cfg.root_folder.join(playlist_name);
        if !matches_whitelist(&playlist_folder, &remote_whitelist) {
            let ids_to_mark: Vec<i64> = evs.iter().map(|ev| ev.id).collect();
            mark_events_synced_async(db_pool.clone(), ids_to_mark).await?;
            continue;
        }
        // Process providers sequentially (safety). Could be parallelized with locks.
        // Collapse events and compute original IDs once (invariant across providers).
        let collapsed = collapse_events(evs);
        let original_ids: Vec<i64> = evs.iter().map(|ev| ev.id).collect();

        let mut rename_opt: Option<(String, String)> = None;
        let mut has_delete: bool = false;
        let mut track_ops: Vec<(EventAction, Option<String>)> = Vec::new();
        for op in &collapsed {
            match &op.action {
                EventAction::Rename { from, to } => {
                    rename_opt = Some((from.clone(), to.clone()))
                }
                EventAction::Delete => has_delete = true,
                EventAction::Add => track_ops.push((EventAction::Add, op.track_path.clone())),
                EventAction::Remove => {
                    track_ops.push((EventAction::Remove, op.track_path.clone()))
                }
                _ => {}
            }
        }

        // When a folder rename is present, rewrite stale track paths so that
        // file-based resolution (ISRC extraction, etc.) can find the files at
        // their new location on disk.
        if let Some((ref from_name, ref to_name)) = rename_opt {
            let old_prefix = cfg
                .root_folder
                .join(from_name)
                .to_string_lossy()
                .to_string();
            let new_prefix = cfg
                .root_folder
                .join(to_name)
                .to_string_lossy()
                .to_string();
            for (_action, track_path_opt) in track_ops.iter_mut() {
                if let Some(tp) = track_path_opt {
                    if tp.starts_with(&old_prefix) {
                        let rewritten = format!("{}{}", new_prefix, &tp[old_prefix.len()..]);
                        log::debug!(
                            "rename_rewrite: {} -> {}",
                            tp,
                            rewritten
                        );
                        *tp = rewritten;
                    }
                }
            }
        }

        let mut all_providers_ok = true;
        for (provider_name, provider) in &providers {
            let pl_tag = log_playlist_tag(playlist_name, provider_name);
            log::info!(
                "{} {} {} starting playlist processing (events={})",
                log_run_tag(&worker_id),
                pl_tag,
                log_phase_tag("START"),
                evs.len()
            );

            // Try acquire lock (TTL = 10 minutes default)
            let lock_acquired = tokio::task::spawn_blocking({
                let pool = db_pool.clone();
                let pl = playlist_name.clone();
                let wid = worker_id.clone();
                move || -> Result<bool, anyhow::Error> {
                    let mut conn = pool.get()?;
                    Ok(db::try_acquire_playlist_lock(&mut conn, &pl, &wid, 600)?)
                }
            })
            .await??;

            if !lock_acquired {
                log::info!(
                    "{} {} {} skipped: lock not acquired",
                    log_run_tag(&worker_id),
                    pl_tag,
                    log_phase_tag("LOCK")
                );
                all_providers_ok = false;
                continue;
            }

            let adds = track_ops
                .iter()
                .filter(|(a, _)| matches!(a, EventAction::Add))
                .count();
            let removes = track_ops
                .iter()
                .filter(|(a, _)| matches!(a, EventAction::Remove))
                .count();
            let renames = if rename_opt.is_some() { 1 } else { 0 };
            let deletes = if has_delete { 1 } else { 0 };

            log::info!(
            "{} {} {} events_collapsed adds={} removes={} deletes={} renames={} original_ids={}",
            log_run_tag(&worker_id),
            pl_tag,
            log_phase_tag("COLLAPSE"),
            adds,
            removes,
            deletes,
            renames,
            original_ids.len()
        );

            // Resolve remote playlist id from playlist_map (or create via provider.ensure_playlist if needed).
            let remote_id_opt = tokio::task::spawn_blocking({
                let pool = db_pool.clone();
                let pl = playlist_name.clone();
                let prov = provider_name.clone();
                move || -> Result<Option<String>, anyhow::Error> {
                    let conn = pool.get()?;
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
                                log::info!(
                                    "{} {} {} deleted_remote_playlist id={}",
                                    log_run_tag(&worker_id),
                                    pl_tag,
                                    log_phase_tag("DELETE"),
                                    remote_id
                                );
                                break;
                            }
                            Err(e) => {
                                if attempt >= cfg.max_retries_on_error {
                                    log::error!(
                                        "{} {} {} delete_failed attempts={} error={}",
                                        log_run_tag(&worker_id),
                                        pl_tag,
                                        log_phase_tag("DELETE"),
                                        attempt,
                                        e
                                    );
                                    break;
                                } else {
                                    log::warn!(
                                        "{} {} {} delete_retry attempt={} error={}",
                                        log_run_tag(&worker_id),
                                        pl_tag,
                                        log_phase_tag("DELETE"),
                                        attempt,
                                        e
                                    );
                                    let sleep_secs = 1 << attempt;
                                    log::info!(
                                        "{} {} {} Sleeping for {} seconds",
                                        log_run_tag(&worker_id),
                                        pl_tag,
                                        log_phase_tag("DELETE"),
                                        sleep_secs
                                    );
                                    tokio::time::sleep(std::time::Duration::from_secs(
                                        sleep_secs,
                                    ))
                                    .await;
                                    continue;
                                }
                            }
                        }
                    }
                } else {
                    log::info!(
                        "{} {} {} no_remote_id_for_delete",
                        log_run_tag(&worker_id),
                        pl_tag,
                        log_phase_tag("DELETE")
                    );
                }

                // Remove local playlist_map entry regardless of whether remote deletion succeeded
                let pl = playlist_name.clone();
                let pool = db_pool.clone();
                let prov = provider_name.clone();
                tokio::task::spawn_blocking(move || -> Result<(), anyhow::Error> {
                    let conn = pool.get()?;
                    let _ = db::delete_playlist_map(&conn, &prov, &pl)?;
                    Ok(())
                })
                .await??;

                // Mark original events as synced (so they don't get retried forever)
                let ids_clone = original_ids.clone();
                let pool = db_pool.clone();
                tokio::task::spawn_blocking(move || -> Result<(), anyhow::Error> {
                    let mut conn = pool.get()?;
                    if !ids_clone.is_empty() {
                        db::mark_events_synced(&mut conn, &ids_clone)?;
                    }
                    Ok(())
                })
                .await??;

                // release lock
                release_lock_async(&db_pool, playlist_name, &worker_id).await;

                log::info!(
                    "{} {} {} playlist_deleted_and_events_synced count={}",
                    log_run_tag(&worker_id),
                    pl_tag,
                    log_phase_tag("FINALIZE"),
                    original_ids.len()
                );

                // Move to next playlist/provider
                continue;
            }

            // Precompute desired remote URIs based on the current local playlist
            // so we can reconcile remote contents later once we have a playlist id.
            //
            // IMPORTANT: this operation can be expensive (it may hit the provider for
            // every track in the local playlist) so avoid doing it unless we know we
            // are going to perform a modification.  In particular we skip the work if
            // there are any explicit track add/remove events or if the only thing we
            // are doing is a rename.  A nightly ``Create`` event still needs the
            // computation because it's the only way we can detect an out‑of‑sync
            // playlist.
            let mut reconcile_desired: Option<Vec<String>> = None;
            let has_track_ops = !track_ops.is_empty();
            if !has_delete && !has_track_ops && rename_opt.is_none() {
                log::info!(
                    "{} {} {} reconcile_compute_desired_uris",
                    log_run_tag(&worker_id),
                    pl_tag,
                    log_phase_tag("RESOLVE")
                );
                match desired_remote_uris_for_playlist(cfg, playlist_name, provider.clone(), &db_pool).await {
                    Ok(desired) => {
                        log::info!(
                            "{} {} {} reconcile_desired_uris_computed count={}",
                            log_run_tag(&worker_id),
                            pl_tag,
                            log_phase_tag("RESOLVE"),
                            desired.len()
                        );
                        if !desired.is_empty() {
                            reconcile_desired = Some(desired);
                        }
                    }
                    Err(e) => {
                        log::warn!(
                            "{} {} {} reconcile_compute_desired_uris_failed error={}",
                            log_run_tag(&worker_id),
                            pl_tag,
                            log_phase_tag("RESOLVE"),
                            e
                        );
                    }
                }
            }

            // Compute the desired remote display name for this playlist on this provider.
            let remote_display_name =
                compute_remote_playlist_name(cfg, provider.name(), playlist_name, provider.supports_folder_nesting());

            let mut remote_id = if let Some(rid) = remote_id_opt {
                rid
            } else {
                // create via provider.ensure_playlist
                match provider.ensure_playlist(&remote_display_name, "").await {
                    Ok(rid) => {
                        // persist
                        let pl = playlist_name.clone();
                        let pool = db_pool.clone();
                        let rid_clone = rid.clone();
                        let prov = provider_name.clone();
                        let dn = remote_display_name.clone();
                        tokio::task::spawn_blocking(move || -> Result<(), anyhow::Error> {
                            let conn = pool.get()?;
                            db::upsert_playlist_map(&conn, &prov, &pl, &rid_clone)?;
                            db::set_remote_display_name(&conn, &prov, &pl, &dn)?;
                            Ok(())
                        })
                        .await??;
                        rid
                    }
                    Err(e) => {
                        log::error!(
                            "{} {} {} ensure_remote_playlist_failed error={}",
                            log_run_tag(&worker_id),
                            pl_tag,
                            log_phase_tag("REMOTE_ID"),
                            e
                        );
                        // release lock and continue
                        release_lock_async(&db_pool, playlist_name, &worker_id).await;
                        all_providers_ok = false;
                        continue;
                    }
                }
            };

            // If we have a stored remote id, verify with the provider that it is
            // still a valid, accessible playlist. This covers the case where the
            // user "deletes" (unfollows) a Spotify playlist in the client while
            // our local mapping still points at the old id.
            // verify playlist validity, retrying on transient errors (429/rate limit)
            {
                let mut attempt = 0u32;
                loop {
                    attempt += 1;
                    match provider.playlist_is_valid(&remote_id).await {
                        Ok(None) => {
                            log::warn!(
                                "{} {} {} remote_playlist_inaccessible id={}",
                                log_run_tag(&worker_id),
                                pl_tag,
                                log_phase_tag("REMOTE_ID"),
                                remote_id
                            );
                            match provider.ensure_playlist(&remote_display_name, "").await {
                                Ok(new_id) => {
                                    let pool = db_pool.clone();
                                    let pl = playlist_name.clone();
                                    let prov = provider_name.clone();
                                    let new_id_clone = new_id.clone();
                                    let dn = remote_display_name.clone();
                                    tokio::task::spawn_blocking(move || -> Result<(), anyhow::Error> {
                                        let conn = pool.get()?;
                                        db::upsert_playlist_map(&conn, &prov, &pl, &new_id_clone)?;
                                        db::set_remote_display_name(&conn, &prov, &pl, &dn)?;
                                        Ok(())
                                    })
                                    .await??;

                                    log::info!(
                                        "{} {} {} remote_playlist_recreated new_id={}",
                                        log_run_tag(&worker_id),
                                        pl_tag,
                                        log_phase_tag("REMOTE_ID"),
                                        new_id
                                    );
                                    remote_id = new_id;
                                }
                                Err(e) => {
                                    log::error!(
                                        "{} {} {} remote_playlist_recreate_failed error={}",
                                        log_run_tag(&worker_id),
                                        pl_tag,
                                        log_phase_tag("REMOTE_ID"),
                                        e
                                    );

                                    // Release lock and skip further processing for this playlist.
                                    release_lock_async(&db_pool, playlist_name, &worker_id).await;
                                    all_providers_ok = false;
                                    continue;
                                }
                            }
                            break;
                        }
                        Ok(Some(current_remote_name)) => {
                            // Opportunistically cache the remote display name
                            // that the provider already fetched.
                            if !current_remote_name.is_empty() {
                                let pool = db_pool.clone();
                                let pl = playlist_name.clone();
                                let prov = provider_name.clone();
                                let rn = current_remote_name.clone();
                                let _ = tokio::task::spawn_blocking(move || {
                                    if let Ok(conn) = pool.get() {
                                        let _ = db::set_remote_display_name(&conn, &prov, &pl, &rn);
                                    }
                                })
                                .await;
                            }
                            // normal case, keep existing id
                            break;
                        }
                        Err(e) => {
                            let s = format!("{}", e);
                            if s.contains("rate_limited") || s.contains("429") {
                                // back off and retry
                                let exp = std::cmp::min(1u64 << attempt, 60);
                                log::warn!(
                                    "{} {} {} rate_limited remote_id_wait_s={} error={}",
                                    log_run_tag(&worker_id),
                                    pl_tag,
                                    log_phase_tag("REMOTE_ID"),
                                    exp,
                                    e
                                );
                                log::info!(
                                    "{} {} {} Sleeping for {} seconds",
                                    log_run_tag(&worker_id),
                                    pl_tag,
                                    log_phase_tag("REMOTE_ID"),
                                    exp
                                );
                                tokio::time::sleep(std::time::Duration::from_secs(exp)).await;
                                if attempt >= cfg.max_retries_on_error {
                                    log::error!(
                                        "{} {} {} remote_id_check_give_up attempts={} error={}",
                                        log_run_tag(&worker_id),
                                        pl_tag,
                                        log_phase_tag("REMOTE_ID"),
                                        attempt,
                                        e
                                    );
                                    break;
                                }
                                continue;
                            } else {
                                log::warn!(
                                    "{} {} {} playlist_is_valid_check_failed id={} error={}",
                                    log_run_tag(&worker_id),
                                    pl_tag,
                                    log_phase_tag("REMOTE_ID"),
                                    remote_id,
                                    e
                                );
                                break;
                            }
                        }
                    }
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
            //
            // To avoid hitting the provider API every run when the name is
            // already correct, we check the cached remote display name in
            // the database and skip the PATCH if it already matches.
            let needs_cfg_rename = if rename_opt.is_none() && remote_display_name != *playlist_name {
                let cached_name = {
                    let pool = db_pool.clone();
                    let prov = provider_name.clone();
                    let pl = playlist_name.clone();
                    tokio::task::spawn_blocking(move || -> Result<Option<String>, anyhow::Error> {
                        let conn = pool.get()?;
                        Ok(db::get_remote_display_name(&conn, &prov, &pl)?)
                    })
                    .await??
                };
                if cached_name.as_deref() == Some(&remote_display_name) {
                    log::info!(
                        "{} {} {} display_name_already_correct id={} name={}",
                        log_run_tag(&worker_id),
                        pl_tag,
                        log_phase_tag("RENAME"),
                        remote_id,
                        remote_display_name
                    );
                    false
                } else {
                    true
                }
            } else {
                false
            };
            if needs_cfg_rename {
                let mut attempt = 0u32;
                loop {
                    attempt += 1;
                    let res = provider
                        .rename_playlist(&remote_id, &remote_display_name)
                        .await;
                    match res {
                        Ok(_) => {
                            log::info!(
                                "{} {} {} ensured_display_name id={} name={}",
                                log_run_tag(&worker_id),
                                pl_tag,
                                log_phase_tag("RENAME"),
                                remote_id,
                                remote_display_name
                            );
                            // Cache the display name so subsequent runs skip the rename.
                            {
                                let pool = db_pool.clone();
                                let prov = provider_name.clone();
                                let pl = playlist_name.clone();
                                let dn = remote_display_name.clone();
                                let _ = tokio::task::spawn_blocking(move || {
                                    if let Ok(conn) = pool.get() {
                                        let _ = db::set_remote_display_name(&conn, &prov, &pl, &dn);
                                    }
                                })
                                .await;
                            }
                            break;
                        }
                        Err(e) => {
                            let s = format!("{}", e);
                            // Special handling: if the provider reports that the
                            // playlist id no longer exists (e.g. TIDAL 404),
                            // recreate it from local state and update mappings.
                            if s.contains("tidal rename failed: 404 Not Found")
                                && s.contains("Playlists with id")
                            {
                                log::warn!(
                                    "{} {} {} missing_before_cfg_rename id={}",
                                    log_run_tag(&worker_id),
                                    pl_tag,
                                    log_phase_tag("RENAME"),
                                    remote_id
                                );

                                match provider.ensure_playlist(&remote_display_name, "").await {
                                    Ok(new_id) => {
                                        let pool = db_pool.clone();
                                        let pl = playlist_name.clone();
                                        let prov = provider_name.clone();
                                        let new_id_clone = new_id.clone();
                                        tokio::task::spawn_blocking(
                                            move || -> Result<(), anyhow::Error> {
                                                let conn = pool.get()?;
                                                db::upsert_playlist_map(
                                                    &conn,
                                                    &prov,
                                                    &pl,
                                                    &new_id_clone,
                                                )?;
                                                Ok(())
                                            },
                                        )
                                        .await??;

                                        remote_id = new_id;
                                        log::info!(
                                            "{} {} {} recreated_before_cfg_rename new_id={}",
                                            log_run_tag(&worker_id),
                                            pl_tag,
                                            log_phase_tag("RENAME"),
                                            remote_id
                                        );

                                        // Newly created playlist already has the desired
                                        // display name, so we can stop retrying.
                                        // Cache it in DB too.
                                        {
                                            let pool = db_pool.clone();
                                            let prov = provider_name.clone();
                                            let pl = playlist_name.clone();
                                            let dn = remote_display_name.clone();
                                            let _ = tokio::task::spawn_blocking(move || {
                                                if let Ok(conn) = pool.get() {
                                                    let _ = db::set_remote_display_name(&conn, &prov, &pl, &dn);
                                                }
                                            })
                                            .await;
                                        }
                                        break;
                                    }
                                    Err(err) => {
                                        log::error!(
                                            "{} {} {} recreate_before_cfg_rename_failed error={}",
                                            log_run_tag(&worker_id),
                                            pl_tag,
                                            log_phase_tag("RENAME"),
                                            err
                                        );
                                        break;
                                    }
                                }
                            }

                            if attempt >= cfg.max_retries_on_error {
                                log::error!(
                                    "{} {} {} cfg_rename_failed attempts={} error={}",
                                    log_run_tag(&worker_id),
                                    pl_tag,
                                    log_phase_tag("RENAME"),
                                    attempt,
                                    e
                                );
                                break;
                            } else {
                                let exp = std::cmp::min(1u64 << attempt, 60);
                                log::warn!(
                                    "{} {} {} cfg_rename_retry attempt={} error={} backoff_s={}",
                                    log_run_tag(&worker_id),
                                    pl_tag,
                                    log_phase_tag("RENAME"),
                                    attempt,
                                    e,
                                    exp
                                );
                                log::info!(
                                    "{} {} {} Sleeping for {} seconds",
                                    log_run_tag(&worker_id),
                                    pl_tag,
                                    log_phase_tag("RENAME"),
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
            if let Some((_from, to)) = rename_opt.clone() {
                let new_remote_name = compute_remote_playlist_name(cfg, provider.name(), &to, provider.supports_folder_nesting());
                let mut attempt = 0u32;
                loop {
                    attempt += 1;
                    let res = provider.rename_playlist(&remote_id, &new_remote_name).await;
                    match res {
                        Ok(_) => {
                            log::info!(
                                "{} {} {} explicit_rename id={} new_name={}",
                                log_run_tag(&worker_id),
                                pl_tag,
                                log_phase_tag("RENAME"),
                                remote_id,
                                new_remote_name
                            );
                            // On successful explicit rename, migrate the playlist_map
                            // entry so that the logical playlist key follows the
                            // folder rename. This prevents a subsequent run for the
                            // new logical name from calling ensure_playlist and
                            // creating a duplicate remote playlist.
                            let pool = db_pool.clone();
                            let prov = provider_name.clone();
                            let pl_from = playlist_name.clone();
                            let pl_to = to.clone();
                            let new_name_clone = new_remote_name.clone();
                            let _ = tokio::task::spawn_blocking(
                                move || -> Result<(), anyhow::Error> {
                                    let conn = pool.get()?;
                                    crate::db::migrate_playlist_map(
                                        &conn, &prov, &pl_from, &pl_to,
                                    )?;
                                    // keep playlist cache in sync as well
                                    crate::db::migrate_playlist_cache(
                                        &conn, &pl_from, &pl_to,
                                    )?;
                                    // Cache the new display name under the new key
                                    crate::db::set_remote_display_name(
                                        &conn, &prov, &pl_to, &new_name_clone,
                                    )?;
                                    Ok(())
                                },
                            )
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
                            if s.contains("tidal rename failed: 404 Not Found")
                                && s.contains("Playlists with id")
                            {
                                log::warn!(
                                    "{} {} {} missing_before_explicit_rename id={}",
                                    log_run_tag(&worker_id),
                                    pl_tag,
                                    log_phase_tag("RENAME"),
                                    remote_id
                                );

                                match provider.ensure_playlist(&new_remote_name, "").await {
                                    Ok(new_id) => {
                                        let pool = db_pool.clone();
                                        let pl = playlist_name.clone();
                                        let prov = provider_name.clone();
                                        let new_id_clone = new_id.clone();
                                        tokio::task::spawn_blocking(
                                            move || -> Result<(), anyhow::Error> {
                                                let conn = pool.get()?;
                                                db::upsert_playlist_map(
                                                    &conn,
                                                    &prov,
                                                    &pl,
                                                    &new_id_clone,
                                                )?;
                                                Ok(())
                                            },
                                        )
                                        .await??;

                                        remote_id = new_id;
                                        log::info!(
                                            "{} {} {} recreated_before_explicit_rename new_id={}",
                                            log_run_tag(&worker_id),
                                            pl_tag,
                                            log_phase_tag("RENAME"),
                                            remote_id
                                        );

                                        // Newly created playlist already has the desired
                                        // display name (`new_remote_name`), so we can
                                        // stop retrying.
                                        break;
                                    }
                                    Err(err) => {
                                        log::error!(
                                        "{} {} {} recreate_before_explicit_rename_failed error={}",
                                        log_run_tag(&worker_id),
                                        pl_tag,
                                        log_phase_tag("RENAME"),
                                        err
                                    );
                                        break;
                                    }
                                }
                            }

                            if attempt >= cfg.max_retries_on_error {
                                log::error!(
                                    "{} {} {} explicit_rename_failed attempts={} error={}",
                                    log_run_tag(&worker_id),
                                    pl_tag,
                                    log_phase_tag("RENAME"),
                                    attempt,
                                    e
                                );
                                break;
                            } else {
                                log::warn!(
                                    "{} {} {} explicit_rename_retry attempt={} error={}",
                                    log_run_tag(&worker_id),
                                    pl_tag,
                                    log_phase_tag("RENAME"),
                                    attempt,
                                    e
                                );
                                let sleep_secs = 1 << attempt;
                                log::info!(
                                    "{} {} {} Sleeping for {} seconds",
                                    log_run_tag(&worker_id),
                                    pl_tag,
                                    log_phase_tag("RENAME"),
                                    sleep_secs
                                );
                                tokio::time::sleep(std::time::Duration::from_secs(sleep_secs))
                                    .await;
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

                        let to_add: Vec<String> =
                            desired_set.difference(&remote_set).cloned().collect();
                        let to_remove: Vec<String> =
                            remote_set.difference(&desired_set).cloned().collect();

                        if !to_add.is_empty() {
                            log::info!(
                                "{} {} {} reconcile_missing_tracks scheduling_adds={} ",
                                log_run_tag(&worker_id),
                                pl_tag,
                                log_phase_tag("RECONCILE"),
                                to_add.len()
                            );
                            add_uris.extend(to_add);
                        }

                        if !to_remove.is_empty() {
                            log::info!(
                                "{} {} {} reconcile_extra_tracks scheduling_removes={} ",
                                log_run_tag(&worker_id),
                                pl_tag,
                                log_phase_tag("RECONCILE"),
                                to_remove.len()
                            );
                            remove_uris.extend(to_remove);
                        }
                    }
                    Err(e) => {
                        log::warn!(
                            "{} {} {} reconcile_list_remote_failed error={}",
                            log_run_tag(&worker_id),
                            pl_tag,
                            log_phase_tag("RECONCILE"),
                            e
                        );
                    }
                }
            }

            // Resolve track URIs and build add/remove lists (prefer cache/ISRC when possible, then fallback to metadata search)
            log::info!(
                "{} {} {} resolve_uris_start ops={}",
                log_run_tag(&worker_id),
                pl_tag,
                log_phase_tag("RESOLVE"),
                track_ops.len()
            );

            for (act, track_path_opt) in track_ops.clone().into_iter() {
                if let Some(mut tp) = track_path_opt {
                    // provider-originated URIs may come in directly; we want to
                    // re-resolve everything against the *local* files so that each
                    // destination provider does its own availability lookup.  The
                    // only case where we accept a bare URI is when the event was
                    // generated locally (`uri::…`).
                    if tp.starts_with("uri::") {
                        let uri = tp.trim_start_matches("uri::").to_string();
                        match act {
                            EventAction::Add => add_uris.push(uri),
                            EventAction::Remove => remove_uris.push(uri),
                            _ => {}
                        }
                        continue;
                    }

                    // non-local URIs (spotify:track:..., tidal:track:...) should be
                    // mapped back to a local path via the cache.  If the cache has no
                    // entry, we drop the operation entirely; the local playlist file
                    // is the golden source.
                    if tp.contains(':') && !tp.starts_with(&format!("{}:", provider.name())) {
                        let pool = db_pool.clone();
                        let uri_clone = tp.clone();
                        if let Some((_isrc, mut local_path, _)) = tokio::task::spawn_blocking(move || -> Result<Option<(Option<String>, String, i64)>, anyhow::Error> {
                            let conn = pool.get()?;
                            Ok(db::get_track_cache_by_remote(&conn, &uri_clone)?)
                        })
                        .await??
                        {
                            // the stored path may include an old provider prefix ("tidal::foo")
                            // when we migrated the cache.  strip anything up to the first
                            // "::" so that the downstream resolution logic always works with
                            // the raw filesystem path.
                            if let Some(idx) = local_path.find("::") {
                                local_path = local_path[idx + 2..].to_string();
                            }
                            tp = local_path;
                        } else {
                            log::warn!(
                                "reconcile: skipping provider URI {} because no corresponding local file was cached",
                                tp
                            );
                            continue;
                        }
                    }

                    // Try track cache first (with timestamp) to avoid unnecessary lookups
                    let cached: Option<(Option<String>, Option<String>, i64)> =
                        tokio::task::spawn_blocking({
                            let pool = db_pool.clone();
                            let local_path = tp.clone();
                            let provider_name = provider.name().to_string();
                            move || -> Result<Option<(Option<String>, Option<String>, i64)>, anyhow::Error> {
                                let conn = pool.get()?;
                                Ok(db::get_track_cache_by_local(&conn, &provider_name, &local_path)?)
                            }
                        })
                        .await??;

                    if let Some((_cached_isrc, cached_remote_id, resolved_at)) = &cached {
                        if let Some(uri) = cached_remote_id {
                            match act {
                                EventAction::Add => add_uris.push(uri.clone()),
                                EventAction::Remove => remove_uris.push(uri.clone()),
                                _ => {}
                            }
                            continue;
                        } else {
                            let now = Utc::now().timestamp();
                            if now - *resolved_at < NEGATIVE_CACHE_TTL_SECS {
                                // negative hit still fresh
                                continue;
                            }
                        }
                    }

                    // Try to extract ISRC from local file metadata and perform an ISRC-based search
                    let mut isrc_for_lookup: Option<String> =
                        cached.as_ref().and_then(|(i, _, _)| i.clone());
                    if isrc_for_lookup.is_none() {
                        let p = std::path::Path::new(&tp).to_path_buf();
                        let extracted = match tokio::task::spawn_blocking(move || {
                            crate::util::extract_isrc_from_path(&p)
                        })
                        .await
                        {
                            Ok(v) => v,
                            Err(e) => {
                                log::warn!(
                                    "{} {} {} isrc_extract_task_failed track={} error={}",
                                    log_run_tag(&worker_id),
                                    pl_tag,
                                    log_phase_tag("RESOLVE"),
                                    tp,
                                    e
                                );
                                None
                            }
                        };
                        if let Some(code) = extracted {
                            isrc_for_lookup = Some(code.clone());
                            // persist locally extracted ISRC in cache (without remote id yet)
                            let pool = db_pool.clone();
                            let local_path = tp.clone();
                            let code_for_cache = code.clone();
                            let provider_name = provider.name().to_string();
                            tokio::task::spawn_blocking(move || -> Result<(), anyhow::Error> {
                                let conn = pool.get()?;
                                let _ = crate::db::upsert_track_cache(
                                    &conn,
                                    &provider_name,
                                    &local_path,
                                    Some(code_for_cache.as_str()),
                                    None,
                                )?;
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
                                let pool = db_pool.clone();
                                let local_path = tp.clone();
                                let uri_clone = uri.clone();
                                let isrc_for_cache = isrc.clone();
                                let provider_name = provider.name().to_string();
                                tokio::task::spawn_blocking(
                                    move || -> Result<(), anyhow::Error> {
                                        let conn = pool.get()?;
                                        let _ = crate::db::upsert_track_cache(
                                            &conn,
                                            &provider_name,
                                            &local_path,
                                            Some(isrc_for_cache.as_str()),
                                            Some(&uri_clone),
                                        )?;
                                        Ok(())
                                    },
                                )
                                .await??;
                                continue;
                            }
                            #[allow(non_snake_case)]
                            Ok(None) => {
                                // fall through to metadata-based search
                            }
                            Err(e) => {
                                log::warn!(
                                    "{} {} {} isrc_search_failed track={} error={}",
                                    log_run_tag(&worker_id),
                                    pl_tag,
                                    log_phase_tag("RESOLVE"),
                                    tp,
                                    e
                                );
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
                            log::warn!(
                            "{} {} {} metadata_search_failed track={} artist={} title={} error={}",
                            log_run_tag(&worker_id),
                            pl_tag,
                            log_phase_tag("RESOLVE"),
                            tp,
                            artist,
                            title,
                            e
                        );
                        }
                    }

                    if let Some(uri) = resolved_uri {
                        match act {
                            EventAction::Add => add_uris.push(uri.clone()),
                            EventAction::Remove => remove_uris.push(uri.clone()),
                            _ => {}
                        }
                        // attempt to lookup ISRC from provider; persist resolved uri + isrc into track_cache
                        let pool = db_pool.clone();
                        let local_path = tp.clone();
                        let uri_clone = uri.clone();
                        let provider_clone = provider.clone();
                        let maybe_isrc = provider_clone
                            .lookup_track_isrc(&uri_clone)
                            .await
                            .unwrap_or(None);
                        let provider_name = provider.name().to_string();
                        tokio::task::spawn_blocking(move || -> Result<(), anyhow::Error> {
                            let conn = pool.get()?;
                            let _ = crate::db::upsert_track_cache(
                                &conn,
                                &provider_name,
                                &local_path,
                                maybe_isrc.as_deref(),
                                Some(&uri_clone),
                            )?;
                            Ok(())
                        })
                        .await??;
                    } else {
                        log::warn!(
                            "{} {} {} track_unresolved track={}",
                            log_run_tag(&worker_id),
                            pl_tag,
                            log_phase_tag("RESOLVE"),
                            tp
                        );
                        // record negative cache entry so we don't retry soon
                        let pool = db_pool.clone();
                        let local_path_for_cache = tp.clone();
                        let provider_name_for_cache = provider.name().to_string();
                        let maybe_isrc = cached.as_ref().and_then(|(i,_,_)| i.clone());
                        tokio::task::spawn_blocking(move || -> Result<(), anyhow::Error> {
                            let conn = pool.get()?;
                            let _ = crate::db::upsert_track_cache(
                                &conn,
                                &provider_name_for_cache,
                                &local_path_for_cache,
                                maybe_isrc.as_deref(),
                                None,
                            );
                            Ok(())
                        })
                        .await??;
                    }
                }
            }

            // Deduplicate URIs in add/remove lists before applying batches.
            // This avoids double-applying the same track when both
            // reconciliation and event-driven paths schedule an operation
            // for the same URI in a single run.
            if !add_uris.is_empty() {
                use std::collections::HashSet;
                let mut seen: HashSet<String> = HashSet::new();
                add_uris.retain(|u| seen.insert(u.clone()));
            }
            if !remove_uris.is_empty() {
                use std::collections::HashSet;
                let mut seen: HashSet<String> = HashSet::new();
                remove_uris.retain(|u| seen.insert(u.clone()));
            }


            let provider_arc = provider.clone();
            if let Err(e) = apply_in_batches(
                provider_arc.clone(),
                &mut remote_id,
                &playlist_name,
                &remote_display_name,
                remove_uris,
                false,
                cfg,
                &worker_id,
                &db_pool,
            )
            .await
            {
                log::error!(
                    "{} {} {} apply_removes_failed error={}",
                    log_run_tag(&worker_id),
                    pl_tag,
                    log_phase_tag("BATCH_REM"),
                    e
                );
                all_providers_ok = false;
            }
            if let Err(e) = apply_in_batches(
                provider_arc.clone(),
                &mut remote_id,
                &playlist_name,
                &remote_display_name,
                add_uris,
                true,
                cfg,
                &worker_id,
                &db_pool,
            )
            .await
            {
                log::error!(
                    "{} {} {} apply_adds_failed error={}",
                    log_run_tag(&worker_id),
                    pl_tag,
                    log_phase_tag("BATCH_ADD"),
                    e
                );
                all_providers_ok = false;
            }

            // release lock
            release_lock_async(&db_pool, playlist_name, &worker_id).await;

            log::info!(
                "{} {} {} provider_done",
                log_run_tag(&worker_id),
                pl_tag,
                log_phase_tag("FINALIZE"),
            );
        }

        // Mark events as synced only when ALL providers succeeded.  If any
        // provider failed we leave the events unsynced so the next worker
        // cycle will retry them.
        if all_providers_ok {
            mark_events_synced_async(db_pool.clone(), original_ids.clone()).await?;
            log::info!(
                "{} events_synced count={} playlist={:?}",
                log_run_tag(&worker_id),
                original_ids.len(),
                playlist_name
            );
        } else {
            log::warn!(
                "{} events_NOT_synced count={} playlist={:?} (some providers failed, will retry)",
                log_run_tag(&worker_id),
                original_ids.len(),
                playlist_name
            );
        }
    }
    Ok(())
}

/// Nightly reconciliation: scan the root folder, write playlists for every folder,
/// and enqueue a `Create` event so the worker will reconcile remote playlists later.
pub fn run_nightly_reconcile(cfg: &Config) -> Result<()> {
    log::info!(
        "Starting nightly reconcile over root folder {:?}",
        cfg.root_folder
    );

    let remote_whitelist = cfg.effective_remote_whitelist();
    let tree = crate::watcher::InMemoryTree::build(
        &cfg.root_folder,
        if remote_whitelist.is_empty() {
            None
        } else {
            Some(remote_whitelist)
        },
        Some(&cfg.file_extensions),
    )?;
    // collect thread handles for the enqueue operations so we can join before returning
    let db_pool = db::create_pool(&cfg.db_path)?;
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
        let rel = folder
            .strip_prefix(&cfg.root_folder)
            .unwrap_or(folder)
            .to_path_buf();

        let path_to_parent = rel
            .parent()
            .map(|p| p.to_path_buf())
            .unwrap_or_else(|| std::path::PathBuf::new());
        let path_to_parent_str = if path_to_parent.as_os_str().is_empty() {
            String::new()
        } else {
            let mut s = path_to_parent.display().to_string();
            if !s.ends_with(std::path::MAIN_SEPARATOR) {
                s.push(std::path::MAIN_SEPARATOR);
            }
            s
        };

        let playlist_name = crate::util::expand_template(
            &cfg.local_playlist_template,
            folder_name,
            &path_to_parent_str,
        );
        let playlist_path = folder.join(&playlist_name);

        if cfg.playlist_mode == "flat" {
            if let Err(e) = crate::playlist::write_flat_playlist(
                folder,
                &playlist_path,
                &cfg.playlist_order_mode,
                &cfg.file_extensions,
            ) {
                log::warn!("Failed to write playlist {:?}: {}", playlist_path, e);
            }
        } else {
            if let Err(e) = crate::playlist::write_linked_playlist(
                folder,
                &playlist_path,
                &cfg.linked_reference_format,
                &cfg.local_playlist_template,
            ) {
                log::warn!("Failed to write linked playlist {:?}: {}", playlist_path, e);
            }
        }

        // enqueue Create event in a background thread but keep the handle to join
        let pname = rel.display().to_string();
        let pool = db_pool.clone();
        let h = std::thread::spawn(move || {
            if let Ok(conn) = pool.get() {
                if let Err(e) = crate::db::enqueue_event(
                    &conn,
                    &pname,
                    &crate::models::EventAction::Create,
                    None,
                    None,
                ) {
                    log::warn!(
                        "Failed to enqueue nightly create event for {}: {}",
                        pname,
                        e
                    );
                }
            } else {
                log::warn!("Failed to get pooled connection to enqueue nightly event");
            }
        });
        handles.push(h);
    }

    // join all enqueue threads to ensure events are persisted before returning
    for h in handles {
        let _ = h.join();
    }

    log::info!(
        "Nightly reconcile completed for root folder {:?}",
        cfg.root_folder
    );

    Ok(())
}
