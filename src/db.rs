use crate::models::{Event, EventAction};
use anyhow::{Context, Result};
use chrono::Utc;
use rusqlite::{params, Connection, OptionalExtension};
use std::path::Path;

/// A thread-safe connection pool backed by r2d2 + rusqlite.
pub type DbPool = r2d2::Pool<r2d2_sqlite::SqliteConnectionManager>;

/// Create a connection pool for the given database path.
/// The pool is pre-initialized with a small number of connections so that
/// `spawn_blocking` tasks can check out a connection without opening a new one
/// every time.
pub fn create_pool(path: &Path) -> Result<DbPool> {
    let manager = r2d2_sqlite::SqliteConnectionManager::file(path);
    let pool = r2d2::Pool::builder()
        .max_size(4)
        .build(manager)
        .with_context(|| format!("building DB pool for {}", path.display()))?;
    // Run migrations on a fresh connection from the pool.
    {
        let conn = pool.get().with_context(|| "getting pool connection for migrations")?;
        run_migrations(&conn)?;
    }
    Ok(pool)
}

pub fn open_or_create(path: &Path) -> Result<Connection> {
    let conn = Connection::open(path)?;
    run_migrations(&conn)?;
    Ok(conn)
}

pub fn run_migrations(conn: &Connection) -> Result<()> {
    // In development/tests, the schema file lives at db/schema.sql relative
    // to the project root. In packaged installs, it is installed to
    // /usr/share/music-file-playlist-online-sync/schema.sql and the
    // .install hook already applies it once at install/upgrade time.
    //
    // At runtime we treat missing schema files as "no-op" so that
    // packaged binaries do not fail if the on-disk schema file is absent.

    // Try local development path first.
    let candidates = [
        "db/schema.sql",
        "/usr/share/music-file-playlist-online-sync/schema.sql",
    ];

    let mut last_err: Option<std::io::Error> = None;
    let mut applied = false;
    for path in &candidates {
        match std::fs::read_to_string(path) {
            Ok(sql) => {
                conn.execute_batch(&sql)
                    .with_context(|| format!("applying DB schema from {}", path))?;
                applied = true;
                break;
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                // Try next candidate.
                last_err = Some(e);
                continue;
            }
            Err(e) => return Err(e).with_context(|| format!("reading DB schema from {}", path)),
        }
    }

    // If we didn't apply any schema file, that's not necessarily an error –
    // packaged installs may have already applied schema via the .install
    // hook.  Log at debug level so the situation is visible during
    // development.
    if !applied {
        if let Some(e) = last_err {
            tracing::debug!(
                "DB schema file not found at any known location; skipping migrations: {}",
                e
            );
        }
    }

    // ------------------------------------------------------------------
    // Perform one–time data migrations that cannot be expressed via raw
    // schema.sql.  Historically the `track_cache.local_path` column held
    // the bare filesystem path for Spotify entries; other providers were
    // added later and namespaced by prefixing with "<provider>::".  To
    // allow us to switch to a uniformly-prefixed key we update any rows
    // that lack the separator so that subsequent lookups work correctly.
    // This is safe to run on every start because the WHERE clause only
    // matches unprefixed values.
    //
    // We deliberately ignore "no such table" errors, which can happen if
    // the schema has not been applied yet; in that case the caller will
    // apply the schema and run migrations again when it re‑opens the
    // database.
    let _ = conn.execute(
        "UPDATE track_cache SET local_path = 'spotify::' || local_path \
         WHERE local_path NOT LIKE '%::%';",
        [],
    );

    // Add remote_display_name column to playlist_map if it doesn't exist yet.
    let _ = conn.execute(
        "ALTER TABLE playlist_map ADD COLUMN remote_display_name TEXT;",
        [],
    );

    // Ensure the remote_playlist_contents_cache table exists.  It may be
    // absent in databases created before this feature was added.
    let _ = conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS remote_playlist_contents_cache ( \
           provider_name TEXT NOT NULL, \
           playlist_name TEXT NOT NULL, \
           remote_id TEXT NOT NULL, \
           uris TEXT NOT NULL, \
           cached_at INTEGER NOT NULL, \
           PRIMARY KEY (provider_name, playlist_name) \
         );",
    );

    // Cached playlist item-id maps (track_id -> itemId slots) keyed by
    // provider + remote playlist UUID.  Persisted across runs because this
    // tool is the sole writer to these playlists, so itemIds are stable
    // until we ourselves mutate the playlist.
    let _ = conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS remote_playlist_item_id_cache ( \
           provider_name TEXT NOT NULL, \
           remote_id TEXT NOT NULL, \
           item_ids_json TEXT NOT NULL, \
           etag TEXT, \
           cached_at INTEGER NOT NULL, \
           PRIMARY KEY (provider_name, remote_id) \
         );",
    );

    // Historically playlist_map stored bare playlist_name keys for Spotify.
    // We now uniformly prefix every key with "<provider>::".  Migrate any
    // remaining bare keys by prefixing them with "spotify::".
    let _ = conn.execute(
        "UPDATE playlist_map SET playlist_name = 'spotify::' || playlist_name \
         WHERE playlist_name NOT LIKE '%::%';",
        [],
    );

    // playlist_cache was originally keyed by playlist_name alone, which
    // caused cross-provider URI contamination (Spotify URIs served to
    // Tidal, etc.).  The new schema uses a composite PK
    // (playlist_name, provider_name).  If the old single-column PK table
    // still exists, drop it so that the CREATE TABLE IF NOT EXISTS in
    // schema.sql creates the correct version.  The cache will be rebuilt
    // transparently on the next worker run.
    {
        let has_provider_col: bool = conn
            .prepare("SELECT 1 FROM pragma_table_info('playlist_cache') WHERE name = 'provider_name'")
            .and_then(|mut s| s.exists([]))
            .unwrap_or(false);

        if !has_provider_col {
            let _ = conn.execute("DROP TABLE IF EXISTS playlist_cache", []);
            // Re-apply schema so the new table is created immediately.
            for path in &candidates {
                if let Ok(sql) = std::fs::read_to_string(path) {
                    let _ = conn.execute_batch(&sql);
                    break;
                }
            }
        }
    }

    Ok(())
}

pub fn enqueue_event(
    conn: &Connection,
    playlist_name: &str,
    action: &EventAction,
    track_path: Option<&str>,
    extra: Option<&str>,
) -> Result<()> {
    let action_str = match action {
        EventAction::Add => "add",
        EventAction::Remove => "remove",
        EventAction::Rename { .. } => "rename",
        EventAction::Create => "create",
        EventAction::Delete => "delete",
    };
    let now = chrono::Utc::now().timestamp_millis();
    conn.execute(
        "INSERT INTO event_queue (timestamp, playlist_name, action, track_path, extra, is_synced) VALUES (?1, ?2, ?3, ?4, ?5, 0)",
        params![now, playlist_name, action_str, track_path, extra],
    )?;
    Ok(())
}

pub fn fetch_unsynced_events(conn: &Connection) -> Result<Vec<Event>> {
    let mut stmt = conn.prepare("SELECT id, timestamp, playlist_name, action, track_path, extra, is_synced FROM event_queue WHERE is_synced = 0 ORDER BY timestamp ASC")?;
    let rows = stmt.query_map([], |r| {
        let action: String = r.get(3)?;
        let event_action = match action.as_str() {
            "add" => EventAction::Add,
            "remove" => EventAction::Remove,
            "rename" => {
                // attempt to parse extra JSON for rename details {"from":"...","to":"..."}
                let extra_json: Option<String> = r.get(5).ok();
                if let Some(es) = extra_json {
                    if let Ok(j) = serde_json::from_str::<serde_json::Value>(&es) {
                        let from = j
                            .get("from")
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .to_string();
                        let to = j
                            .get("to")
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .to_string();
                        EventAction::Rename { from, to }
                    } else {
                        EventAction::Rename {
                            from: "".into(),
                            to: "".into(),
                        }
                    }
                } else {
                    EventAction::Rename {
                        from: "".into(),
                        to: "".into(),
                    }
                }
            }
            "create" => EventAction::Create,
            "delete" => EventAction::Delete,
            _ => EventAction::Create,
        };
        Ok(Event {
            id: r.get(0)?,
            timestamp_ms: r.get(1)?,
            playlist_name: r.get(2)?,
            action: event_action,
            track_path: r.get(4).ok(),
            extra: r.get(5).ok(),
            is_synced: r.get::<_, i64>(6)? != 0,
        })
    })?;
    let mut v = Vec::new();
    for r in rows {
        v.push(r?);
    }
    Ok(v)
}

/// Clear all unsynced events from the event_queue table.
/// Returns the number of rows removed.
pub fn clear_unsynced_events(conn: &mut Connection) -> Result<usize> {
    let tx = conn.transaction()?;
    let removed = tx.execute("DELETE FROM event_queue WHERE is_synced = 0", [])?;
    tx.commit()?;
    Ok(removed)
}

pub fn mark_events_synced(conn: &mut Connection, ids: &[i64]) -> Result<()> {
    let tx = conn.transaction()?;
    for id in ids {
        tx.execute(
            "UPDATE event_queue SET is_synced = 1 WHERE id = ?1",
            params![id],
        )?;
    }
    tx.commit()?;
    Ok(())
}

/// Save raw credential JSON for a provider (provider = "spotify" or "tidal")

/// Save raw credential JSON for a provider, with optional client_id/client_secret
pub fn save_credential_raw(
    conn: &Connection,
    provider: &str,
    json_blob: &str,
    client_id: Option<&str>,
    client_secret: Option<&str>,
) -> Result<()> {
    conn.execute(
        "INSERT INTO credentials (provider, token_json, client_id, client_secret, last_refreshed) VALUES (?1, ?2, ?3, ?4, strftime('%s','now')) ON CONFLICT(provider) DO UPDATE SET token_json = excluded.token_json, client_id = excluded.client_id, client_secret = excluded.client_secret, last_refreshed = strftime('%s','now')",
        params![provider, json_blob, client_id, client_secret],
    )?;
    Ok(())
}

/// Load raw credential JSON for a provider

/// Load raw credential JSON and client_id/client_secret for a provider
pub fn load_credential_with_client(
    conn: &Connection,
    provider: &str,
) -> Result<Option<(String, Option<String>, Option<String>)>> {
    let mut stmt = conn.prepare(
        "SELECT token_json, client_id, client_secret FROM credentials WHERE provider = ?1 LIMIT 1",
    )?;
    let row = stmt
        .query_row(params![provider], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, Option<String>>(1)?,
                r.get::<_, Option<String>>(2)?,
            ))
        })
        .optional()?;
    Ok(row)
}

fn playlist_map_key(provider: &str, playlist_name: &str) -> String {
    // All providers are uniformly namespaced as "<provider>::<playlist_name>".
    format!("{}::{}", provider.to_lowercase(), playlist_name)
}

/// Get remote_id for playlist from playlist_map, scoped by provider
pub fn get_remote_playlist_id(
    conn: &Connection,
    provider: &str,
    playlist_name: &str,
) -> Result<Option<String>> {
    let key = playlist_map_key(provider, playlist_name);
    let mut stmt =
        conn.prepare("SELECT remote_id FROM playlist_map WHERE playlist_name = ?1 LIMIT 1")?;
    let row = stmt
        .query_row(params![key], |r| r.get::<_, Option<String>>(0))
        .optional()?;
    Ok(row.flatten())
}

/// Upsert playlist_map entry with remote_id, scoped by provider
pub fn upsert_playlist_map(
    conn: &Connection,
    provider: &str,
    playlist_name: &str,
    remote_id: &str,
) -> Result<()> {
    let key = playlist_map_key(provider, playlist_name);
    conn.execute(
        "INSERT INTO playlist_map (playlist_name, remote_id, last_synced_at) VALUES (?1, ?2, strftime('%s','now')) ON CONFLICT(playlist_name) DO UPDATE SET remote_id = excluded.remote_id, last_synced_at = strftime('%s','now')",
        params![key, remote_id],
    )?;
    Ok(())
}

/// Get the cached remote display name for a playlist_map entry.
pub fn get_remote_display_name(
    conn: &Connection,
    provider: &str,
    playlist_name: &str,
) -> Result<Option<String>> {
    let key = playlist_map_key(provider, playlist_name);
    let mut stmt = conn.prepare(
        "SELECT remote_display_name FROM playlist_map WHERE playlist_name = ?1 LIMIT 1",
    )?;
    let row = stmt
        .query_row(params![key], |r| r.get::<_, Option<String>>(0))
        .optional()?;
    Ok(row.flatten())
}

/// Update the cached remote display name for a playlist_map entry.
pub fn set_remote_display_name(
    conn: &Connection,
    provider: &str,
    playlist_name: &str,
    display_name: &str,
) -> Result<()> {
    let key = playlist_map_key(provider, playlist_name);
    conn.execute(
        "UPDATE playlist_map SET remote_display_name = ?1 WHERE playlist_name = ?2",
        params![display_name, key],
    )?;
    Ok(())
}

/// Return all remote_ids tracked in playlist_map for the given provider.
pub fn get_all_remote_ids_for_provider(
    conn: &Connection,
    provider: &str,
) -> Result<std::collections::HashSet<String>> {
    let prefix = format!("{}::", provider.to_lowercase());
    let mut stmt = conn.prepare(
        "SELECT remote_id FROM playlist_map WHERE playlist_name LIKE ?1 AND remote_id IS NOT NULL",
    )?;
    let rows = stmt.query_map(params![format!("{}%", prefix)], |r| {
        r.get::<_, String>(0)
    })?;
    let mut ids = std::collections::HashSet::new();
    for row in rows {
        ids.insert(row?);
    }
    Ok(ids)
}

/// Delete a playlist_map entry by playlist_name, scoped by provider
pub fn delete_playlist_map(conn: &Connection, provider: &str, playlist_name: &str) -> Result<()> {
    let key = playlist_map_key(provider, playlist_name);
    conn.execute(
        "DELETE FROM playlist_map WHERE playlist_name = ?1",
        params![key],
    )?;
    Ok(())
}

/// Migrate a playlist_map entry from one logical playlist name to another,
/// scoped by provider. Keeps the same remote_id but updates the key so that
/// future events keyed by the new logical name reuse the existing remote
/// playlist instead of creating a duplicate.
pub fn migrate_playlist_map(
    conn: &Connection,
    provider: &str,
    from_playlist_name: &str,
    to_playlist_name: &str,
) -> Result<()> {
    if let Some(remote_id) = get_remote_playlist_id(conn, provider, from_playlist_name)? {
        // Upsert mapping under the new logical name, then delete the old key.
        upsert_playlist_map(conn, provider, to_playlist_name, &remote_id)?;
        delete_playlist_map(conn, provider, from_playlist_name)?;
    }
    Ok(())
}


/// Lookup a playlist cache entry by logical playlist name and provider.
/// Returns (file_mtime, file_size, file_hash, uris_json) if an entry exists.
pub fn get_playlist_cache(
    conn: &Connection,
    playlist_name: &str,
    provider: &str,
) -> Result<Option<(i64, i64, String, String)>> {
    let mut stmt = conn.prepare(
        "SELECT file_mtime, file_size, file_hash, uris FROM playlist_cache WHERE playlist_name = ?1 AND provider_name = ?2 LIMIT 1",
    )?;
    let row = stmt
        .query_row(params![playlist_name, provider], |r| {
            Ok((
                r.get::<_, i64>(0)?,
                r.get::<_, i64>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, String>(3)?,
            ))
        })
        .optional()?;
    Ok(row)
}

/// Upsert a playlist cache entry, scoped by provider. This is called after
/// we resolve a playlist file to a set of remote URIs so we can return the
/// cached result the next time the file is unchanged.
pub fn upsert_playlist_cache(
    conn: &Connection,
    playlist_name: &str,
    provider: &str,
    file_mtime: i64,
    file_size: i64,
    file_hash: &str,
    uris_json: &str,
) -> Result<()> {
    conn.execute(
        "INSERT INTO playlist_cache (playlist_name, provider_name, file_mtime, file_size, file_hash, uris) VALUES (?1, ?2, ?3, ?4, ?5, ?6) ON CONFLICT(playlist_name, provider_name) DO UPDATE SET file_mtime = excluded.file_mtime, file_size = excluded.file_size, file_hash = excluded.file_hash, uris = excluded.uris",
        params![playlist_name, provider, file_mtime, file_size, file_hash, uris_json],
    )?;
    Ok(())
}

/// Get the cached remote playlist contents (list of URIs) for a given provider
/// and playlist.  Returns (remote_id, uris_json) when a cache entry exists.
pub fn get_remote_playlist_contents_cache(
    conn: &Connection,
    provider: &str,
    playlist_name: &str,
) -> Result<Option<(String, String)>> {
    let mut stmt = conn.prepare(
        "SELECT remote_id, uris FROM remote_playlist_contents_cache \
         WHERE provider_name = ?1 AND playlist_name = ?2 LIMIT 1",
    )?;
    let row = stmt
        .query_row(params![provider, playlist_name], |r| {
            Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
        })
        .optional()?;
    Ok(row)
}

/// Upsert the cached remote playlist contents for a given provider and
/// playlist.  Called after every successful list_playlist_tracks response so
/// that subsequent runs with --trust-cache can skip the live request.
pub fn upsert_remote_playlist_contents_cache(
    conn: &Connection,
    provider: &str,
    playlist_name: &str,
    remote_id: &str,
    uris_json: &str,
) -> Result<()> {
    let now = Utc::now().timestamp();
    conn.execute(
        "INSERT INTO remote_playlist_contents_cache \
         (provider_name, playlist_name, remote_id, uris, cached_at) \
         VALUES (?1, ?2, ?3, ?4, ?5) \
         ON CONFLICT(provider_name, playlist_name) DO UPDATE SET \
         remote_id = excluded.remote_id, \
         uris = excluded.uris, \
         cached_at = excluded.cached_at",
        params![provider, playlist_name, remote_id, uris_json, now],
    )?;
    Ok(())
}

/// Migrate a playlist cache entry from one logical name to another, scoped by
/// provider.  When a playlist folder is renamed, the worker performs a similar
/// migration on `playlist_map` so that remote IDs aren't duplicated; the cache
/// needs to be moved as well so that the checksum check continues to work
/// after the rename.  If the source entry does not exist this is a no-op.
pub fn migrate_playlist_cache(
    conn: &Connection,
    provider: &str,
    from_playlist_name: &str,
    to_playlist_name: &str,
) -> Result<()> {
    if let Some((mtime, size, hash, uris)) = get_playlist_cache(conn, from_playlist_name, provider)? {
        upsert_playlist_cache(conn, to_playlist_name, provider, mtime, size, &hash, &uris)?;
        conn.execute(
            "DELETE FROM playlist_cache WHERE playlist_name = ?1 AND provider_name = ?2",
            params![from_playlist_name, provider],
        )?;
    }
    Ok(())
}


fn track_cache_key(provider: &str, local_path: &str) -> String {
    // All providers are now namespaced in the cache key.  In the very first
    // releases only Spotify existed and we stored the raw local path; that
    // caused a mismatch when additional providers were added.  We still need
    // to support old databases, so `run_migrations` will rewrite any
    // existing rows that lack a prefix.  Once the migration has run the
    // helper can consistently prepend the provider name.
    format!("{}::{}", provider.to_lowercase(), local_path)
}

/// Lookup a track cache entry by local path, scoped by provider
pub fn get_track_cache_by_local(
    conn: &Connection,
    provider: &str,
    local_path: &str,
) -> Result<Option<(Option<String>, Option<String>, i64)>> {
    // Returns (isrc, remote_id, resolved_at) if an entry exists.
    let key = track_cache_key(provider, local_path);
    let mut stmt = conn.prepare(
        "SELECT isrc, remote_id, COALESCE(resolved_at,0) FROM track_cache WHERE local_path = ?1 LIMIT 1",
    )?;
    let row = stmt
        .query_row(params![key], |r| {
            Ok((
                r.get::<_, Option<String>>(0)?,
                r.get::<_, Option<String>>(1)?,
                r.get::<_, i64>(2)?,
            ))
        })
        .optional()?;
    Ok(row)
}

/// Upsert track cache entry: (provider, local_path) -> (isrc, remote_id)
pub fn upsert_track_cache(
    conn: &Connection,
    provider: &str,
    local_path: &str,
    isrc: Option<&str>,
    remote_id: Option<&str>,
) -> Result<()> {
    let key = track_cache_key(provider, local_path);
    conn.execute(
        "INSERT INTO track_cache (isrc, local_path, remote_id, resolved_at) VALUES (?1, ?2, ?3, strftime('%s','now')) ON CONFLICT(local_path) DO UPDATE SET isrc = excluded.isrc, remote_id = excluded.remote_id, resolved_at = strftime('%s','now')",
        params![isrc, key, remote_id],
    )?;
    Ok(())
}

/// Lookup a track cache entry by remote URI. Returns (isrc, local_path, resolved_at)
/// if an entry exists. This is used when we receive an event originating from
/// another provider; we convert it back to a local file path so that the target
/// provider can perform its own lookup against the local collection.
pub fn get_track_cache_by_remote(
    conn: &Connection,
    remote_uri: &str,
) -> Result<Option<(Option<String>, String, i64)>> {
    let mut stmt = conn.prepare(
        "SELECT isrc, local_path, COALESCE(resolved_at,0) FROM track_cache WHERE remote_id = ?1 LIMIT 1",
    )?;
    let row = stmt
        .query_row(params![remote_uri], |r| {
            Ok((
                r.get::<_, Option<String>>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, i64>(2)?,
            ))
        })
        .optional()?;
    Ok(row)
}

/// Try to acquire a processing lock for a playlist. Returns true if lock acquired.
/// TTL is seconds for the lease (e.g., 600).
pub fn try_acquire_playlist_lock(
    conn: &mut Connection,
    playlist_name: &str,
    worker_id: &str,
    ttl_seconds: i64,
) -> Result<bool> {
    let now = Utc::now().timestamp();
    let expires_at = now + ttl_seconds;
    let tx = conn.transaction()?;
    // check existing lock
    let row_opt = {
        let mut stmt = tx.prepare(
            "SELECT worker_id, expires_at FROM processing_locks WHERE playlist_name = ?1 LIMIT 1",
        )?;
        stmt.query_row(params![playlist_name], |r| {
            let w: String = r.get(0)?;
            let e: i64 = r.get(1)?;
            Ok((w, e))
        })
        .optional()?
    };

    if let Some((_w, e)) = row_opt {
        if e > now {
            // active lock exists
            tx.commit()?;
            return Ok(false);
        } else {
            // expired lock; we can take over
        }
    }

    // insert or replace
    tx.execute(
        "INSERT INTO processing_locks (playlist_name, worker_id, locked_at, expires_at) VALUES (?1, ?2, ?3, ?4) ON CONFLICT(playlist_name) DO UPDATE SET worker_id = excluded.worker_id, locked_at = excluded.locked_at, expires_at = excluded.expires_at",
        params![playlist_name, worker_id, now, expires_at],
    )?;
    tx.commit()?;
    Ok(true)
}

/// Release a processing lock (only if owned by this worker_id)
pub fn release_playlist_lock(
    conn: &mut Connection,
    playlist_name: &str,
    worker_id: &str,
) -> Result<()> {
    let tx = conn.transaction()?;
    tx.execute(
        "DELETE FROM processing_locks WHERE playlist_name = ?1 AND worker_id = ?2",
        params![playlist_name, worker_id],
    )?;
    tx.commit()?;
    Ok(())
}

/// Get the cached playlist item-id map for a given provider and remote playlist UUID.
/// Returns the full track_id → item_ids mapping and the ETag, if present.
pub fn get_playlist_item_id_cache(
    conn: &Connection,
    provider: &str,
    remote_id: &str,
) -> Result<Option<(std::collections::HashMap<String, Vec<String>>, Option<String>)>> {
    let mut stmt = conn.prepare(
        "SELECT item_ids_json, etag FROM remote_playlist_item_id_cache \
         WHERE provider_name = ?1 AND remote_id = ?2 LIMIT 1",
    )?;
    let row = stmt
        .query_row(params![provider, remote_id], |r| {
            Ok((r.get::<_, String>(0)?, r.get::<_, Option<String>>(1)?))
        })
        .optional()?;
    if let Some((json_str, etag)) = row {
        let map = serde_json::from_str(&json_str)
            .with_context(|| format!("deserializing item_ids_json for remote_id={}", remote_id))?;
        Ok(Some((map, etag)))
    } else {
        Ok(None)
    }
}

/// Upsert the cached playlist item-id map for a given provider and remote playlist UUID.
pub fn upsert_playlist_item_id_cache(
    conn: &Connection,
    provider: &str,
    remote_id: &str,
    item_ids: &std::collections::HashMap<String, Vec<String>>,
    etag: Option<&str>,
) -> Result<()> {
    let json_str = serde_json::to_string(item_ids)
        .with_context(|| format!("serializing item_ids for remote_id={}", remote_id))?;
    let now = Utc::now().timestamp();
    conn.execute(
        "INSERT INTO remote_playlist_item_id_cache \
         (provider_name, remote_id, item_ids_json, etag, cached_at) \
         VALUES (?1, ?2, ?3, ?4, ?5) \
         ON CONFLICT(provider_name, remote_id) DO UPDATE SET \
         item_ids_json = excluded.item_ids_json, \
         etag = excluded.etag, \
         cached_at = excluded.cached_at",
        params![provider, remote_id, json_str, etag, now],
    )?;
    Ok(())
}

/// Delete the cached playlist item-id map for a given provider and remote playlist UUID.
/// Called after any successful mutation so the next run re-fetches fresh positions.
pub fn delete_playlist_item_id_cache(
    conn: &Connection,
    provider: &str,
    remote_id: &str,
) -> Result<()> {
    conn.execute(
        "DELETE FROM remote_playlist_item_id_cache \
         WHERE provider_name = ?1 AND remote_id = ?2",
        params![provider, remote_id],
    )?;
    Ok(())
}
