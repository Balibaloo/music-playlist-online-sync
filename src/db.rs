use crate::models::{Event, EventAction};
use anyhow::Result;
use rusqlite::{params, Connection, OptionalExtension};
use std::path::Path;
use chrono::Utc;

pub fn open_or_create(path: &Path) -> Result<Connection> {
    let conn = Connection::open(path)?;
    run_migrations(&conn)?;
    Ok(conn)
}

pub fn run_migrations(conn: &Connection) -> Result<()> {
    conn.execute_batch(std::fs::read_to_string("db/schema.sql")?.as_str())?;
    Ok(())
}

pub fn enqueue_event(conn: &Connection, playlist_name: &str, action: &EventAction, track_path: Option<&str>, extra: Option<&str>) -> Result<()> {
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
                            let from = j.get("from").and_then(|v| v.as_str()).unwrap_or("").to_string();
                            let to = j.get("to").and_then(|v| v.as_str()).unwrap_or("").to_string();
                            EventAction::Rename { from, to }
                        } else {
                            EventAction::Rename { from: "".into(), to: "".into() }
                        }
                    } else {
                        EventAction::Rename { from: "".into(), to: "".into() }
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
        tx.execute("UPDATE event_queue SET is_synced = 1 WHERE id = ?1", params![id])?;
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
pub fn load_credential_with_client(conn: &Connection, provider: &str) -> Result<Option<(String, Option<String>, Option<String>)>> {
    let mut stmt = conn.prepare("SELECT token_json, client_id, client_secret FROM credentials WHERE provider = ?1 LIMIT 1")?;
    let row = stmt
        .query_row(params![provider], |r| {
            Ok((r.get::<_, String>(0)?, r.get::<_, Option<String>>(1)?, r.get::<_, Option<String>>(2)?))
        })
        .optional()?;
    Ok(row)
}

/// Get remote_id for playlist from playlist_map
pub fn get_remote_playlist_id(conn: &Connection, playlist_name: &str) -> Result<Option<String>> {
    let mut stmt = conn.prepare("SELECT remote_id FROM playlist_map WHERE playlist_name = ?1 LIMIT 1")?;
    let row = stmt.query_row(params![playlist_name], |r| r.get::<_, Option<String>>(0)).optional()?;
    Ok(row.flatten())
}

/// Upsert playlist_map entry with remote_id
pub fn upsert_playlist_map(conn: &Connection, playlist_name: &str, remote_id: &str) -> Result<()> {
    conn.execute(
        "INSERT INTO playlist_map (playlist_name, remote_id, last_synced_at) VALUES (?1, ?2, strftime('%s','now')) ON CONFLICT(playlist_name) DO UPDATE SET remote_id = excluded.remote_id, last_synced_at = strftime('%s','now')",
        params![playlist_name, remote_id],
    )?;
    Ok(())
}

/// Delete a playlist_map entry by playlist_name
pub fn delete_playlist_map(conn: &Connection, playlist_name: &str) -> Result<()> {
    conn.execute(
        "DELETE FROM playlist_map WHERE playlist_name = ?1",
        params![playlist_name],
    )?;
    Ok(())
}

/// Lookup a track cache entry by local path
pub fn get_track_cache_by_local(conn: &Connection, local_path: &str) -> Result<Option<(Option<String>, Option<String>)>> {
    let mut stmt = conn.prepare("SELECT isrc, remote_id FROM track_cache WHERE local_path = ?1 LIMIT 1")?;
    let row = stmt.query_row(params![local_path], |r| Ok((r.get::<_, Option<String>>(0)?, r.get::<_, Option<String>>(1)?))).optional()?;
    Ok(row)
}

/// Upsert track cache entry: local_path -> (isrc, remote_id)
pub fn upsert_track_cache(conn: &Connection, local_path: &str, isrc: Option<&str>, remote_id: Option<&str>) -> Result<()> {
    conn.execute(
        "INSERT INTO track_cache (isrc, local_path, remote_id, resolved_at) VALUES (?1, ?2, ?3, strftime('%s','now')) ON CONFLICT(local_path) DO UPDATE SET isrc = excluded.isrc, remote_id = excluded.remote_id, resolved_at = strftime('%s','now')",
        params![isrc, local_path, remote_id],
    )?;
    Ok(())
}

/// Try to acquire a processing lock for a playlist. Returns true if lock acquired.
/// TTL is seconds for the lease (e.g., 600).
pub fn try_acquire_playlist_lock(conn: &mut Connection, playlist_name: &str, worker_id: &str, ttl_seconds: i64) -> Result<bool> {
    let now = Utc::now().timestamp();
    let expires_at = now + ttl_seconds;
    let tx = conn.transaction()?;
    // check existing lock
    let row_opt = {
        let mut stmt = tx.prepare("SELECT worker_id, expires_at FROM processing_locks WHERE playlist_name = ?1 LIMIT 1")?;
        stmt.query_row(params![playlist_name], |r| {
            let w: String = r.get(0)?;
            let e: i64 = r.get(1)?;
            Ok((w, e))
        }).optional()?
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
pub fn release_playlist_lock(conn: &mut Connection, playlist_name: &str, worker_id: &str) -> Result<()> {
    let tx = conn.transaction()?;
    tx.execute("DELETE FROM processing_locks WHERE playlist_name = ?1 AND worker_id = ?2", params![playlist_name, worker_id])?;
    tx.commit()?;
    Ok(())
}