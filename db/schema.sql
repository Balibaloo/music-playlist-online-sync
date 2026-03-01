-- event queue
CREATE TABLE IF NOT EXISTS event_queue (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  timestamp INTEGER NOT NULL,
  playlist_name TEXT NOT NULL,
  action TEXT NOT NULL,
  track_path TEXT,
  extra TEXT,
  is_synced INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_event_queue_unsynced ON event_queue(is_synced, timestamp);

-- playlist map
CREATE TABLE IF NOT EXISTS playlist_map (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  playlist_name TEXT UNIQUE NOT NULL,
  remote_id TEXT,
  remote_snapshot_id TEXT,
  last_synced_at INTEGER,
  remote_display_name TEXT
);

-- track cache
CREATE TABLE IF NOT EXISTS track_cache (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  isrc TEXT,
  local_path TEXT UNIQUE,
  remote_id TEXT,
  resolved_at INTEGER
);

-- playlist cache: stores metadata for the last-read .m3u file so we can
-- short-circuit expensive resolution when nothing has changed.  The
-- `uris` column contains a JSON array of the computed remote URIs.
-- Keyed by (playlist_name, provider_name) so each provider gets its own
-- cached URI set.
CREATE TABLE IF NOT EXISTS playlist_cache (
  playlist_name TEXT NOT NULL,
  provider_name TEXT NOT NULL DEFAULT '',
  file_mtime INTEGER NOT NULL,
  file_size INTEGER NOT NULL,
  file_hash TEXT NOT NULL,
  uris TEXT NOT NULL,
  PRIMARY KEY (playlist_name, provider_name)
);


-- credentials (now with client_id and client_secret)
CREATE TABLE IF NOT EXISTS credentials (
  provider TEXT PRIMARY KEY,
  token_json TEXT NOT NULL,
  client_id TEXT,
  client_secret TEXT,
  last_refreshed INTEGER
);

-- Remote playlist contents cache: stores the last known set of URIs on the
-- remote provider for each playlist.  Always populated after a successful
-- list_playlist_tracks call; consulted instead of a live request when
-- --trust-cache is set.
CREATE TABLE IF NOT EXISTS remote_playlist_contents_cache (
  provider_name TEXT NOT NULL,
  playlist_name TEXT NOT NULL,
  remote_id TEXT NOT NULL,
  uris TEXT NOT NULL,
  cached_at INTEGER NOT NULL,
  PRIMARY KEY (provider_name, playlist_name)
);

-- processing locks (per-playlist worker lease)
CREATE TABLE IF NOT EXISTS processing_locks (
  playlist_name TEXT PRIMARY KEY,
  worker_id TEXT,
  locked_at INTEGER,
  expires_at INTEGER
);