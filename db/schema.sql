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
  last_synced_at INTEGER
);

-- track cache
CREATE TABLE IF NOT EXISTS track_cache (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  isrc TEXT,
  local_path TEXT UNIQUE,
  remote_id TEXT,
  resolved_at INTEGER
);

-- credentials
CREATE TABLE IF NOT EXISTS credentials (
  provider TEXT PRIMARY KEY,
  token_json TEXT NOT NULL,
  last_refreshed INTEGER
);

-- processing locks (per-playlist worker lease)
CREATE TABLE IF NOT EXISTS processing_locks (
  playlist_name TEXT PRIMARY KEY,
  worker_id TEXT,
  locked_at INTEGER,
  expires_at INTEGER
);