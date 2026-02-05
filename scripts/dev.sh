#!/usr/bin/env bash
set -euo pipefail

# Simple dev bootstrap for Sprint 0.
# Creates a temporary workspace, copies the example config, initializes sqlite DB from db/schema.sql,
# and prints commands to run watcher and worker.

ROOT_DIR=$(pwd)
TMPDIR=$(mktemp -d /tmp/music-sync-dev-XXXX)
MUSIC_DIR="$TMPDIR/music"
LOG_DIR="$TMPDIR/log"
DB_PATH="$TMPDIR/music-sync.db"
CONFIG_OUT="$TMPDIR/config.toml"

mkdir -p "$MUSIC_DIR" "$LOG_DIR"

echo "Creating temp dev workspace: $TMPDIR"

if [ ! -f "$ROOT_DIR/config/example-config.toml" ]; then
  echo "example-config.toml not found in config/ - aborting"
  exit 1
fi

# Copy and patch example config
sed -e "s|root_folder = .*|root_folder = \"$MUSIC_DIR\"|" \
    -e "s|log_dir = .*|log_dir = \"$LOG_DIR\"|" \
    -e "s|db_path = .*|db_path = \"$DB_PATH\"|" \
    "$ROOT_DIR/config/example-config.toml" > "$CONFIG_OUT"

echo "Wrote config to: $CONFIG_OUT"

if command -v sqlite3 >/dev/null 2>&1; then
  echo "Initializing sqlite DB at $DB_PATH using db/schema.sql"
  sqlite3 "$DB_PATH" < "$ROOT_DIR/db/schema.sql"
  echo "DB initialized"
else
  echo "sqlite3 not found. To initialize DB run:"
  echo "  cat db/schema.sql | sqlite3 $DB_PATH"
fi

echo
echo "Dev workspace ready. Sample commands"
echo
echo "# Run watcher in background (BASH example)"
echo "cargo run -- --config $CONFIG_OUT Watcher &"
echo
echo "# Run worker once"
echo "cargo run -- --config $CONFIG_OUT Worker"
echo
echo "Temporary workspace paths"
echo "  root: $TMPDIR"
echo "  music dir: $MUSIC_DIR"
echo "  log dir: $LOG_DIR"
echo "  db: $DB_PATH"
echo "  config: $CONFIG_OUT"

echo
echo "Notes:"
echo " - Ensure you run 'chmod +x scripts/dev.sh' before first use"
echo " - To reuse a persistent dev DB, copy $CONFIG_OUT to a permanent location and edit db_path"
