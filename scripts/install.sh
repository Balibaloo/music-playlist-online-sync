#!/usr/bin/env bash
set -euo pipefail

# build & install helper for the music-playlist-online-sync project
#
# This makes it convenient to rebuild the release binary and copy all
# of the configuration/systemd pieces into the places a packaged
# installation would expect.  You still need to enable/start the
# services yourself (see the messages at the end).

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)

# these paths mirror the policy used by the PKGBUILD/.install file
# and the systemd units.
TARGET_BIN=/usr/bin/music-file-playlist-online-sync
SYSTEMD_DIR=/etc/systemd/system
CONFIG_DIR=/etc/music-sync
CONFIG_FILE=$CONFIG_DIR/config.toml

if ! command -v cargo >/dev/null 2>&1; then
    echo "cargo not found in PATH; please install Rust and Cargo."
    exit 1
fi

# building
echo "building release binary..."
cargo build --release --manifest-path "$ROOT/Cargo.toml"

BINARY="$ROOT/target/release/cli"
if [ ! -x "$BINARY" ]; then
    echo "release binary not found at $BINARY" >&2
    exit 1
fi

# installation (requires root privileges)
if [ "$EUID" -ne 0 ]; then
    echo "This script must be run as root or via sudo." >&2
    exit 1
fi

echo "installing binary to $TARGET_BIN"
install -Dm755 "$BINARY" "$TARGET_BIN"

# service user and runtime directories
SERVICE_USER=music-file-playlist-online-sync
if ! getent group "$SERVICE_USER" >/dev/null; then
    groupadd --system "$SERVICE_USER"
fi
if ! getent passwd "$SERVICE_USER" >/dev/null; then
    useradd --system --no-create-home \
        --shell /usr/bin/nologin \
        --gid "$SERVICE_USER" \
        --home-dir /var/lib/music-sync \
        "$SERVICE_USER"
fi
mkdir -p /var/lib/music-sync /var/log/music-sync /etc/music-sync
touch /var/lib/music-sync/music-sync.db
chown -R "$SERVICE_USER:$SERVICE_USER" /var/lib/music-sync /var/log/music-sync /etc/music-sync
chmod 750 /var/lib/music-sync /var/log/music-sync /etc/music-sync
chmod 640 /var/lib/music-sync/music-sync.db

# config
mkdir -p "$CONFIG_DIR"
if [ ! -e "$CONFIG_FILE" ]; then
    echo "installing example configuration to $CONFIG_FILE"
    install -Dm644 "$ROOT/config/example-config.toml" "$CONFIG_FILE"
    echo "please edit the config file to suit your environment."
else
    echo "$CONFIG_FILE already exists; leaving it untouched."
fi

# systemd units
for unit in "$ROOT/systemd"/*.{service,timer}; do
    [ -e "$unit" ] || continue
    echo "installing $(basename "$unit") to $SYSTEMD_DIR"
    install -Dm644 "$unit" "$SYSTEMD_DIR/$(basename "$unit")"
done

# tell systemd to reload
echo "reloading systemd daemon"
systemctl daemon-reload

cat <<'EOF'

Installation complete.
To enable & start the services run (as root or with sudo):

  systemctl enable --now music-file-playlist-online-sync-watcher.service
  systemctl enable --now music-file-playlist-online-sync-reconcile.timer

NOTE: watcher-driven worker triggering
  The watcher now spawns the worker automatically for file changes:
  - Small batches (≤ watcher_instant_trigger_threshold, default 20 events):
    worker runs immediately after the debounce window.
  - Large batches (> threshold): worker is deferred by
    watcher_deferred_trigger_delay_sec (default 300 s) after activity settles.

NOTE: reconcile now runs the worker
  The Reconcile command (and reconcile.timer) runs the worker immediately
  after enqueueing events — the full scan-then-sync cycle completes in one
  shot without waiting for a separate timer.

EOF
