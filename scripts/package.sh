#!/usr/bin/env bash
set -euo pipefail

# Simple packaging script: builds release binary and creates a tarball containing
# the binary, example config, and systemd unit files. Does not install systemd units.

HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
ROOT=$(cd "$HERE/.." && pwd)
OUTDIR="$ROOT/packaging/dist"

mkdir -p "$OUTDIR"

echo "Building release binary..."
cargo build --release --manifest-path "$ROOT/Cargo.toml"

# The CLI binary is built as `cli` (bin/cli.rs) in target/release. Package it
# under the expected name `music-file-playlist-online-sync` for install paths.
BINARY="$ROOT/target/release/cli"
if [ ! -f "$BINARY" ]; then
  echo "Release binary not found: $BINARY" >&2
  exit 1
fi

PKGNAME="music-file-playlist-online-sync-$(date +%Y%m%d%H%M%S)"
PKGDIR="$OUTDIR/$PKGNAME"
mkdir -p "$PKGDIR"

echo "Copying files..."
# Copy the built CLI and rename to the expected package binary name
cp "$BINARY" "$PKGDIR/music-file-playlist-online-sync"
cp -r "$ROOT/config/example-config.toml" "$PKGDIR/"
mkdir -p "$PKGDIR/systemd"
cp -r "$ROOT/systemd"/* "$PKGDIR/systemd/" 2>/dev/null || true

echo "Creating tarball..."
pushd "$OUTDIR" >/dev/null
tar czf "$PKGNAME.tar.gz" "$PKGNAME"
popd >/dev/null

echo "Package created: $OUTDIR/$PKGNAME.tar.gz"
echo "Contents:"
tar -tzf "$OUTDIR/$PKGNAME.tar.gz"

echo "To install on a system (example):"
echo "# extract"
echo "sudo tar xzf /path/to/$PKGNAME.tar.gz -C /opt/"
echo "# move binary"
echo "sudo install -m 0755 /opt/$PKGNAME/music-file-playlist-online-sync /usr/local/bin/"
echo "# copy systemd units"
echo "sudo cp /opt/$PKGNAME/systemd/* /etc/systemd/system/"
echo "sudo systemctl daemon-reload"
echo "sudo systemctl enable music-file-playlist-online-sync-worker.service"
echo "sudo systemctl start music-file-playlist-online-sync-worker.service"

exit 0
