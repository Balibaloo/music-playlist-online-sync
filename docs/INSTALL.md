# Installation

This document describes a simple way to package and install the `music-file-playlist-online-sync` binary.

Prerequisites
- Rust toolchain to build the binary (for packaging)
- A target system with systemd (installation steps below assume systemd)

Build and package

Run the included packaging script (creates a tarball under `packaging/dist`):

```sh
chmod +x scripts/package.sh
scripts/package.sh
```

Install on target

Example install steps (run as root on the target machine):

```sh
# Extract package (adjust names)
tar xzf /path/to/music-file-playlist-online-sync-YYYYMMDDHHMMSS.tar.gz -C /opt/

# Install binary
install -m 0755 /opt/music-file-playlist-online-sync-YYYYMMDDHHMMSS/music-file-playlist-online-sync /usr/local/bin/

# Copy example config
cp /opt/music-file-playlist-online-sync-YYYYMMDDHHMMSS/example-config.toml /etc/music-sync/config.toml

# Copy systemd units and enable
cp /opt/music-file-playlist-online-sync-YYYYMMDDHHMMSS/systemd/* /etc/systemd/system/
systemctl daemon-reload
systemctl enable music-file-playlist-online-sync-worker.service
systemctl start music-file-playlist-online-sync-worker.service
```

Configuration

Edit `/etc/music-sync/config.toml` to set `root_folder`, `db_path`, and credentials. See `config/example-config.toml` for options.

Security

- Configure file permissions for `/etc/music-sync` and the DB path to restrict access to the service user.
- Do not store plaintext secrets in world-readable locations.
