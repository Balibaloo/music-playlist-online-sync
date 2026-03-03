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
systemctl enable --now music-file-playlist-online-sync-watcher.service
systemctl enable --now music-file-playlist-online-sync-reconcile.timer
```

Configuration

Edit `/etc/music-sync/config.toml` to set `root_folder`, `db_path`, and credentials.

**Filesystem permissions requirement**

The systemd services run as the `music-file-playlist-online-sync` user. That user must be able to **read and write** under your
configured `root_folder` in order to create/update local `.m3u` playlist files. A simple way to grant this without changing
ownership away from your normal user is to use ACLs, for example:

```sh
sudo setfacl -R -m u:music-file-playlist-online-sync:rwx /path/to/your/root_folder
sudo setfacl -R -d -m u:music-file-playlist-online-sync:rwx /path/to/your/root_folder
```

See `config/example-config.toml` for all options, including:
- `local_playlist_template` for on-disk `.m3u` filenames
- `online_root_playlist`, `online_playlist_structure`, and `online_folder_flattening_delimiter` for how online playlists are grouped
- `remote_playlist_template_flat` / `remote_playlist_template_folders` (and the legacy `remote_playlist_template`) for customizing remote playlist display names, supporting `${folder_name}`, `${path_to_parent}`, and the legacy `${relative_path}` alias (expanded as `path_to_parent + folder_name`).
- `watcher_instant_trigger_threshold` (default `20`) — batches at or below this count spawn the worker immediately after debounce.
- `watcher_deferred_trigger_delay_sec` (default `300`) — batches above the threshold arm a deferred timer; the deadline resets on each new burst so overlapping imports coalesce into one run.

**Reconcile runs the worker**

The `Reconcile` command (fired by `reconcile.timer`) now runs the worker immediately after enqueueing events, completing the full scan → sync cycle in one shot without waiting for a separate timer.

**Watcher triggers vs. systemd timers**

With the above two changes the event pipeline is fully self-contained:
- `watcher.service` — real-time file changes, instant or deferred worker spawn.
- `reconcile.timer` — full nightly scan + immediate worker run (drift correction).

Security

- Configure file permissions for `/etc/music-sync` and the DB path to restrict access to the service user.
- Do not store plaintext secrets in world-readable locations.
