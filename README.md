# music-file-playlist-online-sync

Updates in this batch
- Spotify provider now allows overriding auth and API base URLs via environment variables:
  - SPOTIFY_AUTH_BASE (default https://accounts.spotify.com)
  - SPOTIFY_API_BASE (default https://api.spotify.com/v1)
  This makes it possible for tests to point providers at mock servers (e.g., mockito).
- Added a Tidal provider (best-effort) and a Tidal auth helper that accepts pasted token JSON.
- Tests: added provider tests using mockito to simulate Spotify token refresh and playlist flow.
- CI: GitHub Actions workflow (ci.yml) to build, test, and upload a release binary artifact.

Running provider tests locally
1. Install Rust toolchain (rustup)
2. Run:
   cargo test --test provider_spotify_mock_tests

   The test uses mockito and the provider's env overrides to point API/auth calls at the mock server. It creates a temporary DB and stores an expired token to exercise token refresh logic.

# music-file-playlist-online-sync

Quick start for developers (Sprint 0)

Prerequisites

Build

1. Build debug binary:

```sh
cargo build
```

2. Run tests:

```sh
cargo test
```

Dev helper (fast bootstrap)

Use the included helper to create a temporary workspace, initialize a test SQLite DB from `db/schema.sql`, and print commands to run the watcher and worker.

Make the script executable and run it:

```sh
chmod +x scripts/dev.sh
scripts/dev.sh
```

The script will output paths for the generated config and DB and example `cargo run` commands such as:

```sh
# Run watcher (background)
cargo run -- --config /tmp/music-sync-dev/config.toml Watcher &

# Run worker once (one-shot)
cargo run -- --config /tmp/music-sync-dev/config.toml Worker
```

Example config

The repo contains `config/example-config.toml` — the dev script will copy and adapt it to a temporary location for local testing (root folder, DB path, and log dir set to the temp workspace).

Spotify & Tidal quick guide

Spotify

```sh
# example (replace values)
music-file-playlist-online-sync auth spotify
```

Tidal

Issue board skeleton


Developer goals for Sprint 0


If anything fails during setup, open an issue or paste the failing command and output and I'll help debug.

**Packaging & Installation**

Use the packaging script to build a release binary and create a tarball with example config and systemd units:

```sh
chmod +x scripts/package.sh
scripts/package.sh
```

See `docs/INSTALL.md` for example installation steps on a target machine.
