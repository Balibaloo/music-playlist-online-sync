use mockito::Server;
use music_file_playlist_online_sync::api::spotify::SpotifyProvider;
use music_file_playlist_online_sync::api::Provider;
use music_file_playlist_online_sync::db;
use rusqlite::Connection;
use serde_json::json;
use std::env;
use std::sync::{Mutex, OnceLock};
use tempfile::tempdir;
use tokio;

fn test_env_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

#[test]
fn spotify_token_refresh_and_playlist_flow() {
    let _guard = test_env_lock().lock().unwrap();
    // Create mock server outside of any tokio runtime
    let mut server = Server::new();
    let mock_url = server.url();
    env::set_var("SPOTIFY_AUTH_BASE", &mock_url);
    env::set_var("SPOTIFY_API_BASE", &mock_url);

    // Run the async test body on a fresh runtime
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async move {
        // Mock token refresh endpoint (accounts.../api/token)
        let _m_token = server
            .mock("POST", "/api/token")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                json!({
                    "access_token": "new_access_token",
                    "token_type": "Bearer",
                    "expires_in": 3600,
                    "scope": "playlist-modify-private playlist-modify-public",
                })
                .to_string(),
            )
            .create();

        // Mock /me endpoint
        let _m_me = server
            .mock("GET", "/me")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(json!({ "id": "mock_user" }).to_string())
            .create();

        // Mock list playlists endpoint used by ensure_playlist before create.
        let _m_list = server
            .mock("GET", "/users/mock_user/playlists")
            .match_query(mockito::Matcher::UrlEncoded("limit".into(), "50".into()))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(json!({ "items": [], "next": null }).to_string())
            .create();

        // Mock create playlist endpoint
        let _m_create = server
            .mock("POST", "/users/mock_user/playlists")
            .with_status(201)
            .with_header("content-type", "application/json")
            .with_body(json!({ "id": "mock_playlist_id" }).to_string())
            .create();

        // Mock add tracks endpoint
        let _m_add = server
            .mock("POST", "/playlists/mock_playlist_id/tracks")
            .with_status(201)
            .with_header("content-type", "application/json")
            .with_body(json!({ "snapshot_id": "s1" }).to_string())
            .create();

        // Prepare a temporary DB and insert an expired token that will trigger a refresh
        let td = tempdir().unwrap();
        let db_path = td.path().join("test.db");
        let conn = Connection::open(&db_path).unwrap();
        db::run_migrations(&conn).unwrap();

        // create a StoredToken-like JSON with expired access token
        let now = chrono::Utc::now().timestamp();
        let stored = json!({
            "access_token": "old_token",
            "token_type": "Bearer",
            "expires_at": now - 1000, // expired
            "refresh_token": "refresh_token_value",
            "scope": "playlist-modify-private"
        })
        .to_string();
        db::save_credential_raw(&conn, "spotify", &stored, None, None).unwrap();

        // Instantiate provider with dummy client id/secret and point to the temp db
        let provider = SpotifyProvider::new(
            "cid".into(),
            "csecret".into(),
            db_path.clone(),
            Default::default(),
        );

        // Call ensure_playlist which will call /me, refresh token, and create playlist
        let res = provider.ensure_playlist("Test Playlist", "desc").await;
        assert!(res.is_ok());
        assert_eq!(res.unwrap(), "mock_playlist_id");
    });
}

#[test]
fn spotify_ensure_playlist_reuses_existing_exact_name() {
    let _guard = test_env_lock().lock().unwrap();
    let mut server = Server::new();
    let mock_url = server.url();
    env::set_var("SPOTIFY_AUTH_BASE", &mock_url);
    env::set_var("SPOTIFY_API_BASE", &mock_url);

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async move {
        let _m_token = server
            .mock("POST", "/api/token")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                json!({
                    "access_token": "new_access_token",
                    "token_type": "Bearer",
                    "expires_in": 3600,
                    "scope": "playlist-modify-private playlist-modify-public",
                })
                .to_string(),
            )
            .create();

        let _m_me = server
            .mock("GET", "/me")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(json!({ "id": "mock_user" }).to_string())
            .create();

        let _m_list = server
            .mock("GET", "/users/mock_user/playlists")
            .match_query(mockito::Matcher::UrlEncoded("limit".into(), "50".into()))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                json!({
                    "items": [
                        { "id": "existing_playlist_id", "name": "Test Playlist" }
                    ],
                    "next": null
                })
                .to_string(),
            )
            .expect(1)
            .create();

        let td = tempdir().unwrap();
        let db_path = td.path().join("test.db");
        let conn = Connection::open(&db_path).unwrap();
        db::run_migrations(&conn).unwrap();

        let now = chrono::Utc::now().timestamp();
        let stored = json!({
            "access_token": "old_token",
            "token_type": "Bearer",
            "expires_at": now - 1000,
            "refresh_token": "refresh_token_value",
            "scope": "playlist-modify-private"
        })
        .to_string();
        db::save_credential_raw(&conn, "spotify", &stored, None, None).unwrap();

        let provider = SpotifyProvider::new(
            "cid".into(),
            "csecret".into(),
            db_path.clone(),
            Default::default(),
        );

        let res = provider.ensure_playlist("Test Playlist", "desc").await;
        assert!(res.is_ok());
        assert_eq!(res.unwrap(), "existing_playlist_id");
    });
}
