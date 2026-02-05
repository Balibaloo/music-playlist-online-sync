use mockito::Server;
use std::env;
use tempfile::tempdir;
use rusqlite::Connection;
use serde_json::json;
use music_file_playlist_online_sync::api::spotify::SpotifyProvider;
use music_file_playlist_online_sync::api::Provider;
use music_file_playlist_online_sync::db;

#[test]
fn spotify_refresh_failure_returns_error() {
    // mock server returns 500 for token refresh and 401 for /me to trigger refresh
    let mut server = Server::new();
    let base = server.url();
    env::set_var("SPOTIFY_AUTH_BASE", &base);
    env::set_var("SPOTIFY_API_BASE", &base);

    let _m_me = server.mock("GET", "/me")
        .with_status(401)
        .with_header("content-type", "application/json")
        .with_body(r#"{"error":"unauthorized"}"#)
        .create();

    let _m_token = server.mock("POST", "/api/token")
        .with_status(500)
        .with_header("content-type", "application/json")
        .with_body(r#"{"error":"server"}"#)
        .create();

    // prepare DB with expired token to force refresh path
    let td = tempdir().unwrap();
    let db_path = td.path().join("test.db");
    let conn = Connection::open(&db_path).unwrap();
    db::run_migrations(&conn).unwrap();

    // stored token with expired access and a refresh_token
    let now = chrono::Utc::now().timestamp();
    let stored = json!({
        "access_token": "old",
        "token_type": "Bearer",
        "expires_at": now - 1000,
        "refresh_token": "refresh",
        "scope": ""
    }).to_string();
    db::save_credential_raw(&conn, "spotify", &stored).unwrap();

    let provider = SpotifyProvider::new("cid".into(), "csecret".into(), db_path.clone());
    let mut rt = tokio::runtime::Runtime::new().unwrap();
    let res = rt.block_on(async move {
        provider.ensure_playlist("X", "").await
    });

    assert!(res.is_err(), "expected error when token refresh fails via ensure_playlist");
}
