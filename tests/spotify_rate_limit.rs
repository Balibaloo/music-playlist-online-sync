use mockito::Server;
use std::env;
use tempfile::tempdir;
use rusqlite::Connection;
use serde_json::json;
use music_file_playlist_online_sync::api::spotify::SpotifyProvider;
use music_file_playlist_online_sync::api::Provider;
use music_file_playlist_online_sync::db;

#[test]
fn spotify_add_tracks_rate_limited_returns_rate_limited_error() {
    let mut server = Server::new();
    let base = server.url();
    env::set_var("SPOTIFY_API_BASE", &base);
    env::set_var("SPOTIFY_AUTH_BASE", &base);

    // mock playlist add endpoint to return 429 with retry-after
    let _m_add = server.mock("POST", "/playlists/mock_playlist_id/tracks")
        .with_status(429)
        .with_header("retry-after", "3")
        .with_header("content-type", "application/json")
        .with_body(r#"{"error":"rate_limited"}"#)
        .create();

    // prepare DB with a valid (non-expired) token so get_bearer works
    let td = tempdir().unwrap();
    let db_path = td.path().join("test.db");
    let conn = Connection::open(&db_path).unwrap();
    db::run_migrations(&conn).unwrap();
    let now = chrono::Utc::now().timestamp();
    let stored = json!({
        "access_token": "valid",
        "token_type": "Bearer",
        "expires_at": now + 3600,
        "refresh_token": null,
        "scope": ""
    }).to_string();
    db::save_credential_raw(&conn, "spotify", &stored).unwrap();

    let provider = SpotifyProvider::new("cid".into(), "csecret".into(), db_path.clone());
    let mut rt = tokio::runtime::Runtime::new().unwrap();
    let res = rt.block_on(async move {
        provider.add_tracks("mock_playlist_id", &vec!["spotify:track:1".to_string()]).await
    });

    assert!(res.is_err());
    let s = format!("{}", res.err().unwrap());
    assert!(s.contains("rate_limited") || s.contains("retry_after"));
}
