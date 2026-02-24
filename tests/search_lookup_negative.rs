use mockito::Server;
use music_file_playlist_online_sync::api::spotify::SpotifyProvider;
use music_file_playlist_online_sync::api::Provider;
use music_file_playlist_online_sync::db;
use rusqlite::Connection;
use serde_json::json;
use std::env;
use tempfile::tempdir;

#[test]
fn spotify_search_returns_none_on_error() {
    let mut server = Server::new();
    let base = server.url();
    env::set_var("SPOTIFY_API_BASE", &base);
    env::set_var("SPOTIFY_AUTH_BASE", &base);

    // mock search endpoint to return 500
    let _m_search = server
        .mock("GET", "/search")
        .with_status(500)
        .with_header("content-type", "application/json")
        .with_body(r#"{"error":"server"}"#)
        .create();

    // prepare DB with a valid token so get_bearer works
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
    })
    .to_string();
    db::save_credential_raw(&conn, "spotify", &stored, None, None).unwrap();

    let provider = SpotifyProvider::new("cid".into(), "csecret".into(), db_path.clone());
    let rt = tokio::runtime::Runtime::new().unwrap();
    let res = rt.block_on(async move { provider.search_track_uri("Title", "Artist").await });
    assert!(res.is_ok());
    assert!(res.unwrap().is_none());
}
