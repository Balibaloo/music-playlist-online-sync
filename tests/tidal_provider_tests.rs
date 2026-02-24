use mockito::Server;
use music_file_playlist_online_sync::api::tidal::TidalProvider;
use music_file_playlist_online_sync::api::Provider;
use music_file_playlist_online_sync::db;
use rusqlite::Connection;
use serde_json::json;
use std::env;
use tempfile::tempdir;

#[test]
fn tidal_ensure_playlist_happy_path() {
    let mut server = Server::new();
    let base = server.url();
    env::set_var("TIDAL_API_BASE", &base);
    env::set_var("TIDAL_AUTH_BASE", &base);

    // TidalProvider::ensure_playlist calls POST /playlists?countryCode=US, so
    // the mock must include the query string or mockito will return 501.
    let _m_create = server
        .mock("POST", "/playlists?countryCode=US")
        .with_status(201)
        .with_header("content-type", "application/json")
        .with_body(json!({ "id": "tidal_pl_1" }).to_string())
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
    db::save_credential_raw(&conn, "tidal", &stored, None, None).unwrap();

    let provider = TidalProvider::new("cid".into(), "csecret".into(), db_path.clone(), None);
    let rt = tokio::runtime::Runtime::new().unwrap();
    let res = rt.block_on(async move { provider.ensure_playlist("Test", "").await });
    if res.is_err() {
        println!("tidal ensure_playlist error: {:?}", res);
    }
    assert!(res.is_ok());
    assert_eq!(res.unwrap(), "tidal_pl_1");
}
