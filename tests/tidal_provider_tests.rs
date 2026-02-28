use mockito::{Server, Matcher};
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

#[test]
fn tidal_add_tracks_filters_invalid_ids() {
    let mut server = Server::new();
    let base = server.url();
    env::set_var("TIDAL_API_BASE", &base);
    env::set_var("TIDAL_AUTH_BASE", &base);

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

    // case A: mixed valid/invalid URIs. expect exactly one POST with the valid id
    let mut m_add = server
        .mock("POST", "/playlists/playlist1/relationships/items?countryCode=US")
        .match_body(Matcher::Regex("\\\"id\\\":\\\"123\\\"".into()))
        .with_status(200)
        .expect(1)
        .create();

    let uris = vec![
        "tidal:track:123".into(),
        "tidal:track:0".into(),
        "tidal:track:-5".into(),
        "tidal:track:foo".into(),
        "tidal:track:".into(),
    ];

    let res = rt.block_on(provider.add_tracks("playlist1", &uris));
    assert!(res.is_ok(), "add_tracks should succeed even with invalid ids");
    m_add.assert();

    // case B: entirely invalid list should not send any requests
    let mut m_none = server
        .mock("POST", "/playlists/playlist1/relationships/items?countryCode=US")
        .with_status(200)
        .expect(0)
        .create();
    let uris2 = vec![
        "tidal:track:0".into(),
        "tidal:track:-1".into(),
        "tidal:track:foo".into(),
        "tidal:track:".into(),
    ];
    let res2 = rt.block_on(provider.add_tracks("playlist1", &uris2));
    assert!(res2.is_ok(), "add_tracks should succeed with no valid ids");
    m_none.assert();
}

#[test]
fn tidal_search_helpers_ignore_zero_ids() {
    let mut server = Server::new();
    let base = server.url();
    env::set_var("TIDAL_API_BASE", &base);
    env::set_var("TIDAL_AUTH_BASE", &base);

    // no real token required for search; just a dummy value in DB
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

    // Mock search /search/tracks returning id=0
    let body = json!({
        "items": [{"id": "0"}]
    })
    .to_string();
    let _m = server
        .mock("GET", "/search/tracks?query=test%20&limit=1&countryCode=US")
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(body)
        .create();

    let res = rt.block_on(provider.search_track_uri("test", ""));
    assert!(res.unwrap().is_none(), "should ignore zero id from search");

    // now test isrc variant with id 0
    let body2 = json!({"data": [{"id": 0}] }).to_string();
    let _m2 = server
        .mock("GET", "/tracks?countryCode=US&filter%5Bisrc%5D=XYZ")
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(body2)
        .create();
    let res2 = rt.block_on(provider.search_track_uri_by_isrc("XYZ"));
    assert!(res2.unwrap().is_none(), "isrc search should ignore zero id");
}
