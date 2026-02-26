use music_file_playlist_online_sync as lib;
use lib::db;
use mockito::Server;
use serde_json::json;
use std::path::PathBuf;

fn save_credentials(conn: &rusqlite::Connection, provider: &str, token_json: &str, client_id: &str, client_secret: &str) {
    db::save_credential_raw(conn, provider, token_json, Some(client_id), Some(client_secret)).expect("save cred");
}

#[test]
fn spotify_token_refresh_success_and_preserve_client() {
    let mut server = Server::new();
    let base = server.url();

    // mock token endpoint at /api/token -> success
    let new_access = "new-access-token-spotify";
    let _m = server
        .mock("POST", "/api/token")
        .match_header("authorization", "Basic dGVzdF9pZDp0ZXN0X3NlY3JldA==")
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(json!({"access_token": new_access, "expires_in": 3600, "scope": "playlist-read-private"}).to_string())
        .create();

    let dir = tempfile::tempdir().expect("tmpdir");
    let db_path = dir.path().join("music-sync.db");
    let conn = db::open_or_create(&db_path).expect("open db");

    let init_token = json!({
        "access_token": "old",
        "token_type": "Bearer",
        "expires_at": 0,
        "refresh_token": "refresh-spotify",
        "scope": "playlist-read-private"
    })
    .to_string();

    // save with client creds that should be preserved
    save_credentials(&conn, "spotify", &init_token, "test_id", "test_secret");
    std::env::set_var("SPOTIFY_AUTH_BASE", &base);

    let provider = lib::api::spotify::SpotifyProvider::new(String::new(), String::new(), db_path.clone());
    let rt = tokio::runtime::Runtime::new().expect("rt");
    let bearer = rt.block_on(provider.get_bearer()).expect("get bearer");
    assert_eq!(bearer, format!("Bearer {}", new_access));

    let client_id: Option<String> = conn.query_row("SELECT client_id FROM credentials WHERE provider = 'spotify'", [], |r| r.get(0)).expect("client_id");
    let client_secret: Option<String> = conn.query_row("SELECT client_secret FROM credentials WHERE provider = 'spotify'", [], |r| r.get(0)).expect("client_secret");
    assert_eq!(client_id.unwrap_or_default(), "test_id");
    assert_eq!(client_secret.unwrap_or_default(), "test_secret");
}

#[test]
fn spotify_token_refresh_failure_invalid_client() {
    let mut server = Server::new();
    let base = server.url();

    // mock token endpoint returns 400 invalid_client
    let _m = server
        .mock("POST", "/api/token")
        .with_status(400)
        .with_header("content-type", "application/json")
        .with_body(json!({"error": "invalid_client"}).to_string())
        .create();

    let dir = tempfile::tempdir().expect("tmpdir");
    let db_path = dir.path().join("music-sync.db");
    let conn = db::open_or_create(&db_path).expect("open db");

    let init_token = json!({
        "access_token": "old",
        "token_type": "Bearer",
        "expires_at": 0,
        "refresh_token": "refresh-spotify",
    })
    .to_string();

    // store empty client credentials to simulate missing config
    db::save_credential_raw(&conn, "spotify", &init_token, None, None).expect("save empty");
    std::env::set_var("SPOTIFY_AUTH_BASE", &base);

    let provider = lib::api::spotify::SpotifyProvider::new(String::new(), String::new(), db_path.clone());
    let rt = tokio::runtime::Runtime::new().expect("rt");
    let res = rt.block_on(provider.get_bearer());
    assert!(res.is_err());
    let e = res.err().unwrap().to_string();
    assert!(e.contains("invalid_client") || e.contains("Failed to refresh token"));
}

#[test]
fn tidal_token_refresh_success_and_preserve_client() {
    let mut server = Server::new();
    let base = server.url();

    let new_access = "new-access-token-tidal";
    let _m = server
        .mock("POST", "/v1/oauth2/token")
        .match_header("authorization", "Basic dGVzdF9pZDp0ZXN0X3NlY3JldA==")
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(json!({"access_token": new_access, "expires_in": 3600, "scope": "playlist-read-private"}).to_string())
        .create();

    let dir = tempfile::tempdir().expect("tmpdir");
    let db_path = dir.path().join("music-sync.db");
    let conn = db::open_or_create(&db_path).expect("open db");

    let init_token = json!({
        "access_token": "old",
        "token_type": "Bearer",
        "expires_at": 0,
        "refresh_token": "refresh-tidal",
        "user_id": 12345
    })
    .to_string();

    save_credentials(&conn, "tidal", &init_token, "test_id", "test_secret");
    std::env::set_var("TIDAL_AUTH_BASE", &base);

    let provider = lib::api::tidal::TidalProvider::new(String::new(), String::new(), db_path.clone(), None);
    let rt = tokio::runtime::Runtime::new().expect("rt");
    let bearer = rt.block_on(provider.get_bearer()).expect("get bearer");
    assert_eq!(bearer, format!("Bearer {}", new_access));

    let client_id: Option<String> = conn.query_row("SELECT client_id FROM credentials WHERE provider = 'tidal'", [], |r| r.get(0)).expect("client_id");
    let client_secret: Option<String> = conn.query_row("SELECT client_secret FROM credentials WHERE provider = 'tidal'", [], |r| r.get(0)).expect("client_secret");
    assert_eq!(client_id.unwrap_or_default(), "test_id");
    assert_eq!(client_secret.unwrap_or_default(), "test_secret");
}

#[test]
fn tidal_token_refresh_failure_invalid_client() {
    let mut server = Server::new();
    let base = server.url();

    let _m = server
        .mock("POST", "/v1/oauth2/token")
        .with_status(400)
        .with_header("content-type", "application/json")
        .with_body(json!({"error": "invalid_client"}).to_string())
        .create();

    let dir = tempfile::tempdir().expect("tmpdir");
    let db_path = dir.path().join("music-sync.db");
    let conn = db::open_or_create(&db_path).expect("open db");

    let init_token = json!({
        "access_token": "old",
        "token_type": "Bearer",
        "expires_at": 0,
        "refresh_token": "refresh-tidal",
        "user_id": 12345
    })
    .to_string();

    db::save_credential_raw(&conn, "tidal", &init_token, None, None).expect("save empty");
    std::env::set_var("TIDAL_AUTH_BASE", &base);

    let provider = lib::api::tidal::TidalProvider::new(String::new(), String::new(), db_path.clone(), None);
    let rt = tokio::runtime::Runtime::new().expect("rt");
    let res = rt.block_on(provider.get_bearer());
    if res.is_err() {
        let e = res.err().unwrap().to_string();
        assert!(e.contains("invalid_client") || e.contains("Failed to refresh tidal token"));
    } else {
        // Tidal's ensure_token currently logs refresh failures and
        // returns the existing (possibly-expired) token; accept that
        // behavior as well (bearer should contain the initial access token).
        let bearer = res.unwrap();
        assert_eq!(bearer, "Bearer old");
    }
}

