use std::fs::File;
use std::io::Write;
use tempfile::tempdir;

use music_file_playlist_online_sync::config::Config;
use music_file_playlist_online_sync::db;

#[test]
fn config_from_path_parses_toml() {
    let td = tempdir().unwrap();
    let cfg_path = td.path().join("cfg.toml");
    let mut f = File::create(&cfg_path).unwrap();
    let toml = r#"
root_folder = "/tmp/music"
db_path = "/tmp/test.db"
log_dir = "/tmp"
"#;
    f.write_all(toml.as_bytes()).unwrap();
    let cfg = Config::from_path(&cfg_path).expect("parse config");
    assert_eq!(cfg.root_folder.to_str().unwrap(), "/tmp/music");
    assert_eq!(cfg.db_path.to_str().unwrap(), "/tmp/test.db");
}

#[test]
fn run_migrations_creates_tables() {
    let td = tempdir().unwrap();
    let db_path = td.path().join("test.db");
    let conn = rusqlite::Connection::open(&db_path).unwrap();
    // run migrations should execute schema.sql
    db::run_migrations(&conn).expect("run migrations");
    // verify that event_queue table exists
    let mut stmt = conn
        .prepare("SELECT name FROM sqlite_master WHERE type='table' AND name='event_queue'")
        .unwrap();
    let mut rows = stmt.query([]).unwrap();
    let found = rows.next().unwrap().is_some();
    assert!(found, "event_queue table should exist after migrations");
}
