use music_file_playlist_online_sync::collapse::collapse_events;
use music_file_playlist_online_sync::db;
use music_file_playlist_online_sync::models::{Event, EventAction};
use rusqlite::Connection;
use tempfile::tempdir;

#[test]
fn test_try_acquire_release_lock() {
    let td = tempdir().unwrap();
    let db_path = td.path().join("test.db");
    let mut conn = Connection::open(&db_path).unwrap();
    db::run_migrations(&conn).unwrap();

    let got = db::try_acquire_playlist_lock(&mut conn, "pl1", "worker1", 10).unwrap();
    assert!(got, "should acquire initial lock");

    // second attempt should fail
    let got2 = db::try_acquire_playlist_lock(&mut conn, "pl1", "worker2", 10).unwrap();
    assert!(!got2, "second worker should not acquire lock");

    // release with wrong worker -> no effect
    db::release_playlist_lock(&mut conn, "pl1", "wrong").unwrap();
    // still not acquirable by worker2
    let got3 = db::try_acquire_playlist_lock(&mut conn, "pl1", "worker2", 10).unwrap();
    assert!(!got3, "lock still held by worker1");

    // release by owner
    db::release_playlist_lock(&mut conn, "pl1", "worker1").unwrap();
    let got4 = db::try_acquire_playlist_lock(&mut conn, "pl1", "worker2", 10).unwrap();
    assert!(got4, "should acquire after release");
}

#[test]
fn collapse_add_remove_pair() {
    let a = Event {
        id: 1,
        timestamp_ms: 1,
        playlist_name: "p".into(),
        action: EventAction::Add,
        track_path: Some("t.mp3".into()),
        extra: None,
        is_synced: false,
    };
    let r = Event {
        id: 2,
        timestamp_ms: 2,
        playlist_name: "p".into(),
        action: EventAction::Remove,
        track_path: Some("t.mp3".into()),
        extra: None,
        is_synced: false,
    };
    let res = collapse_events(&[a, r]);
    // After collapse, no add/remove for t.mp3
    assert!(res.iter().all(|e| e.track_path.as_deref() != Some("t.mp3")));
}
