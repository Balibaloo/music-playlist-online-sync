use music_file_playlist_online_sync::db;
use rusqlite::Connection;
use tempfile::tempdir;
use chrono::Utc;

#[test]
fn playlist_map_and_track_cache_persistence() {
    let td = tempdir().unwrap();
    let db_path = td.path().join("test.db");
    let conn = Connection::open(&db_path).unwrap();
    db::run_migrations(&conn).unwrap();

    // test playlist_map upsert (scoped by provider; use spotify for compatibility)
    db::upsert_playlist_map(&conn, "spotify", "mypl", "rid123").unwrap();
    let got = db::get_remote_playlist_id(&conn, "spotify", "mypl").unwrap();
    assert_eq!(got, Some("rid123".into()));

    // test track cache upsert + lookup (scoped by provider; use spotify)
    db::upsert_track_cache(
        &conn,
        "spotify",
        "/music/a/song.mp3",
        Some("ISRC123"),
        Some("rid-trk-1"),
    )
    .unwrap();
    let cached = db::get_track_cache_by_local(&conn, "spotify", "/music/a/song.mp3").unwrap();
    assert!(cached.is_some());
    let (isrc, rid, resolved_at) = cached.unwrap();
    assert_eq!(isrc.unwrap(), "ISRC123");
    assert_eq!(rid.unwrap(), "rid-trk-1");
    // resolved_at should be recent (within a minute)
    assert!((Utc::now().timestamp() - resolved_at).abs() < 60);

    // negative lookup: insert entry with no remote_id and verify it persists with timestamp
    db::upsert_track_cache(&conn, "spotify", "/music/a/missing.mp3", None, None).unwrap();
    let neg = db::get_track_cache_by_local(&conn, "spotify", "/music/a/missing.mp3").unwrap();
    assert!(neg.is_some());
    let (_isrc2, rid2, ts2) = neg.unwrap();
    assert!(rid2.is_none());
    assert!((Utc::now().timestamp() - ts2).abs() < 60);
}
