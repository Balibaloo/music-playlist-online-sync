use music_file_playlist_online_sync::db;
use rusqlite::Connection;
use tempfile::tempdir;

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
    let (isrc, rid) = cached.unwrap();
    assert_eq!(isrc.unwrap(), "ISRC123");
    assert_eq!(rid.unwrap(), "rid-trk-1");
}
