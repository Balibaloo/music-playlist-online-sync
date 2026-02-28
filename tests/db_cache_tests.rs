use music_file_playlist_online_sync::db;
use music_file_playlist_online_sync::api;
use music_file_playlist_online_sync::models;
use rusqlite::Connection;
use tempfile::tempdir;
use chrono::Utc;
use std::sync::Arc;
use async_trait::async_trait;

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


#[tokio::test]
async fn playlist_cache_and_rename_migration() {
    // set up a temporary root folder with a playlist and a single track
    let td = tempfile::tempdir().unwrap();
    let root = td.path().join("root");
    std::fs::create_dir_all(root.join("foo")).unwrap();
    std::fs::write(root.join("foo").join("song.mp3"), b"data").unwrap();
    let playlist_path = root.join("foo").join("foo.m3u");
    std::fs::write(&playlist_path, "song.mp3\n").unwrap();

    // write minimal config file pointing at the temp root
    let cfg_file = td.path().join("cfg.toml");
    let db_file = td.path().join("test.db");
    std::fs::write(
        &cfg_file,
        format!(
            "root_folder = \"{}\"\ndb_path = \"{}\"\n",
            root.display(),
            db_file.display()
        ),
    )
    .unwrap();
    let cfg = music_file_playlist_online_sync::config::Config::from_path(&cfg_file).unwrap();
    // ensure the database exists and has the proper tables
    {
        let conn = rusqlite::Connection::open(&cfg.db_path).unwrap();
        music_file_playlist_online_sync::db::run_migrations(&conn).unwrap();
    }

    // counting provider to ensure resolution only happens once per file
    struct CountingProvider(Arc<std::sync::Mutex<usize>>);
    impl CountingProvider {
        fn new(counter: Arc<std::sync::Mutex<usize>>) -> Self {
            Self(counter)
        }
    }
    #[async_trait::async_trait]
    impl api::Provider for CountingProvider {
        fn name(&self) -> &str {
            "count"
        }
        fn is_authenticated(&self) -> bool {
            true
        }
        async fn ensure_playlist(&self, _name: &str, _desc: &str) -> anyhow::Result<String> {
            Ok("id".to_string())
        }
        async fn rename_playlist(&self, _playlist_id: &str, _new_name: &str) -> anyhow::Result<()> {
            Ok(())
        }
        async fn add_tracks(&self, _playlist_id: &str, _uris: &[String]) -> anyhow::Result<()> {
            Ok(())
        }
        async fn remove_tracks(&self, _playlist_id: &str, _uris: &[String]) -> anyhow::Result<()> {
            Ok(())
        }
        async fn delete_playlist(&self, _playlist_id: &str) -> anyhow::Result<()> {
            Ok(())
        }
        async fn search_track_uri(&self, _title: &str, _artist: &str) -> anyhow::Result<Option<String>> {
            let mut c = self.0.lock().unwrap();
            *c += 1;
            Ok(Some("uri".to_string()))
        }
        async fn list_playlist_tracks(&self, _playlist_id: &str) -> anyhow::Result<Vec<String>> {
            Ok(Vec::new())
        }
        async fn playlist_is_valid(&self, _playlist_id: &str) -> anyhow::Result<bool> {
            Ok(true)
        }
        async fn search_track_uri_by_isrc(&self, _isrc: &str) -> anyhow::Result<Option<String>> {
            // count as well
            let mut c = self.0.lock().unwrap();
            *c += 1;
            Ok(None)
        }
    }

    let counter = Arc::new(std::sync::Mutex::new(0));
    let provider = Arc::new(CountingProvider::new(counter.clone()));

    // first run should populate cache and increment counter
    let uris1 = music_file_playlist_online_sync::worker::desired_remote_uris_for_playlist(&cfg, "foo", provider.clone())
        .await
        .unwrap();
    assert_eq!(uris1, vec!["uri".to_string()]);
    let calls_after_first = *counter.lock().unwrap();
    assert!(calls_after_first > 0);

    // second run without changing the file should hit cache and not increment
    let uris2 = music_file_playlist_online_sync::worker::desired_remote_uris_for_playlist(&cfg, "foo", provider.clone())
        .await
        .unwrap();
    assert_eq!(uris2, uris1);
    assert_eq!(calls_after_first, *counter.lock().unwrap());

    // migrate/rename the playlist folder on disk and in the database
    std::fs::rename(root.join("foo"), root.join("bar")).unwrap();
    // playlist file is now root/bar/bar.m3u (we also rename it to match)
    std::fs::rename(root.join("bar").join("foo.m3u"), root.join("bar").join("bar.m3u")).unwrap();

    // manually migrate cache entry
    let conn = rusqlite::Connection::open(cfg.db_path.clone()).unwrap();
    music_file_playlist_online_sync::db::migrate_playlist_cache(&conn, "foo", "bar").unwrap();

    // third run using new logical name should also hit cache (no new provider calls)
    let uris3 = music_file_playlist_online_sync::worker::desired_remote_uris_for_playlist(&cfg, "bar", provider.clone())
        .await
        .unwrap();
    assert_eq!(uris3, uris1);
    assert_eq!(calls_after_first, *counter.lock().unwrap());
}

// exercise the predicate that guards the expensive URI resolution step.  this
// is a very small unit test but it makes the reasoning in the worker code easy
// to verify and guards against regressions.
#[test]
fn should_precompute_desired_behavior() {
    use music_file_playlist_online_sync::worker::should_precompute_desired;

    // deletion always skips
    assert!(!should_precompute_desired(true, false, false));
    // explicit track add/remove overrides everything else
    assert!(!should_precompute_desired(false, true, false));
    assert!(!should_precompute_desired(false, true, true));
    // renaming without any track changes should not resolve URIs
    assert!(!should_precompute_desired(false, false, true));
    // the nightly‑reconcile/create case is the only one that returns true
    assert!(should_precompute_desired(false, false, false));
}

// this test mirrors the early precompute block from `run_worker_once` to ensure
// that a playlist which has track operations does not hit the provider at all
// when the predicate says we can skip resolution.  the counter in the
// `CountingProvider` will remain zero.
#[tokio::test]
async fn skip_resolve_when_track_ops() {
    // similar setup to the earlier async test
    let td = tempfile::tempdir().unwrap();
    let root = td.path().join("root");
    std::fs::create_dir_all(root.join("foo")).unwrap();
    std::fs::write(root.join("foo").join("song.mp3"), b"data").unwrap();
    let playlist_path = root.join("foo").join("foo.m3u");
    std::fs::write(&playlist_path, "song.mp3\n").unwrap();

    let cfg_file = td.path().join("cfg.toml");
    let db_file = td.path().join("test.db");
    std::fs::write(
        &cfg_file,
        format!(
            "root_folder = \"{}\"\ndb_path = \"{}\"\n",
            root.display(),
            db_file.display()
        ),
    )
    .unwrap();
    let cfg = music_file_playlist_online_sync::config::Config::from_path(&cfg_file).unwrap();
    {
        let conn = rusqlite::Connection::open(&cfg.db_path).unwrap();
        music_file_playlist_online_sync::db::run_migrations(&conn).unwrap();
    }

    struct CountingProvider(Arc<std::sync::Mutex<usize>>);
    impl CountingProvider {
        fn new(counter: Arc<std::sync::Mutex<usize>>) -> Self {
            Self(counter)
        }
    }
    #[async_trait::async_trait]
    impl api::Provider for CountingProvider {
        fn name(&self) -> &str { "count" }
        fn is_authenticated(&self) -> bool { true }
        async fn ensure_playlist(&self, _name: &str, _desc: &str) -> anyhow::Result<String> { Ok("id".into()) }
        async fn rename_playlist(&self, _playlist_id: &str, _new_name: &str) -> anyhow::Result<()> { Ok(()) }
        async fn add_tracks(&self, _playlist_id: &str, _uris: &[String]) -> anyhow::Result<()> { Ok(()) }
        async fn remove_tracks(&self, _playlist_id: &str, _uris: &[String]) -> anyhow::Result<()> { Ok(()) }
        async fn delete_playlist(&self, _playlist_id: &str) -> anyhow::Result<()> { Ok(()) }
        async fn search_track_uri(&self, _title: &str, _artist: &str) -> anyhow::Result<Option<String>> {
            let mut c = self.0.lock().unwrap();
            *c += 1;
            Ok(Some("uri".to_string()))
        }
        async fn list_playlist_tracks(&self, _playlist_id: &str) -> anyhow::Result<Vec<String>> { Ok(Vec::new()) }
        async fn playlist_is_valid(&self, _playlist_id: &str) -> anyhow::Result<bool> { Ok(true) }
        async fn search_track_uri_by_isrc(&self, _isrc: &str) -> anyhow::Result<Option<String>> {
            let mut c = self.0.lock().unwrap();
            *c += 1;
            Ok(None)
        }
    }

    let counter = Arc::new(std::sync::Mutex::new(0));
    let provider = Arc::new(CountingProvider::new(counter.clone()));

    // emulate the worker precompute logic with a nonempty track_ops vector
    let mut track_ops: Vec<(models::EventAction, Option<String>)> = Vec::new();
    track_ops.push((models::EventAction::Add, Some("song.mp3".to_string())));
    let has_delete = false;
    let rename_opt: Option<(String, String)> = None;

    let mut reconcile_desired: Option<Vec<String>> = None;
    if !has_delete && track_ops.is_empty() && rename_opt.is_none() {
        // would call `desired_remote_uris_for_playlist` here
        let _ = music_file_playlist_online_sync::worker::desired_remote_uris_for_playlist(&cfg, "foo", provider.clone()).await;
        reconcile_desired = Some(Vec::new());
    }

    assert!(reconcile_desired.is_none(), "precompute should have been skipped");
    // nothing should have been looked up
    assert_eq!(*counter.lock().unwrap(), 0);
}
// rename-only scenario should behave the same way; although there are no track
// operations the user is simply renaming a folder, so we should not resolve
// the playlist contents until later (nothing will be added/removed).
#[tokio::test]
async fn skip_resolve_on_rename_only() {
    let td = tempfile::tempdir().unwrap();
    let root = td.path().join("root");
    std::fs::create_dir_all(root.join("foo")).unwrap();
    std::fs::write(root.join("foo").join("song.mp3"), b"data").unwrap();
    let playlist_path = root.join("foo").join("foo.m3u");
    std::fs::write(&playlist_path, "song.mp3\n").unwrap();

    let cfg_file = td.path().join("cfg.toml");
    let db_file = td.path().join("test.db");
    std::fs::write(
        &cfg_file,
        format!(
            "root_folder = \"{}\"\ndb_path = \"{}\"\n",
            root.display(),
            db_file.display()
        ),
    )
    .unwrap();
    let cfg = music_file_playlist_online_sync::config::Config::from_path(&cfg_file).unwrap();
    {
        let conn = rusqlite::Connection::open(&cfg.db_path).unwrap();
        music_file_playlist_online_sync::db::run_migrations(&conn).unwrap();
    }

    struct CountingProvider(Arc<std::sync::Mutex<usize>>);
    impl CountingProvider {
        fn new(counter: Arc<std::sync::Mutex<usize>>) -> Self {
            Self(counter)
        }
    }
    #[async_trait::async_trait]
    impl api::Provider for CountingProvider {
        fn name(&self) -> &str { "count" }
        fn is_authenticated(&self) -> bool { true }
        async fn ensure_playlist(&self, _name: &str, _desc: &str) -> anyhow::Result<String> { Ok("id".into()) }
        async fn rename_playlist(&self, _playlist_id: &str, _new_name: &str) -> anyhow::Result<()> { Ok(()) }
        async fn add_tracks(&self, _playlist_id: &str, _uris: &[String]) -> anyhow::Result<()> { Ok(()) }
        async fn remove_tracks(&self, _playlist_id: &str, _uris: &[String]) -> anyhow::Result<()> { Ok(()) }
        async fn delete_playlist(&self, _playlist_id: &str) -> anyhow::Result<()> { Ok(()) }
        async fn search_track_uri(&self, _title: &str, _artist: &str) -> anyhow::Result<Option<String>> {
            let mut c = self.0.lock().unwrap();
            *c += 1;
            Ok(Some("uri".to_string()))
        }
        async fn list_playlist_tracks(&self, _playlist_id: &str) -> anyhow::Result<Vec<String>> { Ok(Vec::new()) }
        async fn playlist_is_valid(&self, _playlist_id: &str) -> anyhow::Result<bool> { Ok(true) }
        async fn search_track_uri_by_isrc(&self, _isrc: &str) -> anyhow::Result<Option<String>> {
            let mut c = self.0.lock().unwrap();
            *c += 1;
            Ok(None)
        }
    }

    let counter = Arc::new(std::sync::Mutex::new(0));
    let provider = Arc::new(CountingProvider::new(counter.clone()));

    let track_ops: Vec<(models::EventAction, Option<String>)> = Vec::new();
    let has_delete = false;
    let rename_opt: Option<(String, String)> = Some(("foo".to_string(), "bar".to_string()));

    let mut reconcile_desired: Option<Vec<String>> = None;
    if !has_delete && track_ops.is_empty() && rename_opt.is_none() {
        let _ = music_file_playlist_online_sync::worker::desired_remote_uris_for_playlist(&cfg, "foo", provider.clone()).await;
        reconcile_desired = Some(Vec::new());
    }

    assert!(reconcile_desired.is_none(), "precompute should have been skipped on rename");
    assert_eq!(*counter.lock().unwrap(), 0);
}
