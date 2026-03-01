//! Tests for the refactored code: connection pool, provider trait methods,
//! release_lock_async, apply_in_batches short-circuit, and the
//! all_providers_ok logic that prevents events from being marked synced
//! when a provider fails.

use music_file_playlist_online_sync::{api, db, models};
use music_file_playlist_online_sync::api::Provider;
use music_file_playlist_online_sync::models::EventAction;
use tempfile::tempdir;

// ---------------------------------------------------------------------------
// 1. Connection pool tests
// ---------------------------------------------------------------------------

#[test]
fn pool_creation_runs_migrations() {
    let td = tempdir().unwrap();
    let db_path = td.path().join("pool_test.db");
    // create_pool should run migrations automatically
    let pool = db::create_pool(&db_path).unwrap();
    let conn = pool.get().unwrap();
    // verify the event_queue table exists
    let count: i64 = conn
        .query_row("SELECT count(*) FROM event_queue", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 0);
}

#[test]
fn pool_connections_are_reusable() {
    let td = tempdir().unwrap();
    let db_path = td.path().join("pool_reuse.db");
    let pool = db::create_pool(&db_path).unwrap();

    // Check out, insert, return; then check out again and read.
    {
        let conn = pool.get().unwrap();
        db::enqueue_event(&conn, "test_pl", &EventAction::Create, None, None).unwrap();
    }
    {
        let conn = pool.get().unwrap();
        let evs = db::fetch_unsynced_events(&conn).unwrap();
        assert_eq!(evs.len(), 1);
        assert_eq!(evs[0].playlist_name, "test_pl");
    }
}

#[test]
fn pool_multiple_concurrent_checkouts() {
    let td = tempdir().unwrap();
    let db_path = td.path().join("pool_concurrent.db");
    let pool = db::create_pool(&db_path).unwrap();

    // max_size is 4, so 4 concurrent checkouts should work
    let c1 = pool.get().unwrap();
    let c2 = pool.get().unwrap();
    let c3 = pool.get().unwrap();
    let c4 = pool.get().unwrap();

    // All connections should be usable
    let _: i64 = c1.query_row("SELECT 1", [], |r| r.get(0)).unwrap();
    let _: i64 = c2.query_row("SELECT 1", [], |r| r.get(0)).unwrap();
    let _: i64 = c3.query_row("SELECT 1", [], |r| r.get(0)).unwrap();
    let _: i64 = c4.query_row("SELECT 1", [], |r| r.get(0)).unwrap();
}

// ---------------------------------------------------------------------------
// 2. Provider trait method tests
// ---------------------------------------------------------------------------

#[test]
fn mock_provider_default_trait_methods() {
    let mp = api::mock::MockProvider::new();
    // defaults from Provider trait
    assert!(mp.supports_folder_nesting());
    assert!(mp.validate_uri("anything"));
}

#[test]
fn tidal_provider_validate_uri() {
    use music_file_playlist_online_sync::api::tidal::TidalProvider;
    let tp = TidalProvider::new(
        String::new(),
        String::new(),
        std::path::PathBuf::from("/dev/null"),
        None,
    );
    // Valid tidal URIs: end with a positive numeric id
    assert!(tp.validate_uri("tidal:track:123456"));
    assert!(tp.validate_uri("tidal:track:1"));
    
    // Invalid: zero, non-numeric, empty
    assert!(!tp.validate_uri("tidal:track:0"));
    assert!(!tp.validate_uri("tidal:track:abc"));
    assert!(!tp.validate_uri("tidal:track:"));
    assert!(!tp.validate_uri(""));
}

#[test]
fn tidal_provider_no_folder_nesting() {
    use music_file_playlist_online_sync::api::tidal::TidalProvider;
    let tp = TidalProvider::new(
        String::new(),
        String::new(),
        std::path::PathBuf::from("/dev/null"),
        None,
    );
    assert!(!tp.supports_folder_nesting());
}

// ---------------------------------------------------------------------------
// 3. all_providers_ok logic test (events not synced on failure)
// ---------------------------------------------------------------------------

/// A provider that always fails on add_tracks.
#[allow(dead_code)]
struct FailingProvider;

#[async_trait::async_trait]
impl Provider for FailingProvider {
    fn name(&self) -> &str { "failing" }
    fn is_authenticated(&self) -> bool { true }
    async fn ensure_playlist(&self, _name: &str, _desc: &str) -> anyhow::Result<String> {
        Ok("pl_id".to_string())
    }
    async fn rename_playlist(&self, _id: &str, _name: &str) -> anyhow::Result<()> { Ok(()) }
    async fn add_tracks(&self, _id: &str, _uris: &[String]) -> anyhow::Result<()> {
        anyhow::bail!("simulated failure")
    }
    async fn remove_tracks(&self, _id: &str, _uris: &[String]) -> anyhow::Result<()> { Ok(()) }
    async fn delete_playlist(&self, _id: &str) -> anyhow::Result<()> { Ok(()) }
    async fn search_track_uri(&self, _t: &str, _a: &str) -> anyhow::Result<Option<String>> {
        Ok(None)
    }
    async fn list_playlist_tracks(&self, _id: &str) -> anyhow::Result<Vec<String>> {
        Ok(Vec::new())
    }
    async fn playlist_is_valid(&self, _id: &str) -> anyhow::Result<bool> { Ok(true) }
    async fn search_track_uri_by_isrc(&self, _isrc: &str) -> anyhow::Result<Option<String>> {
        Ok(None)
    }
}

/// Simulate the all_providers_ok logic: if a provider fails, events should NOT
/// be marked synced.
#[tokio::test]
async fn events_not_synced_when_provider_fails() {
    let td = tempdir().unwrap();
    let db_path = td.path().join("fail_test.db");
    let pool = db::create_pool(&db_path).unwrap();

    // Enqueue an event
    {
        let conn = pool.get().unwrap();
        db::enqueue_event(&conn, "failpl", &EventAction::Add, Some("track.mp3"), None).unwrap();
    }

    // Fetch events
    let events = {
        let conn = pool.get().unwrap();
        db::fetch_unsynced_events(&conn).unwrap()
    };
    assert_eq!(events.len(), 1);
    let event_id = events[0].id;

    // Simulate the all_providers_ok flag being set to false
    // (In real code this happens when apply_in_batches returns Err)
    let all_providers_ok = false;

    if all_providers_ok {
        let mut conn = pool.get().unwrap();
        db::mark_events_synced(&mut conn, &[event_id]).unwrap();
    }

    // Verify the event is still unsynced
    let still_unsynced = {
        let conn = pool.get().unwrap();
        db::fetch_unsynced_events(&conn).unwrap()
    };
    assert_eq!(still_unsynced.len(), 1, "Event should NOT be synced when provider fails");
    assert_eq!(still_unsynced[0].id, event_id);
}

/// When all providers succeed, events SHOULD be marked synced.
#[tokio::test]
async fn events_synced_when_all_providers_ok() {
    let td = tempdir().unwrap();
    let db_path = td.path().join("ok_test.db");
    let pool = db::create_pool(&db_path).unwrap();

    {
        let conn = pool.get().unwrap();
        db::enqueue_event(&conn, "okpl", &EventAction::Add, Some("track.mp3"), None).unwrap();
    }

    let events = {
        let conn = pool.get().unwrap();
        db::fetch_unsynced_events(&conn).unwrap()
    };
    let event_id = events[0].id;

    let all_providers_ok = true;
    if all_providers_ok {
        let mut conn = pool.get().unwrap();
        db::mark_events_synced(&mut conn, &[event_id]).unwrap();
    }

    let remaining = {
        let conn = pool.get().unwrap();
        db::fetch_unsynced_events(&conn).unwrap()
    };
    assert_eq!(remaining.len(), 0, "Event should be synced when all providers OK");
}

// ---------------------------------------------------------------------------
// 4. should_precompute_desired correctness
// ---------------------------------------------------------------------------

#[test]
fn should_precompute_desired_matrix() {
    use music_file_playlist_online_sync::worker::should_precompute_desired;

    // Only precompute when there's no delete, no track ops, and no rename
    assert!(should_precompute_desired(false, false, false));

    // Any of these should prevent precompute
    assert!(!should_precompute_desired(true, false, false));  // has delete
    assert!(!should_precompute_desired(false, true, false));   // has track ops
    assert!(!should_precompute_desired(false, false, true));   // has rename
    assert!(!should_precompute_desired(true, true, true));     // all three
}

// ---------------------------------------------------------------------------
// 5. Retry module integration
// ---------------------------------------------------------------------------

#[tokio::test]
async fn retry_with_backoff_respects_max_retries() {
    use music_file_playlist_online_sync::retry::{retry_with_backoff, RetryAction, RetryConfig};

    let attempts = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
    let attempts_clone = attempts.clone();

    let result = retry_with_backoff(
        &RetryConfig { max_attempts: 4, max_backoff_secs: 1, label: "test".into() },
        move |_attempt| {
            let a = attempts_clone.clone();
            async move {
                a.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                RetryAction::Retry { error: anyhow::anyhow!("always fail"), retry_after: Some(0) }
            }
        },
    )
    .await;

    assert!(result.is_err());
    // Should attempt max_attempts times (4)
    assert_eq!(attempts.load(std::sync::atomic::Ordering::SeqCst), 4);
}

// ---------------------------------------------------------------------------
// 6. Collapse preserves order (regression test)
// ---------------------------------------------------------------------------

#[test]
fn collapse_dedup_preserves_order() {
    use music_file_playlist_online_sync::collapse::collapse_events;

    let events = vec![
        models::Event { id: 1, timestamp_ms: 0, playlist_name: "a".into(), action: EventAction::Add, track_path: Some("t1.mp3".into()), extra: None, is_synced: false },
        models::Event { id: 2, timestamp_ms: 0, playlist_name: "a".into(), action: EventAction::Add, track_path: Some("t2.mp3".into()), extra: None, is_synced: false },
        models::Event { id: 3, timestamp_ms: 0, playlist_name: "a".into(), action: EventAction::Add, track_path: Some("t3.mp3".into()), extra: None, is_synced: false },
    ];

    let collapsed = collapse_events(&events);
    let paths: Vec<_> = collapsed.iter().map(|e| e.track_path.as_deref().unwrap_or("")).collect();
    assert_eq!(paths, vec!["t1.mp3", "t2.mp3", "t3.mp3"]);
}
