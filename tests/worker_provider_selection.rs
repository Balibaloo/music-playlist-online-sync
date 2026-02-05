use tempfile::tempdir;
use rusqlite::Connection;
use music_file_playlist_online_sync::db;
use music_file_playlist_online_sync::config::Config;
use music_file_playlist_online_sync::worker::run_worker_once;
use music_file_playlist_online_sync::models::EventAction;

#[test]
fn run_worker_uses_mock_provider_and_marks_events_synced() {
    let td = tempdir().unwrap();
    let db_path = td.path().join("test.db");
    let conn = Connection::open(&db_path).unwrap();
    db::run_migrations(&conn).unwrap();

    // enqueue a Create event for playlist "pl1"
    db::enqueue_event(&conn, "pl1", &EventAction::Create, None, None).unwrap();

    let cfg = Config {
        root_folder: td.path().join("root"),
        whitelist: String::new(),
        local_playlist_template: "${folder_name}.m3u".into(),
        remote_playlist_template: "${relative_path}".into(),
        playlist_description_template: String::new(),
        playlist_order_mode: "append".into(),
        playlist_mode: "flat".into(),
        linked_reference_format: "relative".into(),
        debounce_ms: 100,
        log_dir: td.path().join("logs"),
        token_refresh_interval: 3600,
        worker_interval_sec: 60,
        nightly_reconcile_cron: "0 3 * * *".into(),
        queue_length_stop_cloud_sync_threshold: None,
        max_retries_on_error: 3,
        max_batch_size_spotify: 100,
        db_path: db_path.clone(),
    };

    // run worker once
    let mut rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async move { run_worker_once(&cfg).await.unwrap() });

    // verify events marked synced
    let cnt: i64 = conn.query_row("SELECT COUNT(*) FROM event_queue WHERE is_synced = 0", [], |r| r.get(0)).unwrap();
    assert_eq!(cnt, 0);
}
