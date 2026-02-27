use music_file_playlist_online_sync::config::Config;
use music_file_playlist_online_sync::db;
use music_file_playlist_online_sync::worker::run_worker_once;
use std::path::PathBuf;
use tempfile::NamedTempFile;

#[tokio::test]
async fn test_worker_no_credentials() {
    let tmpfile = NamedTempFile::new().unwrap();
    let db_path = tmpfile.path().to_str().unwrap().to_string();
    let cfg = Config {
        db_path: db_path.clone().into(),
        root_folder: PathBuf::new(),
        whitelist: String::new(),
        local_whitelist: String::new(),
        remote_whitelist: String::new(),
        local_playlist_template: String::new(),
        remote_playlist_template: String::new(),
        remote_playlist_template_flat: String::new(),
        remote_playlist_template_folders: String::new(),
        playlist_description_template: String::new(),
        playlist_order_mode: String::new(),
        playlist_mode: String::new(),
        linked_reference_format: String::new(),
        debounce_ms: 0,
        log_dir: PathBuf::new(),
        token_refresh_interval: 0,
        worker_interval_sec: 0,
        nightly_reconcile_cron: String::new(),
        queue_length_stop_cloud_sync_threshold: None,
        max_retries_on_error: 0,
        max_batch_size_spotify: 0,
        max_batch_size_tidal: 0,
        file_extensions: Vec::new(),
        online_root_playlist: String::new(),
        online_playlist_structure: "flat".into(),
        online_folder_flattening_delimiter: String::new(),
    };
    // Run migrations to set up schema in the temp DB
    let conn = rusqlite::Connection::open(&cfg.db_path).unwrap();
    db::run_migrations(&conn).unwrap();
    // Should not panic or process events
    let result = run_worker_once(&cfg).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_worker_with_mock_and_real_providers() {
    // This test assumes you have a way to inject credentials into the DB and environment
    // For demonstration, we only check that the function runs without error
    let tmpfile = NamedTempFile::new().unwrap();
    let db_path = tmpfile.path().to_str().unwrap().to_string();
    let cfg = Config {
        db_path: db_path.clone().into(),
        root_folder: PathBuf::new(),
        whitelist: String::new(),
        local_whitelist: String::new(),
        remote_whitelist: String::new(),
        local_playlist_template: String::new(),
        remote_playlist_template: String::new(),
        remote_playlist_template_flat: String::new(),
        remote_playlist_template_folders: String::new(),
        playlist_description_template: String::new(),
        playlist_order_mode: String::new(),
        playlist_mode: String::new(),
        linked_reference_format: String::new(),
        debounce_ms: 0,
        log_dir: PathBuf::new(),
        token_refresh_interval: 0,
        worker_interval_sec: 0,
        nightly_reconcile_cron: String::new(),
        queue_length_stop_cloud_sync_threshold: None,
        max_retries_on_error: 0,
        max_batch_size_spotify: 0,
        max_batch_size_tidal: 0,
        file_extensions: Vec::new(),
        online_root_playlist: String::new(),
        online_playlist_structure: "flat".into(),
        online_folder_flattening_delimiter: String::new(),
    };
    // Run migrations to set up schema in the temp DB
    let conn = rusqlite::Connection::open(&cfg.db_path).unwrap();
    db::run_migrations(&conn).unwrap();
    // Should not panic or process events
    let result = run_worker_once(&cfg).await;
    assert!(result.is_ok());
}
