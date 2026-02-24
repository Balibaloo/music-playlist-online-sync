#[test]
fn nightly_reconcile_enqueues_events() -> Result<(), Box<dyn std::error::Error>> {
    let tmp = tempfile::tempdir()?;
    let root = tmp.path().join("root");
    std::fs::create_dir_all(&root)?;

    // create a sample folder with one track
    let album = root.join("Album1");
    std::fs::create_dir_all(&album)?;
    let track = album.join("01 - Test Track.mp3");
    std::fs::write(&track, b"")?;

    // build a minimal config
    let cfg = music_file_playlist_online_sync::config::Config {
        root_folder: root.clone(),
        whitelist: String::new(),
        local_playlist_template: "${folder_name}.m3u".into(),
        remote_playlist_template: "${relative_path}".into(),
        remote_playlist_template_flat: String::new(),
        remote_playlist_template_folders: String::new(),
        playlist_description_template: String::new(),
        playlist_order_mode: "append".into(),
        playlist_mode: "flat".into(),
        linked_reference_format: "relative".into(),
        debounce_ms: 100,
        log_dir: tmp.path().join("logs"),
        token_refresh_interval: 3600,
        worker_interval_sec: 60,
        nightly_reconcile_cron: "0 3 * * *".into(),
        queue_length_stop_cloud_sync_threshold: None,
        max_retries_on_error: 3,
        max_batch_size_spotify: 100,
        file_extensions: vec!["*.mp3".into()],
        online_root_playlist: String::new(),
        online_playlist_structure: "flat".into(),
        online_folder_flattening_delimiter: String::new(),
        db_path: tmp.path().join("test.db"),
    };

    // run reconcile
    music_file_playlist_online_sync::worker::run_nightly_reconcile(&cfg)?;
    // verify DB has queued events (ensure migrations applied)
    let conn =
        music_file_playlist_online_sync::db::open_or_create(std::path::Path::new(&cfg.db_path))?;
    let cnt: i64 = conn.query_row("SELECT COUNT(*) FROM event_queue", [], |r| r.get(0))?;
    assert!(
        cnt > 0,
        "expected event_queue to have entries after reconcile"
    );

    Ok(())
}
