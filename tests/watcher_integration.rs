use std::fs::{self, File};
use std::time::Duration;
use tempfile::tempdir;

use music_file_playlist_online_sync::config::Config;
use music_file_playlist_online_sync::watcher;

#[test]
fn run_watcher_writes_initial_playlists() {
    let td = tempdir().unwrap();
    let root = td.path().join("root");
    fs::create_dir_all(&root.join("a")).unwrap();
    fs::create_dir_all(&root.join("b")).unwrap();
    // create some tracks
    let _ = File::create(&root.join("a").join("s1.mp3")).unwrap();
    let _ = File::create(&root.join("b").join("s2.mp3")).unwrap();

    // write a temporary config TOML and load via Config::from_path
    let cfg_path = td.path().join("cfg.toml");
    let cfg_toml = format!(
        r#"
root_folder = "{}"
db_path = "{}"
log_dir = "{}"
debounce_ms = 100
playlist_mode = "flat"
local_playlist_template = "${{folder_name}}.m3u"
"#,
        root.display(),
        td.path().join("db.sqlite").display(),
        td.path().display()
    );
    fs::write(&cfg_path, cfg_toml).unwrap();
    let cfg = Config::from_path(&cfg_path).expect("load cfg");

    // Run only the initial watcher pass (DB + scan + playlist writes)
    // so the test can complete without blocking on the long-running loop.
    watcher::run_watcher_initial_pass(&cfg).expect("run watcher initial pass");

    // initial playlists should have been written for both folders
    let p_a = root.join("a").join("a.m3u");
    let p_b = root.join("b").join("b.m3u");

    // wait briefly for file writes
    std::thread::sleep(Duration::from_millis(200));
    assert!(p_a.exists(), "playlist for a should exist");
    assert!(p_b.exists(), "playlist for b should exist");
}
