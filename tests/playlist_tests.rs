use std::fs::{self, File};
use std::io::Read;
use std::thread::sleep;
use std::time::Duration;
use tempfile::tempdir;

use music_file_playlist_online_sync::playlist;

#[test]
fn flat_playlist_ordering() {
    let td = tempdir().unwrap();
    let root = td.path();
    fs::create_dir_all(root).unwrap();
    // create two files with different modification times
    let f1 = root.join("a_song.mp3");
    let f2 = root.join("b_song.mp3");
    File::create(&f1).unwrap();
    sleep(Duration::from_millis(10));
    File::create(&f2).unwrap();

    let plist = root.join("out.m3u");
    // alphabetical order (default append)
    playlist::write_flat_playlist(root, &plist, "append", &vec!["*.mp3".to_string()]).unwrap();
    let mut s = String::new();
    File::open(&plist).unwrap().read_to_string(&mut s).unwrap();
    let lines: Vec<&str> = s.lines().collect();
    // M3U header
    assert_eq!(lines[0], "#EXTM3U");
    // First track should be a_song.mp3 (alphabetical)
    assert!(lines[1].contains("#EXTINF"));
    assert!(lines[1].contains("a_song.mp3"));

    // sync_order should list by modified time (a then b)
    let plist2 = root.join("out2.m3u");
    playlist::write_flat_playlist(root, &plist2, "sync_order", &vec!["*.mp3".to_string()]).unwrap();
    let mut s2 = String::new();
    File::open(&plist2)
        .unwrap()
        .read_to_string(&mut s2)
        .unwrap();
    let lines2: Vec<&str> = s2.lines().collect();
    // Header again
    assert_eq!(lines2[0], "#EXTM3U");
    // With sync_order we still expect a_song.mp3 to be first track
    assert!(lines2[1].contains("#EXTINF"));
    assert!(lines2[1].contains("a_song.mp3"));
}

#[test]
fn linked_playlist_children_refs() {
    let td = tempdir().unwrap();
    let root = td.path().join("root");
    let c1 = root.join("c1");
    let c2 = root.join("c2");
    fs::create_dir_all(&c1).unwrap();
    fs::create_dir_all(&c2).unwrap();
    // create child playlists
    let child1 = c1.join("c1.m3u");
    let child2 = c2.join("c2.m3u");
    File::create(&child1).unwrap();
    File::create(&child2).unwrap();

    let out = root.join("root.m3u");
    playlist::write_linked_playlist(&root, &out, "relative", "${folder_name}.m3u").unwrap();
    let mut s = String::new();
    File::open(&out).unwrap().read_to_string(&mut s).unwrap();
    let lines: Vec<&str> = s.lines().collect();
    assert_eq!(lines.len(), 2);
    assert!(lines[0].contains("c1.m3u") || lines[1].contains("c1.m3u"));
}
