use tempfile::tempdir;
use std::fs::{self, File};
use std::io::Write;
use std::path::PathBuf;

use music_file_playlist_online_sync::watcher::{InMemoryTree, SyntheticEvent, LogicalOp};

#[test]
fn in_memory_tree_applies_synthetic_events() {
    let td = tempdir().unwrap();
    let root = td.path().join("root");
    fs::create_dir_all(&root.join("a")).unwrap();
    fs::create_dir_all(&root.join("b")).unwrap();
    // create a track under a
    let track_a = root.join("a").join("song1.mp3");
    let mut f = File::create(&track_a).unwrap();
    writeln!(f, "data").unwrap();

    let mut tree = InMemoryTree::build(&root, None).expect("build tree");
    // file exists so node should have the track
    let folder_a = root.join("a");
    assert!(tree.nodes.contains_key(&folder_a));

    // apply a synthetic file create in b
    let new_track = root.join("b").join("new.mp3");
    let ops = tree.apply_synthetic_event(SyntheticEvent::FileCreate(new_track.clone()));
    assert!(matches!(ops.get(0).unwrap(), LogicalOp::Add { .. }));
    // node should now contain the new track
    assert!(tree.nodes.get(&root.join("b")).unwrap().tracks.contains(&new_track));

    // apply remove
    let ops = tree.apply_synthetic_event(SyntheticEvent::FileRemove(new_track.clone()));
    assert!(matches!(ops.get(0).unwrap(), LogicalOp::Remove { .. }));

    // rename between folders
    let from = track_a.clone();
    let to = root.join("b").join("song1_renamed.mp3");
    let ops = tree.apply_synthetic_event(SyntheticEvent::FileRename { from: from.clone(), to: to.clone() });
    // expect remove + add
    assert!(matches!(ops.get(0).unwrap(), LogicalOp::Remove { .. }));
    assert!(matches!(ops.get(1).unwrap(), LogicalOp::Add { .. }));
}
