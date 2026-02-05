use music_file_playlist_online_sync::collapse::collapse_events;
use music_file_playlist_online_sync::models::{Event, EventAction};

#[test]
fn collapse_add_remove_pair() {
    let a = Event { id:1, timestamp_ms:1, playlist_name:"p".into(), action: EventAction::Add, track_path: Some("t.mp3".into()), extra: None, is_synced:false };
    let r = Event { id:2, timestamp_ms:2, playlist_name:"p".into(), action: EventAction::Remove, track_path: Some("t.mp3".into()), extra: None, is_synced:false };
    let res = collapse_events(&[a, r]);
    // Should not contain add/remove for t.mp3 after collapse
    assert!(res.iter().all(|e| e.track_path.as_deref() != Some("t.mp3")));
}