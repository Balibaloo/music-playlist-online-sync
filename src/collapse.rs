use crate::models::{Event, EventAction};
use std::collections::HashMap;

/// Collapse neutral add/remove pairs and resolve per-playlist final operations.
/// Returns a vector of events (not necessarily identical to inputs) representing the minimal set
/// of actions to apply in order.
///
/// Algorithm:
/// - Track an insertion-ordered map of track_path -> (index, action).
/// - Cancel Add followed by Remove (or vice-versa) for the same track_path.
/// - Preserve temporal ordering of surviving track ops by sorting on insertion index.
/// - Create/Delete/Rename events are preserved in their original order and placed before track ops.
pub fn collapse_events(events: &[Event]) -> Vec<Event> {
    // We operate per-playlist in the worker; this function assumes events for one playlist.
    //
    // `track_state` maps track_path -> (insertion_index, action).
    // `next_index` monotonically increases to preserve insertion order.
    let mut track_state: HashMap<String, (usize, EventAction)> = HashMap::new();
    let mut next_index: usize = 0;
    let mut other_ops: Vec<Event> = Vec::new(); // holds create/delete/rename events to be preserved in order

    for ev in events {
        match &ev.action {
            EventAction::Add => {
                if let Some(tp) = &ev.track_path {
                    match track_state.get(tp).map(|(_, a)| a) {
                        Some(EventAction::Remove) => {
                            // cancel both
                            track_state.remove(tp);
                        }
                        _ => {
                            track_state.insert(tp.clone(), (next_index, EventAction::Add));
                            next_index += 1;
                        }
                    }
                }
            }
            EventAction::Remove => {
                if let Some(tp) = &ev.track_path {
                    match track_state.get(tp).map(|(_, a)| a) {
                        Some(EventAction::Add) => {
                            // cancel
                            track_state.remove(tp);
                        }
                        _ => {
                            track_state.insert(tp.clone(), (next_index, EventAction::Remove));
                            next_index += 1;
                        }
                    }
                }
            }
            EventAction::Rename { .. } | EventAction::Create | EventAction::Delete => {
                other_ops.push(ev.clone());
            }
        }
    }

    // Reconstruct minimal event list: other_ops (in original order) then track ops
    // sorted by their insertion index so temporal ordering is preserved.
    let mut out: Vec<Event> = Vec::new();
    out.extend(other_ops.into_iter());

    let mut track_entries: Vec<(String, usize, EventAction)> = track_state
        .into_iter()
        .map(|(path, (idx, action))| (path, idx, action))
        .collect();
    track_entries.sort_by_key(|&(_, idx, _)| idx);

    for (path, _, action) in track_entries {
        let ev = Event {
            id: 0,
            timestamp_ms: chrono::Utc::now().timestamp_millis(),
            playlist_name: "".into(),
            action,
            track_path: Some(path),
            extra: None,
            is_synced: false,
        };
        out.push(ev);
    }

    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::Event;
    #[test]
    fn collapse_add_remove_pair() {
        let a = Event {
            id: 1,
            timestamp_ms: 1,
            playlist_name: "p".into(),
            action: EventAction::Add,
            track_path: Some("t.mp3".into()),
            extra: None,
            is_synced: false,
        };
        let r = Event {
            id: 2,
            timestamp_ms: 2,
            playlist_name: "p".into(),
            action: EventAction::Remove,
            track_path: Some("t.mp3".into()),
            extra: None,
            is_synced: false,
        };
        let res = collapse_events(&[a, r]);
        assert!(
            res.iter().all(|e| match e.action {
                EventAction::Add | EventAction::Remove => false,
                _ => true,
            }) == false
                || res.is_empty()
        );
        // In our simple impl they cancel out => no track ops included
        // Only ensure no Add/Remove remains for that track
        assert!(res.iter().all(|e| e.track_path.as_deref() != Some("t.mp3")));
    }

    #[test]
    fn collapse_preserves_insertion_order() {
        let events = vec![
            Event {
                id: 1,
                timestamp_ms: 1,
                playlist_name: "p".into(),
                action: EventAction::Add,
                track_path: Some("a.mp3".into()),
                extra: None,
                is_synced: false,
            },
            Event {
                id: 2,
                timestamp_ms: 2,
                playlist_name: "p".into(),
                action: EventAction::Add,
                track_path: Some("b.mp3".into()),
                extra: None,
                is_synced: false,
            },
            Event {
                id: 3,
                timestamp_ms: 3,
                playlist_name: "p".into(),
                action: EventAction::Remove,
                track_path: Some("a.mp3".into()),
                extra: None,
                is_synced: false,
            },
            Event {
                id: 4,
                timestamp_ms: 4,
                playlist_name: "p".into(),
                action: EventAction::Add,
                track_path: Some("c.mp3".into()),
                extra: None,
                is_synced: false,
            },
        ];
        let res = collapse_events(&events);
        let track_ops: Vec<&str> = res
            .iter()
            .filter(|e| matches!(e.action, EventAction::Add | EventAction::Remove))
            .map(|e| e.track_path.as_deref().unwrap())
            .collect();
        // a.mp3 was added then removed -> cancelled
        // b.mp3 (Add) should come before c.mp3 (Add), preserving insertion order
        assert_eq!(track_ops, vec!["b.mp3", "c.mp3"]);
    }
}
