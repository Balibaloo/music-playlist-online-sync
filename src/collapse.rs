use crate::models::{Event, EventAction};
use std::collections::HashMap;

/// Collapse neutral add/remove pairs and resolve per-playlist final operations.
/// Returns a vector of events (not necessarily identical to inputs) representing the minimal set
/// of actions to apply in order.
///
/// Simple algorithm used by prototype:
/// - Track a hashmap of track_path -> last action (Add or Remove)
/// - Renames are applied first; if a rename shows up, update keys accordingly.
/// - Cancel Add followed by Remove (or vice-versa).
pub fn collapse_events(events: &[Event]) -> Vec<Event> {
    // We operate per-playlist in the worker; this function assumes events for one playlist.
    let mut track_state: HashMap<String, EventAction> = HashMap::new();
    let mut other_ops: Vec<Event> = Vec::new(); // holds create/delete/rename events to be preserved in order

    for ev in events {
        match &ev.action {
            EventAction::Add => {
                if let Some(tp) = &ev.track_path {
                    match track_state.get(tp) {
                        Some(EventAction::Remove) => {
                            // remove cancel previous remove -> cancel both by deleting state
                            track_state.remove(tp);
                        }
                        _ => {
                            track_state.insert(tp.clone(), EventAction::Add);
                        }
                    }
                }
            }
            EventAction::Remove => {
                if let Some(tp) = &ev.track_path {
                    match track_state.get(tp) {
                        Some(EventAction::Add) => {
                            // cancel
                            track_state.remove(tp);
                        }
                        _ => {
                            track_state.insert(tp.clone(), EventAction::Remove);
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
    let mut out: Vec<Event> = Vec::new();
    out.extend(other_ops.into_iter());

    for (path, action) in track_state.into_iter() {
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
        let a = Event { id:1, timestamp_ms:1, playlist_name:"p".into(), action: EventAction::Add, track_path: Some("t.mp3".into()), extra: None, is_synced:false };
        let r = Event { id:2, timestamp_ms:2, playlist_name:"p".into(), action: EventAction::Remove, track_path: Some("t.mp3".into()), extra: None, is_synced:false };
        let res = collapse_events(&[a, r]);
        assert!(res.iter().all(|e| match e.action { EventAction::Add|EventAction::Remove => false, _ => true } ) == false || res.is_empty());
        // In our simple impl they cancel out => no track ops included
        // Only ensure no Add/Remove remains for that track
        assert!(res.iter().all(|e| e.track_path.as_deref() != Some("t.mp3")));
    }
}