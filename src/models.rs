use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EventAction {
    Add,
    Remove,
    // rename uses extra JSON: {"from":"old","to":"new"}
    Rename { from: String, to: String },
    Create,
    Delete,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    pub id: i64,
    pub timestamp_ms: i64,
    pub playlist_name: String,
    pub action: EventAction,
    pub track_path: Option<String>,
    pub extra: Option<String>,
    pub is_synced: bool,
}