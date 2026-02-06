use serde::Deserialize;
use std::path::PathBuf;

#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    pub root_folder: PathBuf,
    #[serde(default)]
    pub whitelist: String,
    #[serde(default = "default_local_template")]
    pub local_playlist_template: String,
    #[serde(default = "default_remote_template")]
    pub remote_playlist_template: String,
    #[serde(default)]
    pub playlist_description_template: String,
    #[serde(default = "default_playlist_order_mode")]
    pub playlist_order_mode: String,
    #[serde(default = "default_playlist_mode")]
    pub playlist_mode: String,
    #[serde(default = "default_linked_reference_format")]
    pub linked_reference_format: String,
    #[serde(default = "default_debounce")]
    pub debounce_ms: u64,
    #[serde(default = "default_log_dir")]
    pub log_dir: PathBuf,
    #[serde(default = "default_token_refresh_interval")]
    pub token_refresh_interval: u64,

    // Worker/timing
    #[serde(default = "default_worker_interval")]
    pub worker_interval_sec: u64,
    #[serde(default = "default_nightly_cron")]
    pub nightly_reconcile_cron: String,

    // Queue behavior
    #[serde(default)]
    pub queue_length_stop_cloud_sync_threshold: Option<u64>,

    #[serde(default = "default_max_retries")]
    pub max_retries_on_error: u32,

    #[serde(default = "default_max_batch_spotify")]
    pub max_batch_size_spotify: usize,

    // path to database file
    #[serde(default = "default_db_path")]
    pub db_path: PathBuf,

    /// Whitelist of file extensions to treat as track/media files.
    /// Examples: ["*.mp3", "*.flac", "wav"]. Case-insensitive.
    #[serde(default = "default_file_extensions")]
    pub file_extensions: Vec<String>,
}

fn default_local_template() -> String { "${folder_name}.m3u".into() }
fn default_remote_template() -> String { "${relative_path}".into() }
fn default_playlist_order_mode() -> String { "append".into() }
fn default_playlist_mode() -> String { "flat".into() }
fn default_linked_reference_format() -> String { "relative".into() }
fn default_debounce() -> u64 { 250 }
fn default_log_dir() -> PathBuf { "/var/log/music-sync".into() }
fn default_token_refresh_interval() -> u64 { 3600 }
fn default_worker_interval() -> u64 { 300 }
fn default_nightly_cron() -> String { "0 3 * * *".into() }
fn default_max_retries() -> u32 { 3 }
fn default_max_batch_spotify() -> usize { 100 }
fn default_db_path() -> PathBuf { "/var/lib/music-sync/music-sync.db".into() }

fn default_file_extensions() -> Vec<String> {
    vec![
        "*.mp3",
        "*.flac",
        "*.ogg",
        "*.wav",
        "*.mp4",
        "*.m4a",
    ]
    .into_iter()
    .map(String::from)
    .collect()
}

impl Config {
    pub fn from_path(path: &std::path::Path) -> anyhow::Result<Self> {
        let s = std::fs::read_to_string(path)?;
        let cfg: Config = toml::from_str(&s)?;
        Ok(cfg)
    }
}