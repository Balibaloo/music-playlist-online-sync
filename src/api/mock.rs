use super::Provider;
use anyhow::Result;
use async_trait::async_trait;
use tracing::info;

/// A simple mock provider used in tests and when no real credentials are present.
/// It logs operations and returns deterministic fake IDs/URIs.
pub struct MockProvider {}

impl MockProvider {
    pub fn new() -> Self {
        Self {}
    }
    fn is_authenticated(&self) -> bool {
        false
    }
    fn name(&self) -> &str {
        "mock"
    }
}

#[async_trait]
impl Provider for MockProvider {
    fn name(&self) -> &str {
        MockProvider::name(self)
    }
    fn is_authenticated(&self) -> bool {
        MockProvider::is_authenticated(self)
    }
    async fn ensure_playlist(&self, name: &str, _description: &str) -> Result<String> {
        info!("MockProvider: ensure_playlist {}", name);
        Ok(format!("mock-playlist-{}", name))
    }

    async fn rename_playlist(&self, playlist_id: &str, new_name: &str) -> Result<()> {
        info!("MockProvider: rename_playlist {} -> {}", playlist_id, new_name);
        Ok(())
    }

    async fn add_tracks(&self, playlist_id: &str, uris: &[String]) -> Result<()> {
        info!("MockProvider: add_tracks {} -> {} tracks", playlist_id, uris.len());
        Ok(())
    }

    async fn remove_tracks(&self, playlist_id: &str, uris: &[String]) -> Result<()> {
        info!("MockProvider: remove_tracks {} -> {} tracks", playlist_id, uris.len());
        Ok(())
    }

    async fn search_track_uri(&self, title: &str, artist: &str) -> Result<Option<String>> {
        info!("MockProvider: search {} - {}", title, artist);
        Ok(Some(format!("mock:track:{}:{}", title, artist)))
    }
}