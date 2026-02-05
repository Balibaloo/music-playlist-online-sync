pub mod spotify;
pub mod mock;
pub mod tidal;
pub mod spotify_auth;
pub mod tidal_auth;

use anyhow::Result;

/// Provider trait: a minimal set of operations the worker needs.
/// Implementations: spotify::SpotifyProvider, mock::MockProvider, and later tidal::TidalProvider.
#[async_trait::async_trait]
pub trait Provider: Send + Sync {
    /// Ensure playlist exists (create or fetch) and return remote playlist id.
    async fn ensure_playlist(&self, name: &str, description: &str) -> Result<String>;

    /// Rename a playlist remote id
    async fn rename_playlist(&self, playlist_id: &str, new_name: &str) -> Result<()>;

    /// Add tracks (URIs) to playlist (batching done by caller)
    async fn add_tracks(&self, playlist_id: &str, uris: &[String]) -> Result<()>;

    /// Remove tracks (URIs) from playlist
    async fn remove_tracks(&self, playlist_id: &str, uris: &[String]) -> Result<()>;

    /// Search for a track by metadata: title, artist. Return a remote URI if found.
    async fn search_track_uri(&self, title: &str, artist: &str) -> Result<Option<String>>;

    /// Lookup track metadata (e.g., ISRC) given a resolved URI. Default implementation returns None.
    async fn lookup_track_isrc(&self, _uri: &str) -> Result<Option<String>> {
        Ok(None)
    }
        /// Return the provider's name (for logging, UI, etc)
        fn name(&self) -> &str;

        /// Return true if the provider is authenticated and ready to process events
        fn is_authenticated(&self) -> bool;
}