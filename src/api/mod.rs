pub mod mock;
pub mod pkce;
pub mod spotify;
pub mod spotify_auth;
pub mod tidal;
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

    /// Delete a playlist entirely on the provider side
    async fn delete_playlist(&self, playlist_id: &str) -> Result<()>;

    /// List all track URIs currently in a remote playlist.
    /// Implementations should return a de-duplicated, stable list.
    async fn list_playlist_tracks(&self, playlist_id: &str) -> Result<Vec<String>>;

    /// Search for a track by metadata: title, artist. Return a remote URI if found.
    async fn search_track_uri(&self, title: &str, artist: &str) -> Result<Option<String>>;

    /// Search for a track by ISRC, if supported by the provider. Default returns None.
    async fn search_track_uri_by_isrc(&self, _isrc: &str) -> Result<Option<String>> {
        Ok(None)
    }

    /// Lookup track metadata (e.g., ISRC) given a resolved URI. Default implementation returns None.
    async fn lookup_track_isrc(&self, _uri: &str) -> Result<Option<String>> {
        Ok(None)
    }

    /// Return true if the given playlist id still refers to a valid,
    /// accessible playlist on the provider. The default implementation
    /// assumes playlists remain valid forever and always returns true.
    ///
    /// Providers like Spotify that treat "delete" as an "unfollow" can
    /// override this to detect when the current user no longer has access
    /// to the playlist so callers can recreate it.
    async fn playlist_is_valid(&self, _playlist_id: &str) -> Result<bool> {
        Ok(true)
    }
    /// Return the provider's name (for logging, UI, etc)
    fn name(&self) -> &str;

    /// Return true if the provider is authenticated and ready to process events
    fn is_authenticated(&self) -> bool;
}
