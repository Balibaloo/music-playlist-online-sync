pub mod mock;
pub mod pkce;
pub mod spotify;
pub mod spotify_auth;
pub mod tidal;
pub mod tidal_auth;

use anyhow::Result;
use crate::config::Config;

/// Describes an HTTP request to be executed by [`Provider::execute_request`].
///
/// Build via the constructor methods ([`RequestSpec::get`], [`RequestSpec::post`], etc.).
/// The `Authorization: Bearer <token>` header is injected automatically.
pub struct RequestSpec {
    pub(crate) method: reqwest::Method,
    pub(crate) url: String,
    /// Optional JSON body. When present reqwest sets `Content-Type: application/json`
    /// before any explicit headers below, so a subsequent [`RequestSpec::header`] call
    /// can override that content type (e.g. for Tidal's `application/vnd.tidal.v1+json`).
    pub(crate) json: Option<serde_json::Value>,
    /// Extra headers applied *after* the JSON body so they can override Content-Type.
    pub(crate) headers: Vec<(String, String)>,
}

impl RequestSpec {
    fn new(method: reqwest::Method, url: impl Into<String>) -> Self {
        Self { method, url: url.into(), json: None, headers: vec![] }
    }
    pub fn get(url: impl Into<String>) -> Self    { Self::new(reqwest::Method::GET,    url) }
    pub fn post(url: impl Into<String>) -> Self   { Self::new(reqwest::Method::POST,   url) }
    pub fn put(url: impl Into<String>) -> Self    { Self::new(reqwest::Method::PUT,    url) }
    pub fn patch(url: impl Into<String>) -> Self  { Self::new(reqwest::Method::PATCH,  url) }
    pub fn delete(url: impl Into<String>) -> Self { Self::new(reqwest::Method::DELETE, url) }

    /// Attach a JSON body (sets `Content-Type: application/json` by default).
    pub fn json(mut self, body: serde_json::Value) -> Self {
        self.json = Some(body);
        self
    }

    /// Append an extra HTTP header.  Applied *after* the JSON body so it can
    /// override `Content-Type` when an API requires a custom media type.
    pub fn header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.push((name.into(), value.into()));
        self
    }
}

/// Provider trait: a minimal set of operations the worker needs.
/// Implementations: spotify::SpotifyProvider, mock::MockProvider, tidal::TidalProvider.
#[async_trait::async_trait]
pub trait Provider: Send + Sync {
    // ------------------------------------------------------------------
    // Required: HTTP plumbing (used by the default `execute_request` impl)
    // ------------------------------------------------------------------

    /// Return the underlying `reqwest::Client` for this provider.
    fn http_client(&self) -> &reqwest::Client;

    /// Return a valid `"Bearer <token>"` string, loading and/or refreshing
    /// the stored access token as needed.
    async fn get_bearer(&self) -> Result<String>;

    /// Force-refresh the access token.  Called automatically by
    /// [`Self::execute_request`] when the server returns 401 Unauthorized.
    async fn refresh_token(&self) -> Result<()>;

    // ------------------------------------------------------------------
    // Provided: shared request execution with automatic retry / back-off
    // ------------------------------------------------------------------

    /// Execute the HTTP request described by `spec`, retrying automatically on:
    ///
    /// - **401 Unauthorized** – calls [`Self::refresh_token`] once, then retries.
    /// - **429 Too Many Requests** – sleeps for `Retry-After + 1` seconds, up to
    ///   three attempts.
    ///
    /// Returns the raw `Response` for any status code after exhausting retries.
    /// Non-2xx statuses (other than 401/429 handled above) are returned as-is;
    /// callers are responsible for checking `resp.status().is_success()`.
    ///
    /// **This method must not be overridden** by provider implementations.
    async fn execute_request(&self, op: &str, spec: &RequestSpec) -> Result<reqwest::Response> {
        use reqwest::header::AUTHORIZATION;
        const MAX_RATE_LIMIT_RETRIES: u32 = 3;
        let mut attempt: u32 = 0;
        loop {
            attempt += 1;
            let bearer = self.get_bearer().await?;
            let client = self.http_client();
            let mut builder = match spec.method {
                reqwest::Method::GET    => client.get(&spec.url),
                reqwest::Method::POST   => client.post(&spec.url),
                reqwest::Method::PUT    => client.put(&spec.url),
                reqwest::Method::PATCH  => client.patch(&spec.url),
                reqwest::Method::DELETE => client.delete(&spec.url),
                ref m => return Err(anyhow::anyhow!(
                    "execute_request: unsupported method {}", m
                )),
            };
            builder = builder.header(AUTHORIZATION, &bearer);
            // Apply JSON body first; extra headers follow so they can override Content-Type.
            if let Some(body) = &spec.json {
                builder = builder.json(body);
            }
            for (k, v) in &spec.headers {
                builder = builder.header(k.as_str(), v.as_str());
            }
            let resp = builder.send().await?;
            let status = resp.status();

            if status == reqwest::StatusCode::TOO_MANY_REQUESTS
                && attempt <= MAX_RATE_LIMIT_RETRIES
            {
                let retry_after = resp
                    .headers()
                    .get("retry-after")
                    .and_then(|v| v.to_str().ok())
                    .and_then(|s| s.parse::<u64>().ok())
                    .unwrap_or(2);
                let sleep_secs = retry_after + 1;
                log::info!(
                    "{} {} rate limited – sleeping {} s (attempt {})",
                    self.name(), op, sleep_secs, attempt,
                );
                tokio::time::sleep(std::time::Duration::from_secs(sleep_secs)).await;
                continue;
            }

            if status == reqwest::StatusCode::UNAUTHORIZED && attempt == 1 {
                log::debug!(
                    "{} {} got 401, refreshing token and retrying",
                    self.name(), op,
                );
                self.refresh_token().await?;
                continue;
            }

            return Ok(resp);
        }
    }

    // ------------------------------------------------------------------
    // Required: playlist operations
    // ------------------------------------------------------------------

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

    /// Lookup track metadata (e.g., ISRC) given a resolved URI. Default returns None.
    async fn lookup_track_isrc(&self, _uri: &str) -> Result<Option<String>> {
        Ok(None)
    }

    /// Check whether a remote playlist id is still valid and accessible.
    ///
    /// Returns `Ok(Some(current_name))` when valid, `Ok(None)` when the playlist
    /// no longer exists or is inaccessible.  The default implementation always
    /// returns `Ok(Some(""))` (assumes perpetual validity).
    async fn playlist_is_valid(&self, _playlist_id: &str) -> Result<Option<String>> {
        Ok(Some(String::new()))
    }

    /// Return the provider's name (for logging, UI, etc.)
    fn name(&self) -> &str;

    /// Return true if the provider is authenticated and ready to process events.
    fn is_authenticated(&self) -> bool;

    /// Whether this provider supports hierarchical playlist folders.
    fn supports_folder_nesting(&self) -> bool {
        true
    }

    /// Maximum number of track URIs to send in a single batch request.
    fn max_batch_size(&self, cfg: &Config) -> usize {
        cfg.max_batch_size_spotify
    }

    /// Validate a resolved remote URI before it is sent to the provider.
    /// Return `true` if the URI is acceptable, `false` to drop it.
    fn validate_uri(&self, _uri: &str) -> bool {
        true
    }
}
