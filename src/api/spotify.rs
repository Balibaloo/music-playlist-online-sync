use super::{Provider, RequestSpec};
use crate::db;
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use base64::{engine::general_purpose, Engine as _};
use chrono::Utc;
use log::debug;
use reqwest::header::AUTHORIZATION;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::env;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredToken {
    pub access_token: String,
    pub token_type: String,
    pub expires_at: i64, // epoch seconds
    pub refresh_token: Option<String>,
    pub scope: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[tokio::test]
    async fn cache_db_updates_when_cold() {
        // Create a unique temp DB path
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let mut path = std::env::temp_dir();
        path.push(format!("mfpos_test_{}.db", nanos));

        // Ensure migrations/schema are applied
        let _ = crate::db::open_or_create(&path).expect("open_or_create DB");

        let cfg = Config::default();
        let prov = SpotifyProvider::new("".into(), "".into(), path.clone(), cfg);

        // Start with cold in-memory cache
        {
            let cache = prov.playlist_cache.lock().await;
            assert!(cache.is_none());
        }

        // Add an entry (should write to DB without warming full cache)
        prov.cache_add_entry("pl1", "My Playlist").await;

        // Verify DB row exists and contains the new entry
        {
            let conn = rusqlite::Connection::open(&path).expect("open db");
            let got = crate::db::get_provider_playlist_list_cache(&conn, "spotify")
                .expect("get cache");
            assert!(got.is_some());
            let entries = got.unwrap();
            assert_eq!(entries.len(), 1);
            assert_eq!(entries[0].0, "pl1");
            assert_eq!(entries[0].1, "My Playlist");
        }

        // Update the name
        prov.cache_update_name("pl1", "My Playlist Renamed").await;
        {
            let conn = rusqlite::Connection::open(&path).expect("open db");
            let got = crate::db::get_provider_playlist_list_cache(&conn, "spotify")
                .expect("get cache");
            assert!(got.is_some());
            let entries = got.unwrap();
            assert_eq!(entries.len(), 1);
            assert_eq!(entries[0].1, "My Playlist Renamed");
        }

        // Remove the entry
        prov.cache_remove_entry("pl1").await;
        {
            let conn = rusqlite::Connection::open(&path).expect("open db");
            let got = crate::db::get_provider_playlist_list_cache(&conn, "spotify")
                .expect("get cache");
            // DB row should still exist but be an empty entries vector
            assert!(got.is_some());
            let entries = got.unwrap();
            assert!(entries.is_empty());
        }

        // Cleanup
        let _ = std::fs::remove_file(&path);
    }
}

/// Spotify provider backed by Spotify Web API.
/// Token management reads token JSON from DB and persists refreshed tokens.
/// Endpoints may be overridden by SPOTIFY_AUTH_BASE and SPOTIFY_API_BASE env vars (useful for tests).
pub struct SpotifyProvider {
    client: Client,
    client_id: String,
    client_secret: String,
    db_path: std::path::PathBuf,
    config: crate::config::Config,
    token: tokio::sync::Mutex<Option<StoredToken>>,
    user_id: tokio::sync::Mutex<Option<String>>,
    /// Cached result of `list_user_playlists()` so we only fetch the full
    /// library once per worker run instead of once per playlist.
    playlist_cache: tokio::sync::Mutex<Option<Vec<(String, String)>>>,
}

impl SpotifyProvider {
    /// Check whether the current user still has access to the given
    /// playlist id. This is used to detect the case where a playlist
    /// was "deleted" (unfollowed) in the Spotify client while our
    /// local mapping still points at the old id.
    async fn playlist_is_accessible(&self, playlist_id: &str) -> Result<Option<String>> {
        // Check whether the playlist is still visible in the user's library.
        let playlists = self.list_user_playlists().await?;
        let found = playlists.iter().find(|(id, _name)| id == playlist_id);
        let current_name = match found {
            Some((_id, name)) => name.clone(),
            None => {
                debug!(
                    "Spotify playlist {} no longer present in user library; treating mapping as invalid",
                    playlist_id
                );
                return Ok(None);
            }
        };

        // Extra safety: confirm accessibility via the single-playlist endpoint.
        // execute_request handles 401 (token refresh) and 429 (rate limit) automatically.
        let url = format!("{}/playlists/{}", Self::api_base(), playlist_id);
        let resp = self
            .execute_request("playlist_is_accessible", &RequestSpec::get(&url))
            .await?;
        let status = resp.status();
        if status.is_success() {
            return Ok(Some(current_name));
        }
        if status == reqwest::StatusCode::NOT_FOUND || status == reqwest::StatusCode::FORBIDDEN {
            debug!(
                "Spotify playlist {} not accessible (status {}); treating as invalid",
                playlist_id, status
            );
            return Ok(None);
        }
        Err(anyhow!("playlist_is_accessible failed: {}", status))
    }
    /// List all track URIs for a given Spotify playlist.
    async fn list_playlist_tracks_internal(&self, playlist_id: &str) -> Result<Vec<String>> {
        let mut uris = Vec::new();
        let mut next: Option<String> = Some(format!(
            "{}/playlists/{}/tracks?fields=items(track(uri)),next&limit=100",
            Self::api_base(),
            playlist_id
        ));

        while let Some(url) = next {
            let resp = self
                .execute_request("list_playlist_tracks", &RequestSpec::get(&url))
                .await?;
            let status = resp.status();
            if !status.is_success() {
                let txt = resp.text().await.unwrap_or_default();
                return Err(anyhow!(
                    "list playlist tracks failed: {} => {}",
                    status,
                    txt
                ));
            }
            let j: serde_json::Value = resp.json().await?;
            if let Some(items) = j["items"].as_array() {
                for it in items {
                    if let Some(uri) = it["track"]["uri"].as_str() {
                        uris.push(uri.to_string());
                    }
                }
            }
            next = j["next"].as_str().map(|s| s.to_string());
        }

        // Deduplicate while preserving order.
        let mut seen = std::collections::HashSet::new();
        uris.retain(|u| seen.insert(u.clone()));
        Ok(uris)
    }
    /// List all playlists for the authenticated user.
    /// Results are cached for the lifetime of the provider instance so that
    /// multiple callers within a single worker run do not re-fetch the entire
    /// Spotify library each time.
    pub async fn list_user_playlists(&self) -> Result<Vec<(String, String)>> {
        // Fast path: return cached result if available.
        {
            let cache = self.playlist_cache.lock().await;
            if let Some(ref cached) = *cache {
                return Ok(cached.clone());
            }
        }
        // DB cache: try the persisted list to avoid re-fetching on every process restart.
        {
            let db_path = self.db_path.clone();
            let cached = tokio::task::spawn_blocking(move || {
                let conn = rusqlite::Connection::open(&db_path)?;
                crate::db::get_provider_playlist_list_cache(&conn, "spotify")
            })
            .await??;
            if let Some(entries) = cached {
                debug!(
                    "list_user_playlists: loaded {} playlists from DB cache",
                    entries.len()
                );
                let mut cache = self.playlist_cache.lock().await;
                *cache = Some(entries.clone());
                return Ok(entries);
            }
        }

        let user_id = self.get_user_id().await?;
        let mut playlists = Vec::new();
        let mut next_url = Some(format!(
            "{}/users/{}/playlists?limit=50",
            Self::api_base(),
            url::form_urlencoded::byte_serialize(user_id.as_bytes()).collect::<String>()
        ));
        while let Some(url) = next_url {
            let resp = self
                .execute_request("list_user_playlists", &RequestSpec::get(&url))
                .await?;
            let status = resp.status();
            if !status.is_success() {
                let txt = resp.text().await.unwrap_or_default();
                return Err(anyhow!("list playlists failed: {} => {}", status, txt));
            }
            let j: serde_json::Value = resp.json().await?;
            if let Some(items) = j["items"].as_array() {
                for pl in items {
                    let name = pl["name"].as_str().unwrap_or("").to_string();
                    let id = pl["id"].as_str().unwrap_or("").to_string();
                    playlists.push((id, name));
                }
            }
            next_url = j["next"].as_str().map(|s| s.to_string());
        }

        // Store in cache so subsequent calls within this worker run are free.
        {
            let mut cache = self.playlist_cache.lock().await;
            *cache = Some(playlists.clone());
        }
        // Persist to DB so subsequent process starts skip re-fetching the library.
        {
            let db_path = self.db_path.clone();
            let to_save = playlists.clone();
            let _ = tokio::task::spawn_blocking(move || {
                let conn = rusqlite::Connection::open(&db_path)?;
                crate::db::upsert_provider_playlist_list_cache(&conn, "spotify", &to_save)
            })
            .await;
        }
        Ok(playlists)
    }

    /// Update the cached name for an existing playlist after a rename,
    /// avoiding a full re-fetch of the user's library.
    async fn cache_update_name(&self, playlist_id: &str, new_name: &str) {
        // If in-memory cache is populated, update it and persist the full
        // entries list as before. If not, operate directly on the DB cache
        // (read-modify-write) so we don't fetch the live provider list.
        let mut did_update_db = false;
        {
            let mut cache = self.playlist_cache.lock().await;
            if let Some(ref mut entries) = *cache {
                if let Some(entry) = entries.iter_mut().find(|(id, _)| id == playlist_id) {
                    entry.1 = new_name.to_string();
                } else {
                    // If the id wasn't present in the in-memory cache, add it.
                    entries.push((playlist_id.to_string(), new_name.to_string()));
                }
                let entries_clone = entries.clone();
                let db_path = self.db_path.clone();
                let _ = tokio::task::spawn_blocking(move || {
                    let conn = rusqlite::Connection::open(&db_path)?;
                    crate::db::upsert_provider_playlist_list_cache(&conn, "spotify", &entries_clone)
                })
                .await;
                did_update_db = true;
            }
        }

        if !did_update_db {
            // DB-only path: read existing DB cache (if any), update or create
            // a minimal entries vector and write it back.
            let db_path = self.db_path.clone();
            let pid = playlist_id.to_string();
            let nm = new_name.to_string();
            let res = tokio::task::spawn_blocking(move || -> Result<(), anyhow::Error> {
                let conn = rusqlite::Connection::open(&db_path)?;
                if let Ok(Some(mut entries)) = crate::db::get_provider_playlist_list_cache(&conn, "spotify") {
                    if let Some(e) = entries.iter_mut().find(|(id, _)| id == &pid) {
                        e.1 = nm.clone();
                    } else {
                        entries.push((pid.clone(), nm.clone()));
                    }
                    crate::db::upsert_provider_playlist_list_cache(&conn, "spotify", &entries)?;
                } else {
                    // No DB cache row: create a minimal one containing this single entry
                    let entries = vec![(pid.clone(), nm.clone())];
                    crate::db::upsert_provider_playlist_list_cache(&conn, "spotify", &entries)?;
                }
                Ok(())
            })
            .await;
            if let Err(e) = res {
                log::warn!("spotify cache_update_name: DB write failed: {}", e);
            }
        }
    }

    /// Add a newly-created playlist to the cache so that subsequent calls
    /// to `list_user_playlists` see it without a round-trip.
    async fn cache_add_entry(&self, playlist_id: &str, name: &str) {
        // Same strategy as for update: if in-memory cache exists update+persist
        // the full list. Otherwise modify the DB cache (or create a minimal
        // DB cache row) without fetching the live provider list.
        let mut did_update_db = false;
        {
            let mut cache = self.playlist_cache.lock().await;
            if let Some(ref mut entries) = *cache {
                entries.push((playlist_id.to_string(), name.to_string()));
                let entries_clone = entries.clone();
                let db_path = self.db_path.clone();
                let _ = tokio::task::spawn_blocking(move || {
                    let conn = rusqlite::Connection::open(&db_path)?;
                    crate::db::upsert_provider_playlist_list_cache(&conn, "spotify", &entries_clone)
                })
                .await;
                did_update_db = true;
            }
        }

        if !did_update_db {
            let db_path = self.db_path.clone();
            let id = playlist_id.to_string();
            let nm = name.to_string();
            let res = tokio::task::spawn_blocking(move || -> Result<(), anyhow::Error> {
                let conn = rusqlite::Connection::open(&db_path)?;
                if let Ok(Some(mut entries)) = crate::db::get_provider_playlist_list_cache(&conn, "spotify") {
                    entries.push((id.clone(), nm.clone()));
                    crate::db::upsert_provider_playlist_list_cache(&conn, "spotify", &entries)?;
                } else {
                    let entries = vec![(id.clone(), nm.clone())];
                    crate::db::upsert_provider_playlist_list_cache(&conn, "spotify", &entries)?;
                }
                Ok(())
            })
            .await;
            if let Err(e) = res {
                log::warn!("spotify cache_add_entry: DB write failed: {}", e);
            }
        }
    }

    /// Remove a deleted playlist from the cache.
    async fn cache_remove_entry(&self, playlist_id: &str) {
        // Remove from in-memory cache if present; otherwise perform a
        // DB-only read-modify-write so the persisted provider cache reflects
        // the removal without warming from the remote API.
        let mut did_update_db = false;
        {
            let mut cache = self.playlist_cache.lock().await;
            if let Some(ref mut entries) = *cache {
                let before = entries.len();
                entries.retain(|(id, _)| id != playlist_id);
                if entries.len() != before {
                    let entries_clone = entries.clone();
                    let db_path = self.db_path.clone();
                    let _ = tokio::task::spawn_blocking(move || {
                        let conn = rusqlite::Connection::open(&db_path)?;
                        crate::db::upsert_provider_playlist_list_cache(&conn, "spotify", &entries_clone)
                    })
                    .await;
                    did_update_db = true;
                }
            }
        }

        if !did_update_db {
            let db_path = self.db_path.clone();
            let pid = playlist_id.to_string();
            let res = tokio::task::spawn_blocking(move || -> Result<(), anyhow::Error> {
                let conn = rusqlite::Connection::open(&db_path)?;
                if let Ok(Some(mut entries)) = crate::db::get_provider_playlist_list_cache(&conn, "spotify") {
                    let before = entries.len();
                    entries.retain(|(id, _)| id != &pid);
                    if entries.len() != before {
                        crate::db::upsert_provider_playlist_list_cache(&conn, "spotify", &entries)?;
                    }
                }
                Ok(())
            })
            .await;
            if let Err(e) = res {
                log::warn!("spotify cache_remove_entry: DB write failed: {}", e);
            }
        }
    }
    pub fn new(
        client_id: String,
        client_secret: String,
        db_path: std::path::PathBuf,
        config: crate::config::Config,
    ) -> Self {
        // If either client_id or client_secret is empty, try to load from DB
        let (client_id, client_secret) = if client_id.is_empty() || client_secret.is_empty() {
            if let Ok(conn) = rusqlite::Connection::open(&db_path) {
                if let Ok(Some((_token_json, db_client_id, db_client_secret))) =
                    crate::db::load_credential_with_client(&conn, "spotify")
                {
                    (
                        db_client_id.unwrap_or(client_id),
                        db_client_secret.unwrap_or(client_secret),
                    )
                } else {
                    (client_id, client_secret)
                }
            } else {
                (client_id, client_secret)
            }
        } else {
            (client_id, client_secret)
        };
        Self {
            client: Client::new(),
            client_id,
            client_secret,
            db_path,
            config,
            token: tokio::sync::Mutex::new(None),
            user_id: tokio::sync::Mutex::new(None),
            playlist_cache: tokio::sync::Mutex::new(None),
        }
    }
    fn is_authenticated(&self) -> bool {
        !self.client_id.is_empty() && !self.client_secret.is_empty()
    }
    fn name(&self) -> &str {
        "spotify"
    }

    /// Return the currently configured client credentials (for tests).
    pub fn creds(&self) -> (&str, &str) {
        (self.client_id.as_str(), self.client_secret.as_str())
    }

    fn auth_base() -> String {
        env::var("SPOTIFY_AUTH_BASE").unwrap_or_else(|_| "https://accounts.spotify.com".into())
    }
    fn api_base() -> String {
        // include v1 path by default
        env::var("SPOTIFY_API_BASE").unwrap_or_else(|_| "https://api.spotify.com/v1".into())
    }

    async fn load_token_from_db(&self) -> Result<Option<StoredToken>> {
        let db_path = self.db_path.clone();
        let json_opt =
            tokio::task::spawn_blocking(move || -> Result<Option<String>, anyhow::Error> {
                let conn = rusqlite::Connection::open(db_path)?;
                Ok(crate::db::load_credential_with_client(&conn, "spotify")?
                    .map(|(json, _, _)| json))
            })
            .await??;

        if let Some(s) = json_opt {
            let st: StoredToken =
                serde_json::from_str(&s).map_err(|e| anyhow!("parse token json: {}", e))?;
            Ok(Some(st))
        } else {
            Ok(None)
        }
    }

    async fn persist_token_to_db(&self, st: &StoredToken) -> Result<()> {
        let db_path = self.db_path.clone();
        let s = serde_json::to_string(&st)?;
        // Pass the client credentials explicitly so the UPSERT does not
        // overwrite them with NULL and wipe them from the DB on every refresh.
        let client_id = self.client_id.clone();
        let client_secret = self.client_secret.clone();
        tokio::task::spawn_blocking(move || -> Result<(), anyhow::Error> {
            let conn = rusqlite::Connection::open(db_path)?;
            db::save_credential_raw(&conn, "spotify", &s, Some(&client_id), Some(&client_secret))?;
            Ok(())
        })
        .await??;
        Ok(())
    }

    async fn ensure_token(&self) -> Result<()> {
        let mut lock = self.token.lock().await;
        if lock.is_none() {
            if let Some(st) = self.load_token_from_db().await? {
                *lock = Some(st);
            }
        }
        if let Some(st) = &*lock {
            let now = Utc::now().timestamp();
            if now + 30 >= st.expires_at {
                debug!("Spotify token is near expiry, refreshing");
                // clone so we can update persisted token in refresh
                let mut cur = st.clone();
                self.refresh_token_internal(&mut cur).await?;
                *lock = Some(cur);
            }
        }
        Ok(())
    }

    async fn refresh_token_internal(&self, cur: &mut StoredToken) -> Result<()> {
        let refresh_token = cur
            .refresh_token
            .clone()
            .ok_or_else(|| anyhow!("no refresh token"))?;
        let params = [
            ("grant_type", "refresh_token"),
            ("refresh_token", &refresh_token),
        ];
        let auth_header = format!(
            "Basic {}",
            general_purpose::STANDARD.encode(format!("{}:{}", self.client_id, self.client_secret))
        );
        let url = format!("{}/api/token", Self::auth_base());
        let resp = self
            .client
            .post(&url)
            .header(AUTHORIZATION, auth_header)
            .form(&params)
            .send()
            .await?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(anyhow!("Failed to refresh token: {} - {}", status, body));
        }
        let j: serde_json::Value = resp.json().await?;
        let access_token = j["access_token"]
            .as_str()
            .ok_or_else(|| anyhow!("no access_token"))?
            .to_string();
        let expires_in = j["expires_in"].as_i64().unwrap_or(3600);
        let scope = j["scope"].as_str().map(|s| s.to_string());
        cur.access_token = access_token;
        cur.token_type = "Bearer".into();
        cur.expires_at = Utc::now().timestamp() + expires_in;
        if let Some(s) = scope {
            cur.scope = Some(s);
        }
        self.persist_token_to_db(cur).await?;
        Ok(())
    }

    pub async fn get_bearer(&self) -> Result<String> {
        self.ensure_token().await?;
        let lock = self.token.lock().await;
        let st = lock.as_ref().ok_or_else(|| anyhow!("no token loaded"))?;
        Ok(format!("Bearer {}", st.access_token))
    }

    async fn get_user_id(&self) -> Result<String> {
        {
            let g = self.user_id.lock().await;
            if let Some(u) = g.as_ref() {
                return Ok(u.clone());
            }
        }
        let url = format!("{}/me", Self::api_base());
        let resp = self
            .execute_request("get_user_id", &RequestSpec::get(&url))
            .await?;
        if !resp.status().is_success() {
            return Err(anyhow!("failed to fetch /me: {}", resp.status()));
        }
        let j: serde_json::Value = resp.json().await?;
        let id = j["id"]
            .as_str()
            .ok_or_else(|| anyhow!("no id"))?
            .to_string();
        let mut g = self.user_id.lock().await;
        *g = Some(id.clone());
        Ok(id)
    }
}

#[async_trait]
impl Provider for SpotifyProvider {
    fn config(&self) -> &crate::config::Config {
        &self.config
    }
    fn http_client(&self) -> &reqwest::Client {
        &self.client
    }
    async fn get_bearer(&self) -> Result<String> {
        SpotifyProvider::get_bearer(self).await
    }
    async fn refresh_token(&self) -> Result<()> {
        // Force-refresh the access token regardless of expiry, e.g. after a 401.
        let mut lock = self.token.lock().await;
        if lock.is_none() {
            if let Some(st) = self.load_token_from_db().await? {
                *lock = Some(st);
            }
        }
        if let Some(st) = &*lock {
            let mut cur = st.clone();
            self.refresh_token_internal(&mut cur).await?;
            *lock = Some(cur);
        }
        Ok(())
    }
    fn name(&self) -> &str {
        SpotifyProvider::name(self)
    }
    fn is_authenticated(&self) -> bool {
        SpotifyProvider::is_authenticated(self)
    }
    async fn ensure_playlist(&self, name: &str, description: &str) -> Result<String> {
        // Before creating a new playlist, check whether the user already owns
        // one with this exact name. This keeps Spotify behavior aligned with
        // TIDAL and prevents duplicates when the local mapping is missing or
        // stale but the remote playlist still exists.
        match self.list_user_playlists().await {
            Ok(playlists) => {
                if let Some((existing_id, _)) = playlists.iter().find(|(_id, n)| n == name) {
                    log::info!(
                        "SpotifyProvider ensure_playlist: found existing playlist '{}' with id {}",
                        name,
                        existing_id
                    );
                    return Ok(existing_id.clone());
                }
            }
            Err(e) => {
                // Non-fatal: if listing fails we fall through to create.
                log::warn!(
                    "SpotifyProvider ensure_playlist: could not list playlists to check for '{}': {}",
                    name,
                    e
                );
            }
        }

        let user_id = self.get_user_id().await?;
        let url = format!(
            "{}/users/{}/playlists",
            Self::api_base(),
            url::form_urlencoded::byte_serialize(user_id.as_bytes()).collect::<String>()
        );
        let body = json!({
            "name": name,
            "description": description,
            "public": false
        });
        let spec = RequestSpec::post(&url)
            .json(body)
            .header("content-type", "application/json");
        let resp = self.execute_request("ensure_playlist", &spec).await?;
        let status = resp.status();
        if !status.is_success() {
            let txt = resp.text().await.unwrap_or_default();
            return Err(anyhow!("create playlist failed: {} => {}", status, txt));
        }
        let j: serde_json::Value = resp.json().await?;
        let id = j["id"]
            .as_str()
            .ok_or_else(|| anyhow!("no id"))?
            .to_string();

        // A new playlist was created – add it to the cache.
        self.cache_add_entry(&id, name).await;

        Ok(id)
    }

    async fn rename_playlist(&self, playlist_id: &str, new_name: &str) -> Result<()> {
        let url = format!("{}/playlists/{}", Self::api_base(), playlist_id);
        let body = json!({ "name": new_name });
        let resp = self
            .execute_request("rename_playlist", &RequestSpec::put(&url).json(body))
            .await?;
        if !resp.status().is_success() {
            return Err(anyhow!("rename failed: {}", resp.status()));
        }
        self.cache_update_name(playlist_id, new_name).await;
        Ok(())
    }

    async fn add_tracks(&self, playlist_id: &str, uris: &[String]) -> Result<()> {
        let url = format!("{}/playlists/{}/tracks", Self::api_base(), playlist_id);
        let body = json!({ "uris": uris });
        let resp = self
            .execute_request("add_tracks", &RequestSpec::post(&url).json(body))
            .await?;
        let status = resp.status();
        if !status.is_success() {
            let txt = resp.text().await.unwrap_or_default();
            return Err(anyhow!("add tracks failed: {} => {}", status, txt));
        }
        Ok(())
    }

    async fn remove_tracks(&self, playlist_id: &str, uris: &[String]) -> Result<()> {
        let url = format!("{}/playlists/{}/tracks", Self::api_base(), playlist_id);
        let tracks: Vec<serde_json::Value> = uris.iter().map(|u| json!({ "uri": u })).collect();
        let body = json!({ "tracks": tracks });
        let resp = self
            .execute_request("remove_tracks", &RequestSpec::delete(&url).json(body))
            .await?;
        let status = resp.status();
        if !status.is_success() {
            let txt = resp.text().await.unwrap_or_default();
            return Err(anyhow!("remove tracks failed: {} => {}", status, txt));
        }
        Ok(())
    }

    async fn delete_playlist(&self, playlist_id: &str) -> Result<()> {
        // Spotify does not support hard-deleting playlists; instead, the
        // current user "unfollows" the playlist (DELETE /playlists/{id}/followers).
        let url = format!("{}/playlists/{}/followers", Self::api_base(), playlist_id);
        let resp = self
            .execute_request("delete_playlist", &RequestSpec::delete(&url))
            .await?;
        if !resp.status().is_success() {
            return Err(anyhow!("delete playlist failed: {}", resp.status()));
        }
        self.cache_remove_entry(playlist_id).await;
        Ok(())
    }

    async fn playlist_is_valid(&self, playlist_id: &str) -> Result<Option<String>> {
        self.playlist_is_accessible(playlist_id).await
    }

    async fn invalidate_playlist_list_cache(&self, playlist_id: &str) {
        // Remove only the dead entry from the in-memory cache. If the cache is
        // cold, warm it from the DB so we can surgically remove one stale id.
        let pid = playlist_id.to_string();
        let db_path = self.db_path.clone();
        let updated = {
            let mut cache = self.playlist_cache.lock().await;
            if cache.is_none() {
                let loaded = tokio::task::spawn_blocking(move || {
                    let conn = rusqlite::Connection::open(&db_path)?;
                    crate::db::get_provider_playlist_list_cache(&conn, "spotify")
                })
                .await
                .ok()
                .and_then(|r| r.ok())
                .flatten();
                *cache = loaded;
            }
            if let Some(ref mut entries) = *cache {
                entries.retain(|(id, _)| id != &pid);
                Some(entries.clone())
            } else {
                None
            }
        };

        let db_path = self.db_path.clone();
        let pid2 = playlist_id.to_string();
        let _ = tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
            let conn = rusqlite::Connection::open(&db_path)?;
            match updated {
                Some(ref entries) => {
                    crate::db::upsert_provider_playlist_list_cache(&conn, "spotify", entries)?;
                }
                None => {
                    crate::db::delete_provider_playlist_list_cache(&conn, "spotify")?;
                }
            }
            log::debug!(
                "invalidate_playlist_list_cache: removed {} from spotify playlist cache",
                pid2
            );
            Ok(())
        })
        .await;
    }

    async fn list_playlist_tracks(&self, playlist_id: &str) -> Result<Vec<String>> {
        self.list_playlist_tracks_internal(playlist_id).await
    }

    async fn search_track_uri(&self, title: &str, artist: &str) -> Result<Option<String>> {
        let q = format!("track:{} artist:{}", title, artist);
        let url = format!(
            "{}/search?q={}&type=track&limit=1",
            Self::api_base(),
            urlencoding::encode(&q)
        );
        let spec = RequestSpec::get(&url).header("accept", "application/json");
        let resp = self.execute_request("search_track_uri", &spec).await?;
        if !resp.status().is_success() {
            return Ok(None);
        }
        let j: serde_json::Value = resp.json().await?;
        if let Some(first) = j["tracks"]["items"].as_array().and_then(|a| a.get(0)) {
            if let Some(uri) = first["uri"].as_str() {
                return Ok(Some(uri.to_string()));
            }
        }
        Ok(None)
    }

    async fn search_track_uri_by_isrc(&self, isrc: &str) -> Result<Option<String>> {
        let q = format!("isrc:{}", isrc);
        let url = format!(
            "{}/search?q={}&type=track&limit=1",
            Self::api_base(),
            urlencoding::encode(&q)
        );
        let spec = RequestSpec::get(&url).header("accept", "application/json");
        let resp = self
            .execute_request("search_track_uri_by_isrc", &spec)
            .await?;
        if !resp.status().is_success() {
            return Ok(None);
        }
        let j: serde_json::Value = resp.json().await?;
        if let Some(first) = j["tracks"]["items"].as_array().and_then(|a| a.get(0)) {
            if let Some(uri) = first["uri"].as_str() {
                return Ok(Some(uri.to_string()));
            }
        }
        Ok(None)
    }

    async fn lookup_track_isrc(&self, uri: &str) -> Result<Option<String>> {
        // Expect URIs like "spotify:track:{id}" or full spotify track URLs; extract id
        let id = if let Some(i) = uri.rsplit(':').next() {
            i.to_string()
        } else {
            // try to parse last path segment
            uri.rsplit('/').next().unwrap_or("").to_string()
        };
        if id.is_empty() {
            return Ok(None);
        }
        let url = format!("{}/tracks/{}", Self::api_base(), id);
        let resp = self
            .execute_request("lookup_track_isrc", &RequestSpec::get(&url))
            .await?;
        if !resp.status().is_success() {
            return Ok(None);
        }
        let j: serde_json::Value = resp.json().await?;
        if let Some(isrc) = j
            .get("external_ids")
            .and_then(|e| e.get("isrc"))
            .and_then(|v| v.as_str())
        {
            return Ok(Some(isrc.to_string()));
        }
        Ok(None)
    }
}
