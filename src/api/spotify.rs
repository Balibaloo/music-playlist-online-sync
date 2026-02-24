use super::Provider;
use crate::db;
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use base64::{engine::general_purpose, Engine as _};
use chrono::Utc;
use log::{debug, warn};
use reqwest::header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE};
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

/// Spotify provider backed by Spotify Web API.
/// Token management reads token JSON from DB and persists refreshed tokens.
/// Endpoints may be overridden by SPOTIFY_AUTH_BASE and SPOTIFY_API_BASE env vars (useful for tests).
pub struct SpotifyProvider {
    client: Client,
    client_id: String,
    client_secret: String,
    db_path: std::path::PathBuf,
    token: tokio::sync::Mutex<Option<StoredToken>>,
    user_id: tokio::sync::Mutex<Option<String>>,
}

impl SpotifyProvider {
    /// Check whether the current user still has access to the given
    /// playlist id. This is used to detect the case where a playlist
    /// was "deleted" (unfollowed) in the Spotify client while our
    /// local mapping still points at the old id.
    async fn playlist_is_accessible(&self, playlist_id: &str) -> Result<bool> {
        // First, check whether the playlist id is still visible in the
        // current user's playlist library. This matches what the user sees
        // in the Spotify UI: if they've "deleted" (unfollowed) the playlist,
        // it will no longer appear in /users/{id}/playlists.
        let playlists = self.list_user_playlists().await?;
        let in_library = playlists.iter().any(|(id, _name)| id == playlist_id);
        if !in_library {
            debug!(
                "Spotify playlist {} no longer present in user library; treating mapping as invalid",
                playlist_id
            );
            return Ok(false);
        }

        // As an extra safety check, confirm the playlist is still accessible
        // via the generic playlist endpoint.
        let bearer = self.get_bearer().await?;
        let url = format!("{}/playlists/{}", Self::api_base(), playlist_id);
        let resp = self
            .client
            .get(&url)
            .header(AUTHORIZATION, &bearer)
            .send()
            .await?;
        let status = resp.status();
        if status.is_success() {
            return Ok(true);
        }
        if status.as_u16() == 401 {
            // Try once more after refreshing token.
            self.ensure_token().await?;
            let bearer2 = self.get_bearer().await?;
            let resp2 = self
                .client
                .get(&url)
                .header(AUTHORIZATION, &bearer2)
                .send()
                .await?;
            let st2 = resp2.status();
            if st2.is_success() {
                return Ok(true);
            }
            // 404/403 after refresh -> treat as invalid mapping.
            if st2 == reqwest::StatusCode::NOT_FOUND || st2 == reqwest::StatusCode::FORBIDDEN {
                debug!(
                    "Spotify playlist {} not accessible after refresh (status {}); treating as invalid",
                    playlist_id,
                    st2
                );
                return Ok(false);
            }
            return Err(anyhow!(
                "playlist_is_accessible failed after refresh: {}",
                st2
            ));
        }

        // 404/403 without needing refresh means the playlist either no
        // longer exists or the user no longer has access to it.
        if status == reqwest::StatusCode::NOT_FOUND || status == reqwest::StatusCode::FORBIDDEN {
            debug!(
                "Spotify playlist {} not accessible (status {}); treating as invalid",
                playlist_id, status
            );
            return Ok(false);
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
            let bearer = self.get_bearer().await?;
            let resp = self
                .client
                .get(&url)
                .header(AUTHORIZATION, &bearer)
                .send()
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
    /// List all playlists for the authenticated user
    pub async fn list_user_playlists(&self) -> Result<Vec<(String, String)>> {
        let user_id = self.get_user_id().await?;
        let bearer = self.get_bearer().await?;
        let mut playlists = Vec::new();
        let mut next_url = Some(format!(
            "{}/users/{}/playlists?limit=50",
            Self::api_base(),
            url::form_urlencoded::byte_serialize(user_id.as_bytes()).collect::<String>()
        ));
        while let Some(url) = next_url {
            let resp = self
                .client
                .get(&url)
                .header(AUTHORIZATION, &bearer)
                .send()
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
        Ok(playlists)
    }
    pub fn new(client_id: String, client_secret: String, db_path: std::path::PathBuf) -> Self {
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
            token: tokio::sync::Mutex::new(None),
            user_id: tokio::sync::Mutex::new(None),
        }
    }
    fn is_authenticated(&self) -> bool {
        !self.client_id.is_empty() && !self.client_secret.is_empty()
    }
    fn name(&self) -> &str {
        "spotify"
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
        tokio::task::spawn_blocking(move || -> Result<(), anyhow::Error> {
            let conn = rusqlite::Connection::open(db_path)?;
            db::save_credential_raw(&conn, "spotify", &s, None, None)?;
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

    async fn get_bearer(&self) -> Result<String> {
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
        let bearer = self.get_bearer().await?;
        let url = format!("{}/me", Self::api_base());
        let resp = self
            .client
            .get(&url)
            .header(AUTHORIZATION, &bearer)
            .send()
            .await?;
        if resp.status() == reqwest::StatusCode::UNAUTHORIZED {
            warn!("Got 401 when fetching /me; attempting token refresh");
            self.ensure_token().await?;
            let bearer2 = self.get_bearer().await?;
            let resp2 = self
                .client
                .get(&url)
                .header(AUTHORIZATION, &bearer2)
                .send()
                .await?;
            if !resp2.status().is_success() {
                return Err(anyhow!("failed to fetch /me: {}", resp2.status()));
            }
            let j: serde_json::Value = resp2.json().await?;
            let id = j["id"]
                .as_str()
                .ok_or_else(|| anyhow!("no id"))?
                .to_string();
            let mut g = self.user_id.lock().await;
            *g = Some(id.clone());
            return Ok(id);
        }
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
    fn name(&self) -> &str {
        SpotifyProvider::name(self)
    }
    fn is_authenticated(&self) -> bool {
        SpotifyProvider::is_authenticated(self)
    }
    async fn ensure_playlist(&self, name: &str, description: &str) -> Result<String> {
        let user_id = self.get_user_id().await?;
        let bearer = self.get_bearer().await?;
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
        let resp = self
            .client
            .post(&url)
            .header(AUTHORIZATION, &bearer)
            .header(CONTENT_TYPE, "application/json")
            .json(&body)
            .send()
            .await?;
        if resp.status().as_u16() == 401 {
            self.ensure_token().await?;
            let bearer2 = self.get_bearer().await?;
            let resp2 = self
                .client
                .post(&url)
                .header(AUTHORIZATION, &bearer2)
                .header(CONTENT_TYPE, "application/json")
                .json(&body)
                .send()
                .await?;
            if !resp2.status().is_success() {
                return Err(anyhow!("create playlist failed: {}", resp2.status()));
            }
            let j: serde_json::Value = resp2.json().await?;
            let id = j["id"]
                .as_str()
                .ok_or_else(|| anyhow!("no id"))?
                .to_string();
            return Ok(id);
        }
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
        Ok(id)
    }

    async fn rename_playlist(&self, playlist_id: &str, new_name: &str) -> Result<()> {
        let url = format!("{}/playlists/{}", Self::api_base(), playlist_id);
        let body = json!({ "name": new_name });
        let mut attempt: u32 = 0;
        loop {
            attempt += 1;
            let bearer = self.get_bearer().await?;
            let resp = self
                .client
                .put(&url)
                .header(AUTHORIZATION, &bearer)
                .json(&body)
                .send()
                .await?;
            let status = resp.status();

            if status.as_u16() == 401 && attempt == 1 {
                // Refresh token once on 401, then retry.
                self.ensure_token().await?;
                continue;
            }

            if status == reqwest::StatusCode::TOO_MANY_REQUESTS && attempt <= 3 {
                let retry_after = resp
                    .headers()
                    .get("retry-after")
                    .and_then(|v| v.to_str().ok())
                    .and_then(|s| s.parse::<u64>().ok())
                    .unwrap_or(2);
                tokio::time::sleep(std::time::Duration::from_secs(retry_after + 1)).await;
                continue;
            }

            if !status.is_success() {
                return Err(anyhow!("rename failed: {}", status));
            }
            return Ok(());
        }
    }

    async fn add_tracks(&self, playlist_id: &str, uris: &[String]) -> Result<()> {
        let bearer = self.get_bearer().await?;
        let url = format!("{}/playlists/{}/tracks", Self::api_base(), playlist_id);
        let body = json!({ "uris": uris });
        let resp = self
            .client
            .post(&url)
            .header(AUTHORIZATION, &bearer)
            .json(&body)
            .send()
            .await?;
        if resp.status().as_u16() == 401 {
            self.ensure_token().await?;
            let bearer2 = self.get_bearer().await?;
            let resp2 = self
                .client
                .post(&url)
                .header(AUTHORIZATION, &bearer2)
                .json(&body)
                .send()
                .await?;
            if !resp2.status().is_success() {
                return Err(anyhow!("add tracks failed: {}", resp2.status()));
            }
            return Ok(());
        }
        if resp.status() == reqwest::StatusCode::TOO_MANY_REQUESTS {
            let retry_after = resp
                .headers()
                .get("retry-after")
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.parse::<u64>().ok());
            return Err(anyhow!("rate_limited: retry_after={:?}", retry_after));
        }
        let status = resp.status();
        if !status.is_success() {
            let txt = resp.text().await.unwrap_or_default();
            return Err(anyhow!("add tracks failed: {} => {}", status, txt));
        }
        Ok(())
    }

    async fn remove_tracks(&self, playlist_id: &str, uris: &[String]) -> Result<()> {
        let bearer = self.get_bearer().await?;
        let url = format!("{}/playlists/{}/tracks", Self::api_base(), playlist_id);
        let tracks: Vec<serde_json::Value> = uris.iter().map(|u| json!({ "uri": u })).collect();
        let body = json!({ "tracks": tracks });
        let resp = self
            .client
            .delete(&url)
            .header(AUTHORIZATION, &bearer)
            .json(&body)
            .send()
            .await?;
        if resp.status().as_u16() == 401 {
            self.ensure_token().await?;
            let bearer2 = self.get_bearer().await?;
            let resp2 = self
                .client
                .delete(&url)
                .header(AUTHORIZATION, &bearer2)
                .json(&body)
                .send()
                .await?;
            if !resp2.status().is_success() {
                return Err(anyhow!("remove tracks failed: {}", resp2.status()));
            }
            return Ok(());
        }
        if resp.status() == reqwest::StatusCode::TOO_MANY_REQUESTS {
            let retry_after = resp
                .headers()
                .get("retry-after")
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.parse::<u64>().ok());
            return Err(anyhow!("rate_limited: retry_after={:?}", retry_after));
        }
        let status = resp.status();
        if !status.is_success() {
            let txt = resp.text().await.unwrap_or_default();
            return Err(anyhow!("remove tracks failed: {} => {}", status, txt));
        }
        Ok(())
    }

    async fn delete_playlist(&self, playlist_id: &str) -> Result<()> {
        // Spotify does not support hard-deleting playlists; instead, the
        // current user "unfollows" the playlist, which effectively removes
        // it from their library. The documented endpoint is:
        // DELETE /playlists/{playlist_id}/followers
        let bearer = self.get_bearer().await?;
        let url = format!("{}/playlists/{}/followers", Self::api_base(), playlist_id);
        let resp = self
            .client
            .delete(&url)
            .header(AUTHORIZATION, &bearer)
            .send()
            .await?;
        if resp.status().as_u16() == 401 {
            self.ensure_token().await?;
            let bearer2 = self.get_bearer().await?;
            let resp2 = self
                .client
                .delete(&url)
                .header(AUTHORIZATION, &bearer2)
                .send()
                .await?;
            if !resp2.status().is_success() {
                return Err(anyhow!("delete playlist failed: {}", resp2.status()));
            }
            return Ok(());
        }
        if !resp.status().is_success() {
            return Err(anyhow!("delete playlist failed: {}", resp.status()));
        }
        Ok(())
    }

    async fn playlist_is_valid(&self, playlist_id: &str) -> Result<bool> {
        self.playlist_is_accessible(playlist_id).await
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
        let resp = self
            .client
            .get(&url)
            .header(AUTHORIZATION, &self.get_bearer().await?)
            .header(ACCEPT, "application/json")
            .send()
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

    async fn search_track_uri_by_isrc(&self, isrc: &str) -> Result<Option<String>> {
        let q = format!("isrc:{}", isrc);
        let url = format!(
            "{}/search?q={}&type=track&limit=1",
            Self::api_base(),
            urlencoding::encode(&q)
        );
        let resp = self
            .client
            .get(&url)
            .header(AUTHORIZATION, &self.get_bearer().await?)
            .header(ACCEPT, "application/json")
            .send()
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
        let bearer = self.get_bearer().await?;
        let resp = self
            .client
            .get(&url)
            .header(AUTHORIZATION, &bearer)
            .send()
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
