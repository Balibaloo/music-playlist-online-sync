use super::Provider;
use crate::db;
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use chrono::Utc;
use base64::{engine::general_purpose, Engine as _};
use reqwest::header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::Arc;
use tracing::{debug, info, warn};

#[derive(Clone, Debug, Serialize, Deserialize)]
struct StoredToken {
    access_token: String,
    token_type: String,
    expires_at: i64, // epoch seconds
    refresh_token: Option<String>,
    scope: Option<String>,
}

/// Minimal Tidal provider implementation. It uses a base URL from env var `TIDAL_API_BASE` for
/// easier testing (mockito). Authentication & endpoints may need tweaks depending on your Tidal
/// application details; this is a best-effort implementation using documented endpoints.
pub struct TidalProvider {
    client: Client,
    client_id: String,
    client_secret: String,
    db_path: std::path::PathBuf,
    token: tokio::sync::Mutex<Option<StoredToken>>,
}

impl TidalProvider {
    pub fn new(client_id: String, client_secret: String, db_path: std::path::PathBuf) -> Self {
        Self {
            client: Client::new(),
            client_id,
            client_secret,
            db_path,
            token: tokio::sync::Mutex::new(None),
        }
    }

    fn base_url() -> String {
        std::env::var("TIDAL_API_BASE").unwrap_or_else(|_| "https://api.tidal.com/v1".into())
    }

    fn auth_base() -> String {
        std::env::var("TIDAL_AUTH_BASE").unwrap_or_else(|_| "https://auth.tidal.com".into())
    }

    async fn load_token_from_db(&self) -> Result<Option<StoredToken>> {
        let db_path = self.db_path.clone();
        let json_opt = tokio::task::spawn_blocking(move || -> Result<Option<String>, anyhow::Error> {
            let conn = rusqlite::Connection::open(db_path)?;
            db::load_credential_raw(&conn, "tidal").map_err(|e| e.into())
        })
        .await??;

        if let Some(s) = json_opt {
            let st: StoredToken = serde_json::from_str(&s)?;
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
            db::save_credential_raw(&conn, "tidal", &s)?;
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
        // If token is near expiry, refresh if we have a refresh token
        if let Some(st) = &*lock {
            let now = Utc::now().timestamp();
            if now + 30 >= st.expires_at {
                debug!("Tidal token near expiry, attempting refresh");
                // attempt refresh if refresh_token present
                let mut cur = st.clone();
                if let Err(e) = self.refresh_token_internal(&mut cur).await {
                    warn!("Tidal token refresh failed: {}", e);
                } else {
                    *lock = Some(cur);
                }
            }
        }
        Ok(())
    }

    async fn refresh_token_internal(&self, cur: &mut StoredToken) -> Result<()> {
        let refresh_token = cur.refresh_token.clone().ok_or_else(|| anyhow!("no refresh token"))?;
        let params = [
            ("grant_type", "refresh_token"),
            ("refresh_token", &refresh_token),
        ];
        let auth_header = format!("Basic {}", base64::engine::general_purpose::STANDARD.encode(format!("{}:{}", self.client_id, self.client_secret)));
        let url = format!("{}/oauth/token", Self::auth_base());
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
            return Err(anyhow!("Failed to refresh tidal token: {} - {}", status, body));
        }
        let j: serde_json::Value = resp.json().await?;
        let access_token = j["access_token"].as_str().ok_or_else(|| anyhow!("no access_token"))?.to_string();
        let expires_in = j["expires_in"].as_i64().unwrap_or(3600);
        let scope = j["scope"].as_str().map(|s| s.to_string());
        cur.access_token = access_token;
        cur.token_type = "Bearer".into();
        cur.expires_at = Utc::now().timestamp() + expires_in;
        if let Some(s) = scope { cur.scope = Some(s); }
        self.persist_token_to_db(cur).await?;
        Ok(())
    }

    async fn get_bearer(&self) -> Result<String> {
        self.ensure_token().await?;
        let lock = self.token.lock().await;
        let st = lock.as_ref().ok_or_else(|| anyhow!("no tidal token loaded"))?;
        Ok(format!("Bearer {}", st.access_token))
    }
}

#[async_trait]
impl Provider for TidalProvider {
    async fn ensure_playlist(&self, name: &str, description: &str) -> Result<String> {
        // Tidal playlist creation (endpoint may differ; adapt to your access pattern).
        let bearer = self.get_bearer().await?;
        let base = Self::base_url();
        // Example endpoint: POST /playlists
        let url = format!("{}/playlists", base);
        let body = json!({
            "title": name,
            "description": description,
            "public": false,
        });
        let resp = self
            .client
            .post(&url)
            .header(AUTHORIZATION, &bearer)
            .header(CONTENT_TYPE, "application/json")
            .json(&body)
            .send()
            .await?;
        let status = resp.status();
        if !status.is_success() {
            let txt = resp.text().await.unwrap_or_default();
            return Err(anyhow!("tidal create playlist failed: {} => {}", status, txt));
        }
        let j: serde_json::Value = resp.json().await?;
        // Tidal returns id under `uuid` or `id` depending on API; try both
        let id = j.get("uuid").and_then(|v| v.as_str()).or_else(|| j.get("id").and_then(|v| v.as_str())).ok_or_else(|| anyhow!("no playlist id in response"))?;
        Ok(id.to_string())
    }

    async fn rename_playlist(&self, playlist_id: &str, new_name: &str) -> Result<()> {
        let bearer = self.get_bearer().await?;
        let base = Self::base_url();
        let url = format!("{}/playlists/{}", base, playlist_id);
        let body = json!({ "title": new_name });
        let resp = self
            .client
            .put(&url)
            .header(AUTHORIZATION, &bearer)
            .json(&body)
            .send()
            .await?;
        let status = resp.status();
        if !status.is_success() {
            let txt = resp.text().await.unwrap_or_default();
            return Err(anyhow!("tidal rename failed: {} => {}", status, txt));
        }
        Ok(())
    }

    async fn add_tracks(&self, playlist_id: &str, uris: &[String]) -> Result<()> {
        let bearer = self.get_bearer().await?;
        let base = Self::base_url();
        // Tidal add tracks endpoint example: POST /playlists/{id}/items
        let url = format!("{}/playlists/{}/items", base, playlist_id);
        // Tidal may expect track ids rather than URIs; assume we pass a list of trackIds
        // Here, attempt to convert URIs "tidal:track:123" to id "123", else pass raw.
        let tracks: Vec<serde_json::Value> = uris.iter().map(|u| {
            if let Some(s) = u.rsplit(':').next() {
                json!({ "trackId": s })
            } else {
                json!({ "uri": u })
            }
        }).collect();
        let body = json!({ "items": tracks });
        let resp = self
            .client
            .post(&url)
            .header(AUTHORIZATION, &bearer)
            .json(&body)
            .send()
            .await?;
        if resp.status() == reqwest::StatusCode::TOO_MANY_REQUESTS {
            let retry_after = resp.headers().get("retry-after").and_then(|v| v.to_str().ok()).and_then(|s| s.parse::<u64>().ok());
            return Err(anyhow!("rate_limited: retry_after={:?}", retry_after));
        }
        let status = resp.status();
        if !status.is_success() {
            let txt = resp.text().await.unwrap_or_default();
            return Err(anyhow!("tidal add tracks failed: {} => {}", status, txt));
        }
        Ok(())
    }

    async fn remove_tracks(&self, playlist_id: &str, uris: &[String]) -> Result<()> {
        let bearer = self.get_bearer().await?;
        let base = Self::base_url();
        let url = format!("{}/playlists/{}/items", base, playlist_id);
        // Tidal delete may require POST with items to delete. This is example behavior.
        let tracks: Vec<serde_json::Value> = uris.iter().map(|u| {
            if let Some(s) = u.rsplit(':').next() {
                json!({ "trackId": s })
            } else {
                json!({ "uri": u })
            }
        }).collect();
        let body = json!({ "items": tracks });
        let resp = self
            .client
            .delete(&url)
            .header(AUTHORIZATION, &bearer)
            .json(&body)
            .send()
            .await?;
        if resp.status() == reqwest::StatusCode::TOO_MANY_REQUESTS {
            let retry_after = resp.headers().get("retry-after").and_then(|v| v.to_str().ok()).and_then(|s| s.parse::<u64>().ok());
            return Err(anyhow!("rate_limited: retry_after={:?}", retry_after));
        }
        let status = resp.status();
        if !status.is_success() {
            let txt = resp.text().await.unwrap_or_default();
            return Err(anyhow!("tidal remove tracks failed: {} => {}", status, txt));
        }
        Ok(())
    }

    async fn search_track_uri(&self, title: &str, artist: &str) -> Result<Option<String>> {
        let base = Self::base_url();
        let q = format!("{} {}", title, artist);
        let url = format!("{}/search/tracks?query={}&limit=1", base, urlencoding::encode(&q));
        let resp = self.client.get(&url).header(ACCEPT, "application/json").send().await?;
        if !resp.status().is_success() {
            return Ok(None);
        }
        let j: serde_json::Value = resp.json().await?;
        if let Some(items) = j["items"].as_array().and_then(|a| a.get(0)) {
            if let Some(id) = items["id"].as_str() {
                // Return a canonical tidal uri representation
                return Ok(Some(format!("tidal:track:{}", id)));
            }
        }
        Ok(None)
    }
}