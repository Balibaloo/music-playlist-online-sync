use super::Provider;
use crate::db;
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use base64::Engine;
use chrono::Utc;
use log;
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::{HashMap, HashSet};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StoredToken {
    pub access_token: String,
    pub token_type: String,
    pub expires_at: i64, // epoch seconds
    pub refresh_token: Option<String>,
    pub scope: Option<String>,
    pub user_id: Option<i64>,
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
    /// Optional logical root folder name under which this application
    /// should group all created playlists in the user's TIDAL collection.
    root_folder_name: Option<String>,
    /// Cached id of the root userCollectionFolder (if created/found).
    root_folder_id: tokio::sync::Mutex<Option<String>>,
}

impl TidalProvider {
    /// List all playlists for the authenticated user
    pub async fn list_user_playlists(&self) -> Result<Vec<(String, String)>> {
        let bearer = self.get_bearer().await?;
        let base = Self::base_url();

        // Require explicit numeric user id from the stored token; this is
        // provided by the JSON pasted from the TIDAL API reference site.
        let user_id = {
            let lock = self.token.lock().await;
            lock.as_ref().and_then(|t| t.user_id)
        }
        .ok_or_else(|| {
            anyhow!(
                "no user_id in tidal token; re-run Tidal auth with JSON that includes 'user_id'"
            )
        })?;

        let cc = Self::country_code();
        let locale = Self::locale();
        // First page: v2 userCollections with include=playlists, which
        // returns playlist resources in `included` and a relationship with
        // pagination links. Request a higher page size to reduce the
        // number of follow-up requests TIDAL needs to serve.
        let url = format!(
            "{}/userCollections/{}?countryCode={}&locale={}&include=playlists&page[limit]=100",
            base, user_id, cc, locale
        );
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

        use std::collections::HashMap;
        let mut by_id: HashMap<String, String> = HashMap::new();

        if let Some(included) = j["included"].as_array() {
            for item in included {
                if item["type"].as_str() == Some("playlists") {
                    if let Some(id) = item["id"].as_str() {
                        let attrs = &item["attributes"];
                        let name = attrs["name"]
                            .as_str()
                            .or_else(|| attrs["title"].as_str())
                            .unwrap_or("")
                            .to_string();
                        by_id.entry(id.to_string()).or_insert(name);
                    }
                }
            }
        }

        // Follow pagination on the playlists relationship to ensure we see
        // **all** playlists, not just the first page.
        let mut next = j["data"]["relationships"]["playlists"]["links"]["next"]
            .as_str()
            .map(|s| s.to_string());

        while let Some(next_path) = next {
            let rel_url = if next_path.starts_with("http") {
                next_path.clone()
            } else {
                format!("{}{}", base, next_path)
            };
            let resp = self
                .client
                .get(&rel_url)
                .header(AUTHORIZATION, &bearer)
                .send()
                .await?;
            if !resp.status().is_success() {
                break;
            }
            let page: serde_json::Value = resp.json().await?;
            // relationships endpoints return only linkage objects; resolve
            // names by fetching each playlist resource.
            if let Some(items) = page["data"].as_array() {
                for pl in items {
                    if pl["type"].as_str() == Some("playlists") {
                        if let Some(id) = pl["id"].as_str() {
                            let id_s = id.to_string();
                            if !by_id.contains_key(&id_s) {
                                let pl_url =
                                    format!("{}/playlists/{}?countryCode={}", base, id, cc);
                                let pl_resp = self
                                    .client
                                    .get(&pl_url)
                                    .header(AUTHORIZATION, &bearer)
                                    .send()
                                    .await?;
                                if !pl_resp.status().is_success() {
                                    continue;
                                }
                                let pl_json: serde_json::Value = pl_resp.json().await?;
                                let attrs = &pl_json["data"]["attributes"];
                                let name = attrs["name"]
                                    .as_str()
                                    .or_else(|| attrs["title"].as_str())
                                    .unwrap_or("")
                                    .to_string();
                                by_id.insert(id_s, name);
                            }
                        }
                    }
                }
            }
            // For relationship pages, the `next` link is usually at the
            // top-level `links.next`.
            next = page["links"]["next"].as_str().map(|s| s.to_string());
        }

        let mut playlists: Vec<(String, String)> = by_id.into_iter().collect();
        // Stable output order: sort by name for determinism.
        playlists.sort_by(|a, b| a.1.cmp(&b.1));
        Ok(playlists)
    }

    fn country_code() -> String {
        std::env::var("TIDAL_COUNTRY_CODE").unwrap_or_else(|_| "US".into())
    }
    fn locale() -> String {
        std::env::var("TIDAL_LOCALE").unwrap_or_else(|_| "en-US".into())
    }
    pub fn new(
        client_id: String,
        client_secret: String,
        db_path: std::path::PathBuf,
        root_folder_name: Option<String>,
    ) -> Self {
        // If either client_id or client_secret is empty, try to load from DB
        let (client_id, client_secret) = if client_id.is_empty() || client_secret.is_empty() {
            if let Ok(conn) = rusqlite::Connection::open(&db_path) {
                if let Ok(Some((_token_json, db_client_id, db_client_secret))) =
                    crate::db::load_credential_with_client(&conn, "tidal")
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
            root_folder_name,
            root_folder_id: tokio::sync::Mutex::new(None),
        }
    }
    fn is_authenticated(&self) -> bool {
        !self.client_id.is_empty() && !self.client_secret.is_empty()
    }
    fn name(&self) -> &str {
        "tidal"
    }

    fn base_url() -> String {
        // Default to the official TIDAL developer base URL; can be
        // overridden (e.g. for tests) via TIDAL_API_BASE.
        std::env::var("TIDAL_API_BASE").unwrap_or_else(|_| "https://openapi.tidal.com/v2".into())
    }

    fn auth_base() -> String {
        std::env::var("TIDAL_AUTH_BASE").unwrap_or_else(|_| "https://auth.tidal.com".into())
    }

    async fn load_token_from_db(&self) -> Result<Option<StoredToken>> {
        let db_path = self.db_path.clone();
        let json_opt =
            tokio::task::spawn_blocking(move || -> Result<Option<String>, anyhow::Error> {
                let conn = rusqlite::Connection::open(db_path)?;
                Ok(
                    crate::db::load_credential_with_client(&conn, "tidal")?
                        .map(|(json, _, _)| json),
                )
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
        // Pass the client credentials explicitly so the UPSERT does not
        // overwrite them with NULL and wipe them from the DB on every refresh.
        let client_id = self.client_id.clone();
        let client_secret = self.client_secret.clone();
        tokio::task::spawn_blocking(move || -> Result<(), anyhow::Error> {
            let conn = rusqlite::Connection::open(db_path)?;
            db::save_credential_raw(
                &conn,
                "tidal",
                &s,
                Some(&client_id),
                Some(&client_secret),
            )?;
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
                log::debug!("Tidal token near expiry, attempting refresh");
                // attempt refresh if refresh_token present
                let mut cur = st.clone();
                if let Err(e) = self.refresh_token_internal(&mut cur).await {
                    log::warn!("Tidal token refresh failed: {}", e);
                } else {
                    *lock = Some(cur);
                }
            }
        }
        Ok(())
    }

    /// Force a token refresh using the stored refresh_token.
    ///
    /// This is primarily intended for the `AuthTest Tidal` CLI helper so
    /// users can verify that their client_id / client_secret and pasted
    /// token JSON support refresh before running playlist operations.
    pub async fn test_refresh_token(&self) -> Result<()> {
        // Ensure we have a token loaded from DB first.
        {
            let mut lock = self.token.lock().await;
            if lock.is_none() {
                if let Some(st) = self.load_token_from_db().await? {
                    *lock = Some(st);
                } else {
                    return Err(anyhow!("no tidal token stored in DB"));
                }
            }
        }

        // Clone current token, attempt a refresh, then persist back into the
        // in-memory cache (and DB via refresh_token_internal).
        let mut cur = {
            let lock = self.token.lock().await;
            lock.as_ref()
                .cloned()
                .ok_or_else(|| anyhow!("no tidal token loaded"))?
        };

        self.refresh_token_internal(&mut cur).await?;

        let mut lock = self.token.lock().await;
        *lock = Some(cur);
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
            base64::engine::general_purpose::STANDARD
                .encode(format!("{}:{}", self.client_id, self.client_secret))
        );
        // Use the documented TIDAL OAuth2 token endpoint
        let url = format!("{}/v1/oauth2/token", Self::auth_base());
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
            return Err(anyhow!(
                "Failed to refresh tidal token: {} - {}",
                status,
                body
            ));
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
        let st = lock
            .as_ref()
            .ok_or_else(|| anyhow!("no tidal token loaded"))?;
        Ok(format!("Bearer {}", st.access_token))
    }

    /// Resolve TIDAL playlist itemIds for the given track ids in a playlist.
    ///
    /// TIDAL's playlist items DELETE endpoint expects a non-null
    /// `meta.itemId` for each relationship identifier. The itemId is
    /// exposed on the playlist items collection, so we first list items
    /// for the playlist and then build a mapping from track id -> itemIds.
    async fn resolve_playlist_item_ids(
        &self,
        playlist_id: &str,
        track_ids: &HashSet<String>,
    ) -> Result<HashMap<String, Vec<String>>> {
        let mut result: HashMap<String, Vec<String>> = HashMap::new();
        if track_ids.is_empty() {
            return Ok(result);
        }

        let base = Self::base_url();
        let bearer = self.get_bearer().await?;
        let cc = Self::country_code();
        let mut next_url = format!(
            "{}/playlists/{}/items?countryCode={}",
            base, playlist_id, cc
        );

        loop {
            let resp = self
                .client
                .get(&next_url)
                .header(AUTHORIZATION, &bearer)
                .send()
                .await?;

            if !resp.status().is_success() {
                let status = resp.status();
                let txt = resp.text().await.unwrap_or_default();
                log::warn!(
                    "Failed to list TIDAL playlist items for {}: {} => {}",
                    playlist_id,
                    status,
                    txt
                );
                break;
            }

            let j: serde_json::Value = resp.json().await?;

            if let Some(items) = j.get("data").and_then(|d| d.as_array()) {
                for item in items {
                    // Try to resolve the underlying track id for this playlist item.
                    let track_id_opt = item
                        .get("relationships")
                        .and_then(|r| r.get("track"))
                        .and_then(|t| t.get("data"))
                        .and_then(|d| d.get("id"))
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string())
                        .or_else(|| {
                            item.get("attributes")
                                .and_then(|a| a.get("trackId").or_else(|| a.get("trackID")))
                                .and_then(|v| v.as_str())
                                .map(|s| s.to_string())
                        });

                    // TIDAL exposes the playlist item id under meta.itemId;
                    // fall back to data.id if needed.
                    let item_id_opt = item
                        .get("meta")
                        .and_then(|m| m.get("itemId"))
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string())
                        .or_else(|| {
                            item.get("id")
                                .and_then(|v| v.as_str())
                                .map(|s| s.to_string())
                        });

                    if let (Some(track_id), Some(item_id)) = (track_id_opt, item_id_opt) {
                        if track_ids.contains(&track_id) {
                            result.entry(track_id).or_default().push(item_id);
                        }
                    }
                }
            }

            if let Some(next) = j
                .get("links")
                .and_then(|l| l.get("next"))
                .and_then(|v| v.as_str())
            {
                if next.is_empty() {
                    break;
                }
                next_url = if next.starts_with("http") {
                    next.to_string()
                } else {
                    format!("{}{}", base, next)
                };
            } else {
                break;
            }
        }

        Ok(result)
    }

    /// List all track ids for a given TIDAL playlist.
    async fn list_playlist_track_ids(&self, playlist_id: &str) -> Result<Vec<String>> {
        let mut out: Vec<String> = Vec::new();
        let base = Self::base_url();
        let bearer = self.get_bearer().await?;
        let cc = Self::country_code();
        let mut next_url = format!(
            "{}/playlists/{}/items?countryCode={}",
            base, playlist_id, cc
        );

        loop {
            let resp = self
                .client
                .get(&next_url)
                .header(AUTHORIZATION, &bearer)
                .send()
                .await?;

            let status = resp.status();
            if status == reqwest::StatusCode::NOT_FOUND {
                // TIDAL returns 404 for playlists with no items; treat that
                // as an empty playlist rather than an error so that
                // reconciliation can proceed to add tracks.
                return Ok(Vec::new());
            }
            if !status.is_success() {
                let txt = resp.text().await.unwrap_or_default();
                return Err(anyhow!(
                    "Failed to list TIDAL playlist items for {}: {} => {}",
                    playlist_id,
                    status,
                    txt
                ));
            }

            let j: serde_json::Value = resp.json().await?;

            if let Some(items) = j.get("data").and_then(|d| d.as_array()) {
                for item in items {
                    let track_id_opt = item
                        .get("relationships")
                        .and_then(|r| r.get("track"))
                        .and_then(|t| t.get("data"))
                        .and_then(|d| d.get("id"))
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string())
                        .or_else(|| {
                            item.get("attributes")
                                .and_then(|a| a.get("trackId").or_else(|| a.get("trackID")))
                                .and_then(|v| v.as_str())
                                .map(|s| s.to_string())
                        });

                    if let Some(id) = track_id_opt {
                        out.push(id);
                    }
                }
            }

            if let Some(next) = j
                .get("links")
                .and_then(|l| l.get("next"))
                .and_then(|v| v.as_str())
            {
                if next.is_empty() {
                    break;
                }
                next_url = if next.starts_with("http") {
                    next.to_string()
                } else {
                    format!("{}{}", base, next)
                };
            } else {
                break;
            }
        }

        // Deduplicate while preserving order.
        let mut seen = std::collections::HashSet::new();
        out.retain(|id| seen.insert(id.clone()));
        Ok(out)
    }

    /// Return the configured root folder name (trimmed) if any.
    fn root_folder(&self) -> Option<String> {
        self.root_folder_name
            .as_ref()
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
    }

    /// Ensure that a TIDAL userCollectionFolder exists to act as the logical
    /// root for all playlists created by this application, returning its id.
    ///
    /// If no root folder name is configured, this is a no-op and returns Ok(None).
    async fn ensure_root_folder(&self) -> Result<Option<String>> {
        let name = if let Some(n) = self.root_folder() {
            n
        } else {
            return Ok(None);
        };

        // Fast path: return cached id if we already resolved it.
        {
            let guard = self.root_folder_id.lock().await;
            if let Some(id) = guard.as_ref() {
                return Ok(Some(id.clone()));
            }
        }

        let base = Self::base_url();
        let bearer = self.get_bearer().await?;

        // Best-effort: try to find an existing folder with this name.
        // The API currently does not expose a direct name filter, so we
        // retrieve folders and match client-side on attributes.name.
        let list_url = format!("{}/userCollectionFolders", base);
        if let Ok(resp) = self
            .client
            .get(&list_url)
            .header(AUTHORIZATION, &bearer)
            .send()
            .await
        {
            if resp.status().is_success() {
                if let Ok(j) = resp.json::<serde_json::Value>().await {
                    if let Some(items) = j.get("data").and_then(|d| d.as_array()) {
                        for item in items {
                            let attrs = &item["attributes"];
                            let fname = attrs["name"].as_str().unwrap_or("");
                            let ctype = attrs["collectionType"].as_str().unwrap_or("");
                            if fname == name && ctype == "PLAYLISTS" {
                                if let Some(id) = item["id"].as_str() {
                                    let mut guard = self.root_folder_id.lock().await;
                                    *guard = Some(id.to_string());
                                    return Ok(Some(id.to_string()));
                                }
                            }
                        }
                    }
                }
            }
        }

        // Not found: create a new userCollectionFolder for playlists.
        let create_url = format!("{}/userCollectionFolders", base);
        let body = json!({
            "data": {
                "type": "userCollectionFolders",
                "attributes": {
                    "collectionType": "PLAYLISTS",
                    "name": name,
                }
            }
        });

        let resp = self
            .client
            .post(&create_url)
            .header(AUTHORIZATION, &bearer)
            // .header(CONTENT_TYPE, "application/vnd.api+json")
            .json(&body)
            .send()
            .await?;

        let status = resp.status();
        if !status.is_success() {
            let txt = resp.text().await.unwrap_or_default();
            log::warn!(
                "Failed to create TIDAL root folder {:?}: {} => {}",
                name,
                status,
                txt
            );
            return Ok(None);
        }

        let j: serde_json::Value = resp.json().await?;
        let id = j
            .get("data")
            .and_then(|d| d.get("id"))
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow!("no folder id in TIDAL root folder create response"))?;

        let mut guard = self.root_folder_id.lock().await;
        *guard = Some(id.to_string());
        Ok(Some(id.to_string()))
    }

    /// Add a playlist to the configured root folder's items relationship.
    async fn add_playlist_to_root_folder(&self, playlist_id: &str) -> Result<()> {
        let folder_id = if let Some(id) = self.ensure_root_folder().await? {
            id
        } else {
            return Ok(());
        };

        let base = Self::base_url();
        let bearer = self.get_bearer().await?;
        let url = format!(
            "{}/userCollectionFolders/{}/relationships/items",
            base, folder_id
        );
        let body = json!({
            "data": [ { "id": playlist_id } ]
        });

        let resp = self
            .client
            .post(&url)
            .header(AUTHORIZATION, &bearer)
            .header(CONTENT_TYPE, "application/vnd.api+json")
            .json(&body)
            .send()
            .await?;

        let status = resp.status();
        if !status.is_success() {
            let txt = resp.text().await.unwrap_or_default();
            log::warn!(
                "Failed to add playlist {} to TIDAL root folder {}: {} => {}",
                playlist_id,
                folder_id,
                status,
                txt
            );
        }
        Ok(())
    }
}

#[async_trait]
impl Provider for TidalProvider {
    fn name(&self) -> &str {
        TidalProvider::name(self)
    }
    fn is_authenticated(&self) -> bool {
        TidalProvider::is_authenticated(self)
    }
    async fn ensure_playlist(&self, name: &str, description: &str) -> Result<String> {
        let base = Self::base_url();
        // JSON:API-style endpoint: POST /playlists
        let url = format!("{}/playlists?countryCode={}", base, Self::country_code());
        // Minimal JSON:API payload; TIDAL's API expects a `data` wrapper.
        let body = json!({
            "data": {
                "type": "playlists",
                "attributes": {
                    // TIDAL uses `name` as the primary playlist label.
                    "name": name,
                    "description": description,
                    "public": false
                }
            }
        });
        let mut attempt: u32 = 0;
        loop {
            attempt += 1;
            let bearer = self.get_bearer().await?;
            let resp = self
                .client
                .post(&url)
                .header(AUTHORIZATION, &bearer)
                .header(CONTENT_TYPE, "application/vnd.tidal.v1+json")
                .json(&body)
                .send()
                .await?;
            let status = resp.status();

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
                let txt = resp.text().await.unwrap_or_default();
                return Err(anyhow!(
                    "tidal create playlist failed: {} => {}",
                    status,
                    txt
                ));
            }
            let j: serde_json::Value = resp.json().await?;
            // Tidal JSON:API responses return id under data.id
            let id = j
                .get("data")
                .and_then(|d| d.get("id"))
                .and_then(|v| v.as_str())
                // Fallbacks for older/undocumented shapes
                .or_else(|| j.get("uuid").and_then(|v| v.as_str()))
                .or_else(|| j.get("id").and_then(|v| v.as_str()))
                .ok_or_else(|| anyhow!("no playlist id in response"))?;
            let id_str = id.to_string();

            // If a logical root folder is configured, best-effort add this
            // playlist under that folder so that all app-created playlists
            // appear grouped together in the user's TIDAL UI.
            if self.root_folder().is_some() {
                if let Err(e) = self.add_playlist_to_root_folder(&id_str).await {
                    log::warn!(
                        "Failed to attach playlist {} to TIDAL root folder: {}",
                        id_str,
                        e
                    );
                }
            }

            return Ok(id_str);
        }
    }

    async fn rename_playlist(&self, playlist_id: &str, new_name: &str) -> Result<()> {
        let base = Self::base_url();
        // JSON:API-style playlist update: PATCH /playlists/{id}
        let url = format!(
            "{}/playlists/{}?countryCode={}",
            base,
            playlist_id,
            Self::country_code()
        );
        let body = json!({
            "data": {
                "type": "playlists",
                "id": playlist_id,
                "attributes": {
                    // Rename uses the same `name` field.
                    "name": new_name
                }
            }
        });
        let mut attempt: u32 = 0;
        loop {
            attempt += 1;
            let bearer = self.get_bearer().await?;
            let resp = self
                .client
                .patch(&url)
                .header(AUTHORIZATION, &bearer)
                .header(CONTENT_TYPE, "application/vnd.tidal.v1+json")
                .json(&body)
                .send()
                .await?;
            let status = resp.status();

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
                let txt = resp.text().await.unwrap_or_default();
                return Err(anyhow!("tidal rename failed: {} => {}", status, txt));
            }
            return Ok(());
        }
    }

    async fn add_tracks(&self, playlist_id: &str, uris: &[String]) -> Result<()> {
        let bearer = self.get_bearer().await?;
        let base = Self::base_url();
        // JSON:API relationship endpoint: POST /playlists/{id}/relationships/items
        let url = format!(
            "{}/playlists/{}/relationships/items?countryCode={}",
            base,
            playlist_id,
            Self::country_code()
        );
        // Convert URIs like "tidal:track:{id}" into JSON:API relationship objects
        // { "data": [{"type": "tracks", "id": "{id}"}, ...] }.
        let data: Vec<serde_json::Value> = uris
            .iter()
            .filter_map(|u| {
                let id = u.rsplit(':').next().unwrap_or("").trim();
                if id.is_empty() {
                    None
                } else {
                    // TIDAL's DELETE playlist items endpoint expects a non-null
                    // `meta` object on each relationship identifier; an empty
                    // object satisfies the schema and avoids INVALID_REQUEST_BODY
                    // errors like "data/0/meta must not be null".
                    Some(json!({ "type": "tracks", "id": id, "meta": {} }))
                }
            })
            .collect();
        if data.is_empty() {
            // Nothing to add once URIs are normalized.
            return Ok(());
        }
        let body = json!({ "data": data });
        let resp = self
            .client
            .post(&url)
            .header(AUTHORIZATION, &bearer)
            .header(CONTENT_TYPE, "application/vnd.tidal.v1+json")
            .json(&body)
            .send()
            .await?;
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
            return Err(anyhow!("tidal add tracks failed: {} => {}", status, txt));
        }
        Ok(())
    }

    async fn remove_tracks(&self, playlist_id: &str, uris: &[String]) -> Result<()> {
        let base = Self::base_url();
        let cc = Self::country_code();

        // Normalize requested URIs into raw track ids and build a set
        // for efficient lookup when scanning playlist items.
        let mut track_ids: HashSet<String> = HashSet::new();
        for u in uris {
            let id = u.rsplit(':').next().unwrap_or("").trim();
            if !id.is_empty() {
                track_ids.insert(id.to_string());
            }
        }
        if track_ids.is_empty() {
            return Ok(());
        }

        // Resolve playlist itemIds for the tracks we want to remove.
        let item_map = self
            .resolve_playlist_item_ids(playlist_id, &track_ids)
            .await?;

        // Build the DELETE payload using both track id and meta.itemId,
        // matching TIDAL's expectation of `meta.itemId` being non-null.
        let mut data: Vec<serde_json::Value> = Vec::new();
        for u in uris {
            let id = u.rsplit(':').next().unwrap_or("").trim();
            if id.is_empty() {
                continue;
            }
            if let Some(item_ids) = item_map.get(id) {
                for item_id in item_ids {
                    data.push(json!({
                        "type": "tracks",
                        "id": id,
                        "meta": { "itemId": item_id }
                    }));
                }
            }
        }

        // If we failed to resolve any playlist items for the requested
        // URIs, treat this as a no-op to avoid repeatedly triggering
        // INVALID_REQUEST_BODY errors.
        if data.is_empty() {
            return Ok(());
        }

        let bearer = self.get_bearer().await?;
        // JSON:API relationship endpoint for deleting items.
        let url = format!(
            "{}/playlists/{}/relationships/items?countryCode={}",
            base, playlist_id, cc
        );
        let body = json!({ "data": data });
        let resp = self
            .client
            .delete(&url)
            .header(AUTHORIZATION, &bearer)
            .header(CONTENT_TYPE, "application/vnd.tidal.v1+json")
            .json(&body)
            .send()
            .await?;
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
            return Err(anyhow!("tidal remove tracks failed: {} => {}", status, txt));
        }
        Ok(())
    }

    async fn search_track_uri(&self, title: &str, artist: &str) -> Result<Option<String>> {
        let bearer = self.get_bearer().await?;
        let base = Self::base_url();
        let q = format!("{} {}", title, artist);
        let url = format!(
            "{}/search/tracks?query={}&limit=1&countryCode={}",
            base,
            urlencoding::encode(&q),
            Self::country_code()
        );
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
        // TIDAL search responses may either return an `items` array directly or
        // wrap it in an `items` object with its own `items` array; handle both.
        let first = j["items"]
            .as_array()
            .and_then(|a| a.get(0))
            .or_else(|| j["items"]["items"].as_array().and_then(|a| a.get(0)));
        if let Some(item) = first {
            if let Some(id) = item["id"].as_str() {
                return Ok(Some(format!("tidal:track:{}", id)));
            } else if let Some(id_num) = item["id"].as_i64() {
                return Ok(Some(format!("tidal:track:{}", id_num)));
            }
        }
        Ok(None)
    }

    async fn search_track_uri_by_isrc(&self, isrc: &str) -> Result<Option<String>> {
        let bearer = self.get_bearer().await?;
        let base = Self::base_url();
        // Use the dedicated ISRC filter endpoint, e.g.:
        //   /tracks?countryCode=US&filter%5Bisrc%5D=DEVF11900580
        // ISRCs are alphanumeric so we can safely embed them without extra encoding.
        let url = format!(
            "{}/tracks?countryCode={}&filter%5Bisrc%5D={}",
            base,
            Self::country_code(),
            isrc
        );
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
        let first = j["data"].as_array().and_then(|a| a.get(0));
        if let Some(item) = first {
            if let Some(id) = item["id"].as_str() {
                return Ok(Some(format!("tidal:track:{}", id)));
            } else if let Some(id_num) = item["id"].as_i64() {
                return Ok(Some(format!("tidal:track:{}", id_num)));
            }
        }
        Ok(None)
    }

    async fn lookup_track_isrc(&self, uri: &str) -> Result<Option<String>> {
        // Expect URIs like "tidal:track:{id}"; extract the id portion.
        let id = if let Some(i) = uri.rsplit(':').next() {
            i.to_string()
        } else {
            uri.rsplit('/').next().unwrap_or("").to_string()
        };
        if id.is_empty() {
            return Ok(None);
        }
        let base = Self::base_url();
        let cc = Self::country_code();
        let url = format!("{}/tracks/{}?countryCode={}", base, id, cc);
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
        // Track ISRC is exposed on the track's attributes in TIDAL's
        // JSON:API schema.
        if let Some(isrc) = j["data"]["attributes"]["isrc"].as_str() {
            return Ok(Some(isrc.to_string()));
        }
        Ok(None)
    }

    async fn delete_playlist(&self, playlist_id: &str) -> Result<()> {
        let bearer = self.get_bearer().await?;
        let base = Self::base_url();
        let url = format!(
            "{}/playlists/{}?countryCode={}",
            base,
            playlist_id,
            Self::country_code()
        );
        let resp = self
            .client
            .delete(&url)
            .header(AUTHORIZATION, &bearer)
            .send()
            .await?;
        let status = resp.status();
        if !status.is_success() {
            let txt = resp.text().await.unwrap_or_default();
            return Err(anyhow!(
                "tidal delete playlist failed: {} => {}",
                status,
                txt
            ));
        }
        Ok(())
    }

    async fn list_playlist_tracks(&self, playlist_id: &str) -> Result<Vec<String>> {
        // For TIDAL we expose track ids as opaque "URIs"; reconciliation
        // logic only cares about set equality, not the scheme itself.
        self.list_playlist_track_ids(playlist_id).await
    }
}
