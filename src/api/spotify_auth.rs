use crate::config::Config;
use crate::db;
use anyhow::{anyhow, Result};
use base64::{engine::general_purpose, Engine as _};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tracing::info;
use url::Url;

/// This module implements a simple manual OAuth helper:
/// 1. Build the Spotify authorization URL and print it.
/// 2. User opens it in a browser, approves and gets redirected to the redirect URI (which may fail if it's a dummy).
/// 3. User copies the full redirect URL and pastes it into this CLI.
/// 4. The CLI extracts the `code` param and exchanges it for an access_token + refresh_token.
/// 5. The tokens are stored in the DB credentials table as JSON.
///
/// This avoids running an embedded HTTP server and works well for manual setup.
#[derive(Serialize, Deserialize)]
struct TokenResponse {
    access_token: String,
    token_type: String,
    expires_in: i64,
    refresh_token: Option<String>,
    scope: Option<String>,
}

pub async fn run_spotify_auth(cfg: &Config) -> Result<()> {
    use std::io;

    println!("Enter your Spotify client_id:");
    let mut client_id = String::new();
    io::stdin().read_line(&mut client_id)?;
    let client_id = client_id.trim().to_string();
    if client_id.is_empty() {
        return Err(anyhow!("no client_id provided"));
    }

    println!("Enter your Spotify client_secret:");
    let mut client_secret = String::new();
    io::stdin().read_line(&mut client_secret)?;
    let client_secret = client_secret.trim().to_string();
    if client_secret.is_empty() {
        return Err(anyhow!("no client_secret provided"));
    }

    println!("Enter your Spotify redirect URI (leave blank for http://127.0.0.1:8888/):");
    let mut redirect_uri = String::new();
    io::stdin().read_line(&mut redirect_uri)?;
    let redirect_uri = {
        let trimmed = redirect_uri.trim();
        if trimmed.is_empty() {
            "http://127.0.0.1:8888/".to_string()
        } else {
            trimmed.to_string()
        }
    };

    // Build the auth URL
    let scopes = vec![
        "playlist-modify-private",
        "playlist-modify-public",
        "playlist-read-private",
        "user-read-private",
        "user-read-email",
    ];
    let mut url = Url::parse("https://accounts.spotify.com/authorize")?;
    url.query_pairs_mut()
        .append_pair("response_type", "code")
        .append_pair("client_id", &client_id)
        .append_pair("scope", &scopes.join(" "))
        .append_pair("redirect_uri", &redirect_uri)
        .append_pair("show_dialog", "true");

    println!(
        "Open this URL in your browser and authorize the application:\n\n{}\n",
        url
    );
    println!("After authorizing, you'll be redirected to your redirect URI. Copy the full redirect URL and paste it here.");
    println!("Paste redirect URL:");
    let mut input = String::new();
    std::io::stdin().read_line(&mut input)?;
    let input = input.trim();
    let parsed = Url::parse(input).map_err(|e| anyhow!("invalid url pasted: {}", e))?;
    let code = parsed
        .query_pairs()
        .find(|(k, _)| k == "code")
        .ok_or_else(|| anyhow!("no code in redirect URL"))?
        .1
        .into_owned();

    // Exchange code for tokens
    let client = Client::new();
    let params = [
        ("grant_type", "authorization_code"),
        ("code", &code),
        ("redirect_uri", &redirect_uri),
    ];
    let auth_header = format!(
        "Basic {}",
        general_purpose::STANDARD.encode(format!("{}:{}", client_id, client_secret))
    );
    let resp = client
        .post("https://accounts.spotify.com/api/token")
        .header("Authorization", auth_header)
        .form(&params)
        .send()
        .await?;
    let status = resp.status();
    if !status.is_success() {
        let txt = resp.text().await.unwrap_or_default();
        return Err(anyhow!("token exchange failed: {} => {}", status, txt));
    }

    let tr: TokenResponse = resp.json().await?;
    // Compute expires_at as now + expires_in
    let expires_at = chrono::Utc::now().timestamp() + tr.expires_in;
    // Build the stored token to match what the provider expects
    let stored_token = crate::api::spotify::StoredToken {
        access_token: tr.access_token,
        token_type: tr.token_type,
        expires_at,
        refresh_token: tr.refresh_token,
        scope: tr.scope,
    };
    let token_json = serde_json::to_string(&stored_token)?;
    // Persist to DB (blocking)
    let db_path = cfg.db_path.clone();
    let client_id = client_id.to_string();
    let client_secret = client_secret.to_string();
    tokio::task::spawn_blocking(move || -> Result<(), anyhow::Error> {
        let conn = rusqlite::Connection::open(db_path)?;
        db::save_credential_raw(
            &conn,
            "spotify",
            &token_json,
            Some(&client_id),
            Some(&client_secret),
        )?;
        Ok(())
    })
    .await??;

    info!("Spotify tokens saved to DB for provider 'spotify'");
    println!("Saved tokens to DB. You can now run the worker which will use Spotify provider.");

    Ok(())
}
