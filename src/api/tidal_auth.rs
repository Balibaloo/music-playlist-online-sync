use crate::config::Config;
use crate::db;
use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use tracing::info;

/// Simple helper to persist Tidal token JSON into the DB.
/// For this flow we ask the user to obtain a token via the
/// TIDAL API reference site and paste the raw JSON response.
#[derive(Serialize, Deserialize)]
struct TokenBlob {
    access_token: String,
    token_type: Option<String>,
    expires_in: Option<i64>,
    refresh_token: Option<String>,
    scope: Option<String>,
    user_id: Option<i64>,
}

pub async fn run_tidal_auth(cfg: &Config) -> Result<()> {
    use chrono::Utc;
    use std::io::{self, Read};

    println!("Enter your Tidal client_id:");
    let mut client_id = String::new();
    io::stdin().read_line(&mut client_id)?;
    let client_id = client_id.trim().to_string();
    if client_id.is_empty() {
        return Err(anyhow!("no client_id provided"));
    }

    println!("Enter your Tidal client_secret:");
    let mut client_secret = String::new();
    io::stdin().read_line(&mut client_secret)?;
    let client_secret = client_secret.trim().to_string();
    if client_secret.is_empty() {
        return Err(anyhow!("no client_secret provided"));
    }

    println!("\nNow obtain a TIDAL OAuth token using the official API reference site.");
    println!("1. Open: https://tidal-music.github.io/tidal-api-reference/ in your browser.");
    println!("2. Use your TIDAL client_id and client_secret to authorize the app.");
    println!("3. In your browser dev tools (Network tab), find the token request to auth.tidal.com.");
    println!("4. In the Response tab of that request, copy the full JSON body (access_token, refresh_token, etc.).");
    println!("\nPaste the JSON response below, then press Ctrl+D (on Linux/macOS) when you're done pasting:\n");

    let mut buf = String::new();
    io::stdin().read_to_string(&mut buf)?;
    let buf = buf.trim();
    if buf.is_empty() {
        return Err(anyhow!("no JSON pasted"));
    }

    let tr: TokenBlob = serde_json::from_str(buf)
        .map_err(|e| anyhow!("failed to parse pasted JSON as token response: {}", e))?;

    let expires_at = Utc::now().timestamp() + tr.expires_in.unwrap_or(3600);
    // Build the stored token to match what the provider expects
    let stored_token = crate::api::tidal::StoredToken {
        access_token: tr.access_token,
        token_type: tr.token_type.unwrap_or_else(|| "Bearer".to_string()),
        expires_at,
        refresh_token: tr.refresh_token,
        scope: tr.scope,
        user_id: tr.user_id,
    };
    let token_json = serde_json::to_string(&stored_token)?;

    // Persist to DB (blocking), together with client_id/client_secret
    let db_path = cfg.db_path.clone();
    let client_id = client_id.to_string();
    let client_secret = client_secret.to_string();
    tokio::task::spawn_blocking(move || -> Result<(), anyhow::Error> {
        let conn = rusqlite::Connection::open(db_path)?;
        db::save_credential_raw(&conn, "tidal", &token_json, Some(&client_id), Some(&client_secret))?;
        Ok(())
    })
    .await??;

    info!("Tidal tokens saved to DB for provider 'tidal'");
    println!("Saved tokens to DB. You can now run the worker which will use the Tidal provider.");

    Ok(())
}