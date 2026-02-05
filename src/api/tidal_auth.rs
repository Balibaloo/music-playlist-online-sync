use crate::config::Config;
use crate::db;
use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use tracing::info;

/// Simple helper to persist Tidal token JSON into the DB.
/// For the prototype we accept a raw JSON blob pasted by the user and store it.
#[derive(Serialize, Deserialize)]
struct TokenBlob {
    access_token: String,
    token_type: Option<String>,
    expires_in: Option<i64>,
    refresh_token: Option<String>,
}

pub async fn run_tidal_auth(cfg: &Config) -> Result<()> {
    println!("Paste Tidal token JSON (single line) and press Enter:");
    let mut input = String::new();
    std::io::stdin().read_line(&mut input)?;
    let s = input.trim();
    if s.is_empty() {
        return Err(anyhow!("no input provided"));
    }
    // basic validation
    let _tb: TokenBlob = serde_json::from_str(s)?;
    let db_path = cfg.db_path.clone();
    let token_json = s.to_string();
    tokio::task::spawn_blocking(move || -> Result<(), anyhow::Error> {
        let conn = rusqlite::Connection::open(db_path)?;
        db::save_credential_raw(&conn, "tidal", &token_json)?;
        Ok(())
    })
    .await??;

    info!("Tidal token saved to DB provider 'tidal'");
    println!("Saved tidal token JSON to DB.");
    Ok(())
}