use music_file_playlist_online_sync::api::{Provider, spotify::SpotifyProvider, tidal::TidalProvider, mock::MockProvider};

#[test]
fn test_mock_provider_trait() {
    let mock = MockProvider::new();
    assert_eq!(mock.name(), "mock");
    assert!(!mock.is_authenticated());
}

#[test]
fn test_spotify_provider_trait() {
    let spotify = SpotifyProvider::new("client_id".to_string(), "client_secret".to_string(), std::path::PathBuf::from("/tmp/db"));
    assert_eq!(spotify.name(), "spotify");
    assert!(spotify.is_authenticated());
}

#[test]
fn test_tidal_provider_trait() {
    let tidal = TidalProvider::new("client_id".to_string(), "client_secret".to_string(), std::path::PathBuf::from("/tmp/db"), None);
    assert_eq!(tidal.name(), "tidal");
    assert!(tidal.is_authenticated());
}

#[test]
fn test_spotify_provider_not_authenticated() {
    let spotify = SpotifyProvider::new("".to_string(), "".to_string(), std::path::PathBuf::from("/tmp/db"));
    assert!(!spotify.is_authenticated());
}

#[test]
fn test_tidal_provider_not_authenticated() {
    let tidal = TidalProvider::new("".to_string(), "".to_string(), std::path::PathBuf::from("/tmp/db"), None);
    assert!(!tidal.is_authenticated());
}

#[tokio::test]
async fn test_provider_auth_playlist_ops() {
    use std::env;
    use std::path::PathBuf;
    // Use real credentials from env for integration test
    let db_path = env::var("MFPOS_TEST_DB").unwrap_or_else(|_| "/tmp/mfpos_test.db".to_string());
    let spotify_id = env::var("SPOTIFY_CLIENT_ID").unwrap_or_default();
    let spotify_secret = env::var("SPOTIFY_CLIENT_SECRET").unwrap_or_default();
    let tidal_id = env::var("TIDAL_CLIENT_ID").unwrap_or_default();
    let tidal_secret = env::var("TIDAL_CLIENT_SECRET").unwrap_or_default();

    // Spotify
    if !spotify_id.is_empty() && !spotify_secret.is_empty() {
        let spotify = SpotifyProvider::new(spotify_id.clone(), spotify_secret.clone(), PathBuf::from(&db_path));
        let playlists = spotify.list_user_playlists().await.expect("Spotify list playlists");
        println!("Spotify playlists: {:?}", playlists);
        // Find max N for Test MFPOS {N}
        let mut max_n = 0;
        for (_, name) in &playlists {
            if let Some(n) = name.strip_prefix("Test MFPOS ").and_then(|s| s.parse::<u32>().ok()) {
                if n > max_n { max_n = n; }
            }
        }
        let new_n = max_n + 1;
        let new_name = format!("Test MFPOS {}", new_n);
        let pl_id = spotify.ensure_playlist(&new_name, "Auth test playlist").await.expect("Spotify create playlist");
        println!("Created Spotify playlist: {} ({})", new_name, pl_id);
        let rename_name = format!("Test MFPOS {}", new_n + 1);
        spotify.rename_playlist(&pl_id, &rename_name).await.expect("Spotify rename playlist");
        println!("Renamed Spotify playlist to: {}", rename_name);
    }

    // Tidal
    if !tidal_id.is_empty() && !tidal_secret.is_empty() {
        let tidal = TidalProvider::new(tidal_id.clone(), tidal_secret.clone(), PathBuf::from(&db_path), None);
        let playlists = tidal.list_user_playlists().await.expect("Tidal list playlists");
        println!("Tidal playlists: {:?}", playlists);
        let mut max_n = 0;
        for (_, name) in &playlists {
            if let Some(n) = name.strip_prefix("Test MFPOS ").and_then(|s| s.parse::<u32>().ok()) {
                if n > max_n { max_n = n; }
            }
        }
        let new_n = max_n + 1;
        let new_name = format!("Test MFPOS {}", new_n);
        let pl_id = tidal.ensure_playlist(&new_name, "Auth test playlist").await.expect("Tidal create playlist");
        println!("Created Tidal playlist: {} ({})", new_name, pl_id);
        let rename_name = format!("Test MFPOS {}", new_n + 1);
        tidal.rename_playlist(&pl_id, &rename_name).await.expect("Tidal rename playlist");
        println!("Renamed Tidal playlist to: {}", rename_name);
    }
}
