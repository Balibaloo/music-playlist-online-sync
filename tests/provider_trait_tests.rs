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
    let tidal = TidalProvider::new("client_id".to_string(), "client_secret".to_string(), std::path::PathBuf::from("/tmp/db"));
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
    let tidal = TidalProvider::new("".to_string(), "".to_string(), std::path::PathBuf::from("/tmp/db"));
    assert!(!tidal.is_authenticated());
}
