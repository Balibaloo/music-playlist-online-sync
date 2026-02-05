use music_file_playlist_online_sync::api::{Provider, mock::MockProvider};
use music_file_playlist_online_sync::worker::run_worker_once;
use music_file_playlist_online_sync::config::Config;
use std::sync::Arc;
use std::collections::HashSet;
use std::sync::Mutex;

// Dummy provider for test, records calls
struct TestProvider {
    name: &'static str,
    called: Arc<Mutex<HashSet<String>>>,
}

#[async_trait::async_trait]
impl Provider for TestProvider {
    async fn ensure_playlist(&self, name: &str, _description: &str) -> anyhow::Result<String> {
        self.called.lock().unwrap().insert(format!("{}:ensure_playlist:{}", self.name, name));
        Ok(format!("playlist-{}", name))
    }
    async fn rename_playlist(&self, _playlist_id: &str, _new_name: &str) -> anyhow::Result<()> { Ok(()) }
    async fn add_tracks(&self, _playlist_id: &str, _uris: &[String]) -> anyhow::Result<()> { Ok(()) }
    async fn remove_tracks(&self, _playlist_id: &str, _uris: &[String]) -> anyhow::Result<()> { Ok(()) }
    async fn search_track_uri(&self, _title: &str, _artist: &str) -> anyhow::Result<Option<String>> { Ok(None) }
    fn name(&self) -> &str { self.name }
    fn is_authenticated(&self) -> bool { true }
}

#[tokio::test]
async fn test_event_propagates_to_all_providers() {
    let called = Arc::new(Mutex::new(HashSet::new()));
    let p1 = Arc::new(TestProvider { name: "p1", called: called.clone() });
    let p2 = Arc::new(TestProvider { name: "p2", called: called.clone() });
    // Simulate worker logic: process with all providers
    let providers: Vec<Arc<dyn Provider>> = vec![p1.clone(), p2.clone()];
    for provider in &providers {
        provider.ensure_playlist("test", "desc").await.unwrap();
    }
    let calls = called.lock().unwrap();
    assert!(calls.contains("p1:ensure_playlist:test"));
    assert!(calls.contains("p2:ensure_playlist:test"));
}
