use crate::config::Config;
use crate::db;
use crate::playlist;
use crate::util;
use crate::models::EventAction;
use notify::{Config as NotifyConfig, Event as NotifyEvent, EventKind, RecommendedWatcher, RecursiveMode, Result as NotifyResult, Watcher};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{mpsc::channel, Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};
use tracing::{debug, error, info, warn};
use walkdir::WalkDir;

/// A node represents one folder: immediate children folders, and immediate track files.
#[derive(Debug, Clone)]
pub struct FolderNode {
    pub path: PathBuf,
    pub children: HashSet<PathBuf>,
    pub tracks: HashSet<PathBuf>,
}

impl FolderNode {
    pub fn new(path: PathBuf) -> Self {
        Self { path, children: HashSet::new(), tracks: HashSet::new() }
    }
}

/// In-memory tree model for the watched root folder.
/// Stores a map path -> FolderNode for immediate-folder/tracks bookkeeping.
#[derive(Debug)]
pub struct InMemoryTree {
    pub root: PathBuf,
    pub nodes: HashMap<PathBuf, FolderNode>,
    pub whitelist: Option<Vec<PathBuf>>,
}

impl InMemoryTree {
    /// Build the tree by scanning the filesystem under root (respecting optional whitelist).
    pub fn build(root: &Path, whitelist: Option<&str>) -> anyhow::Result<Self> {
        let wl = whitelist.map(|s| {
            s.split(':')
                .filter(|p| !p.is_empty())
                .map(|p| {
                    let mut pb = PathBuf::from(p);
                    if pb.is_relative() {
                        pb = root.join(pb);
                    }
                    pb
                })
                .collect::<Vec<_>>()
        });

        let mut nodes: HashMap<PathBuf, FolderNode> = HashMap::new();

        let walker = WalkDir::new(root).follow_links(false).min_depth(0);

        for entry in walker.into_iter().filter_map(|e| e.ok()) {
            let path = entry.path().to_path_buf();
            if entry.file_type().is_dir() {
                // if whitelist exists, skip dirs not under it
                if let Some(ref wlvec) = wl {
                    let mut allowed = false;
                    for w in wlvec {
                        if path.starts_with(w) {
                            allowed = true;
                            break;
                        }
                    }
                    if !allowed {
                        continue;
                    }
                }
                nodes.entry(path.clone()).or_insert_with(|| FolderNode::new(path));
            } else if entry.file_type().is_file() {
                if let Some(parent) = entry.path().parent() {
                    let parent = parent.to_path_buf();
                    let node = nodes.entry(parent.clone()).or_insert_with(|| FolderNode::new(parent.clone()));
                    node.tracks.insert(path.clone());
                }
            }
        }

        // populate children sets
        let keys: Vec<PathBuf> = nodes.keys().cloned().collect();
        for k in keys {
            if let Ok(read) = std::fs::read_dir(&k) {
                for e in read.filter_map(|r| r.ok()) {
                    let p = e.path();
                    if p.is_dir() {
                        if nodes.contains_key(&p) {
                            if let Some(node) = nodes.get_mut(&k) {
                                node.children.insert(p);
                            }
                        }
                    }
                }
            }
        }

        Ok(Self { root: root.to_path_buf(), nodes, whitelist: wl })
    }

    /// Helper to find the folder node nearest ancestor for a file path.
    pub fn folder_for_path(&self, path: &Path) -> Option<PathBuf> {
        let mut p = if path.is_dir() { path.to_path_buf() } else { path.parent().map(|x| x.to_path_buf())? };
        loop {
            if self.nodes.contains_key(&p) {
                return Some(p);
            }
            if p == self.root {
                break;
            }
            if !p.pop() {
                break;
            }
        }
        None
    }

    /// Apply a synthetic event (used in tests) and return list of generated logical operations
    /// that the watcher should enqueue + playlist rebuild targets.
    pub fn apply_synthetic_event(&mut self, op: SyntheticEvent) -> Vec<LogicalOp> {
        let mut out: Vec<LogicalOp> = Vec::new();
        match op {
            SyntheticEvent::FileCreate(p) => {
                if let Some(folder) = self.folder_for_path(&p) {
                    if let Some(node) = self.nodes.get_mut(&folder) {
                        node.tracks.insert(p.clone());
                    } else {
                        // create node if parent dir wasn't present (e.g., new dir)
                        let mut node = FolderNode::new(folder.clone());
                        node.tracks.insert(p.clone());
                        self.nodes.insert(folder.clone(), node);
                    }
                    out.push(LogicalOp::Add { playlist_folder: folder, track_path: p });
                }
            }
            SyntheticEvent::FileRemove(p) => {
                if let Some(folder) = self.folder_for_path(&p) {
                    if let Some(node) = self.nodes.get_mut(&folder) {
                        node.tracks.remove(&p);
                    }
                    out.push(LogicalOp::Remove { playlist_folder: folder, track_path: p });
                }
            }
            SyntheticEvent::FileRename { from, to } => {
                // treat as remove then add
                if let Some(from_folder) = self.folder_for_path(&from) {
                    if let Some(node) = self.nodes.get_mut(&from_folder) {
                        node.tracks.remove(&from);
                    }
                    out.push(LogicalOp::Remove { playlist_folder: from_folder.clone(), track_path: from.clone() });
                }
                if let Some(to_folder) = self.folder_for_path(&to) {
                    if let Some(node) = self.nodes.get_mut(&to_folder) {
                        node.tracks.insert(to.clone());
                    }
                    out.push(LogicalOp::Add { playlist_folder: to_folder.clone(), track_path: to.clone() });
                }
            }
            SyntheticEvent::FolderRename { from, to } => {
                // handle folder rename/move -> emit PlaylistRename
                let from_folder = from.clone();
                let to_folder = to.clone();
                // move node entry if exists
                if self.nodes.contains_key(&from_folder) {
                    let node = self.nodes.remove(&from_folder).unwrap();
                    self.nodes.entry(to_folder.clone()).or_insert(node);
                } else {
                    self.nodes.entry(to_folder.clone()).or_insert_with(|| FolderNode::new(to_folder.clone()));
                }
                // update parent's children sets for destination
                if let Some(parent) = to_folder.parent() {
                    if let Some(node) = self.nodes.get_mut(&parent.to_path_buf()) {
                        node.children.insert(to_folder.clone());
                    }
                }
                out.push(LogicalOp::PlaylistRename { from_folder: from_folder.clone(), to_folder: to_folder.clone() });
            }
            SyntheticEvent::FolderCreate(p) => {
                // create node
                self.nodes.entry(p.clone()).or_insert_with(|| FolderNode::new(p.clone()));
                // update parent's children set
                if let Some(parent) = p.parent() {
                    if let Some(node) = self.nodes.get_mut(&parent.to_path_buf()) {
                        node.children.insert(p.clone());
                    }
                }
                out.push(LogicalOp::Create { playlist_folder: p });
            }
            SyntheticEvent::FolderRemove(p) => {
                // remove node and children membership
                self.nodes.remove(&p);
                if let Some(parent) = p.parent() {
                    if let Some(node) = self.nodes.get_mut(&parent.to_path_buf()) {
                        node.children.remove(&p);
                    }
                }
                out.push(LogicalOp::Delete { playlist_folder: p });
            }
        }
        out
    }
}

/// Logical operations produced by watcher when interpreting FS events.
#[derive(Debug, Clone)]
pub enum LogicalOp {
    Add { playlist_folder: PathBuf, track_path: PathBuf },
    Remove { playlist_folder: PathBuf, track_path: PathBuf },
    Create { playlist_folder: PathBuf },
    Delete { playlist_folder: PathBuf },
    // Playlist rename (folder rename) represented when a folder is renamed/moved.
    PlaylistRename { from_folder: PathBuf, to_folder: PathBuf },
}

/// Synthetic event type for unit tests (so tests don't rely on real notify events).
#[derive(Debug, Clone)]
pub enum SyntheticEvent {
    FileCreate(PathBuf),
    FileRemove(PathBuf),
    FileRename { from: PathBuf, to: PathBuf },
    FolderRename { from: PathBuf, to: PathBuf },
    FolderCreate(PathBuf),
    FolderRemove(PathBuf),
}

/// Start the watcher; this is the long-running entry point called by the CLI.
pub fn run_watcher(cfg: &Config) -> anyhow::Result<()> {
    info!("Starting watcher with root {:?}", cfg.root_folder);
    // Open DB (blocking)
    let conn = db::open_or_create(&cfg.db_path)?;

    // Build initial in-memory tree
    let mut tree = InMemoryTree::build(&cfg.root_folder, if cfg.whitelist.is_empty() { None } else { Some(&cfg.whitelist) })?;
    info!("Initial scan complete: {} folders", tree.nodes.len());

    // Initial playlist writes (flat mode)
    for (folder, _node) in tree.nodes.iter() {
        let folder_name = folder.file_name().and_then(|s| s.to_str()).unwrap_or("");
        let rel = folder.strip_prefix(&cfg.root_folder).unwrap_or(folder).display().to_string();
        let playlist_name = util::expand_template(&cfg.local_playlist_template, folder_name, &rel);
        let playlist_path = folder.join(playlist_name);
        if cfg.playlist_mode == "flat" {
            if let Err(e) = playlist::write_flat_playlist(folder, &playlist_path, &cfg.playlist_order_mode) {
                warn!("Failed to write initial playlist {:?}: {}", playlist_path, e);
            }
        } else {
            if let Err(e) = playlist::write_linked_playlist(folder, &playlist_path, &cfg.linked_reference_format, &cfg.local_playlist_template) {
                warn!("Failed to write initial linked playlist {:?}: {}", playlist_path, e);
            }
        }
    }

    // Shared debounce queue: map playlist folder -> earliest_due Instant
    let debounce_map: Arc<Mutex<HashMap<PathBuf, Instant>>> = Arc::new(Mutex::new(HashMap::new()));
    let debounce_ms = cfg.debounce_ms;

    // Wrap in-memory tree in Arc<Mutex<...>> so notify callback can update it concurrently
    let tree = Arc::new(Mutex::new(tree));

    // Spawn debounce worker thread: writes playlists when their debounce timer elapses and enqueues
    // a generic Create event for the playlist.
    {
        let debounce_map = debounce_map.clone();
        let cfg = cfg.clone();
        let db_path = cfg.db_path.clone();
        let tree = tree.clone();
        thread::spawn(move || {
            loop {
                // collect due playlists
                let due: Vec<PathBuf> = {
                    let mut guard = debounce_map.lock().unwrap();
                    let now = Instant::now();
                    let mut ready = Vec::new();
                    guard.retain(|folder, &mut t| {
                        if t <= now {
                            ready.push(folder.clone());
                            false // remove from map
                        } else {
                            true
                        }
                    });
                    ready
                };

                for folder in due {
                    // Write local playlist and enqueue a generic create/update event for playlist (watcher enqueues per-file ops too)
                    let folder_name = folder.file_name().and_then(|s| s.to_str()).unwrap_or("");
                    let rel = folder.strip_prefix(&cfg.root_folder).unwrap_or(&folder).display().to_string();
                    let playlist_name = util::expand_template(&cfg.local_playlist_template, folder_name, &rel);
                    let playlist_path = folder.join(&playlist_name);

                    // choose playlist mode
                    if cfg.playlist_mode == "flat" {
                        if let Err(e) = playlist::write_flat_playlist(&folder, &playlist_path, &cfg.playlist_order_mode) {
                            warn!("Failed to write playlist {:?}: {}", playlist_path, e);
                        }
                    } else {
                        if let Err(e) = playlist::write_linked_playlist(&folder, &playlist_path, &cfg.linked_reference_format, &cfg.local_playlist_template) {
                            warn!("Failed to write linked playlist {:?}: {}", playlist_path, e);
                        }
                    }

                    // enqueue a generic Create event for the playlist into DB
                    // Run DB mutations in a short-lived blocking thread so we don't block the worker loop
                    let playlist_name2 = playlist_name.clone();
                    let db_path2 = db_path.clone();
                    thread::spawn(move || {
                        if let Ok(conn) = db::open_or_create(std::path::Path::new(&db_path2)) {
                            if let Err(e) = db::enqueue_event(&conn, &playlist_name2, &EventAction::Create, None, None) {
                                warn!("Failed to enqueue event for {}: {}", playlist_name2, e);
                            }
                        } else {
                            warn!("Failed to open DB at {} to enqueue event", db_path2.display());
                        }
                    });
                }

                // small sleep to avoid busy-looping
                std::thread::sleep(Duration::from_millis(50));
            }
        });
    }

    // Now wire up notify to feed events into the in-memory tree and debounce map.
    {
        let debounce_map = debounce_map.clone();
        let tree = tree.clone();
        let cfg_cb = cfg.clone();
        let db_path = cfg_cb.db_path.clone();

        // Create a RecommendedWatcher that will call our closure for each FS event.
        let mut watcher: RecommendedWatcher = match RecommendedWatcher::new(
            move |res: NotifyResult<NotifyEvent>| {
                match res {
                    Ok(ev) => {
                        // convert notify::Event into synthetic events and apply
                        let mut synths: Vec<SyntheticEvent> = Vec::new();
                        // If multiple paths provided it's often a rename; handle that first.
                        if ev.paths.len() >= 2 {
                            let from = ev.paths[0].clone();
                            let to = ev.paths[1].clone();
                            // if both are directories, it's a folder rename/move
                            if from.is_dir() && to.is_dir() {
                                synths.push(SyntheticEvent::FolderRename { from, to });
                            } else {
                                synths.push(SyntheticEvent::FileRename { from, to });
                            }
                        } else {
                            for path in ev.paths.iter() {
                                let is_file = path.is_file();
                                let is_dir = path.is_dir();
                                match &ev.kind {
                                    EventKind::Create(_) => {
                                        if is_file {
                                            synths.push(SyntheticEvent::FileCreate(path.clone()));
                                        } else if is_dir {
                                            synths.push(SyntheticEvent::FolderCreate(path.clone()));
                                        }
                                    }
                                    EventKind::Remove(_) => {
                                        if is_file {
                                            synths.push(SyntheticEvent::FileRemove(path.clone()));
                                        } else if is_dir {
                                            synths.push(SyntheticEvent::FolderRemove(path.clone()));
                                        }
                                    }
                                    EventKind::Modify(_) => {
                                        if is_file {
                                            // treat modify as create/update of file
                                            synths.push(SyntheticEvent::FileCreate(path.clone()));
                                        }
                                    }
                                    _ => {}
                                }
                            }
                        }

                        if !synths.is_empty() {
                            // apply to in-memory tree and enqueue DB events
                            if let Ok(mut t) = tree.lock() {
                                for s in synths.into_iter() {
                                    let ops = t.apply_synthetic_event(s.clone());
                                    for op in ops {
                                        match op {
                                            LogicalOp::Add { playlist_folder, track_path } => {
                                                // set debounce for folder
                                                if let Ok(mut dm) = debounce_map.lock() {
                                                    dm.insert(playlist_folder.clone(), Instant::now() + Duration::from_millis(cfg_cb.debounce_ms));
                                                    // also propagate debounce to ancestor folders so linked playlists rebuild
                                                    if let Some(mut p) = playlist_folder.parent().map(|x| x.to_path_buf()) {
                                                        while p.starts_with(&cfg_cb.root_folder) {
                                                            if t.nodes.contains_key(&p) {
                                                                dm.insert(p.clone(), Instant::now() + Duration::from_millis(cfg_cb.debounce_ms));
                                                            }
                                                            if p == cfg_cb.root_folder { break; }
                                                            if let Some(np) = p.parent().map(|x| x.to_path_buf()) { p = np; } else { break; }
                                                        }
                                                    }
                                                }
                                                // enqueue add event
                                                let playlist_name = util::expand_template(&cfg_cb.local_playlist_template, playlist_folder.file_name().and_then(|s| s.to_str()).unwrap_or(""), &playlist_folder.strip_prefix(&cfg_cb.root_folder).unwrap_or(&playlist_folder).display().to_string());
                                                let db_path2 = db_path.clone();
                                                let pname = playlist_name.clone();
                                                let track = track_path.to_string_lossy().to_string();
                                                thread::spawn(move || {
                                                    if let Ok(conn) = db::open_or_create(std::path::Path::new(&db_path2)) {
                                                        if let Err(e) = db::enqueue_event(&conn, &pname, &EventAction::Add, Some(&track), None) {
                                                            warn!("Failed to enqueue add event: {}", e);
                                                        }
                                                    }
                                                });
                                            }
                                            LogicalOp::Remove { playlist_folder, track_path } => {
                                                if let Ok(mut dm) = debounce_map.lock() {
                                                    dm.insert(playlist_folder.clone(), Instant::now() + Duration::from_millis(cfg_cb.debounce_ms));
                                                    if let Some(mut p) = playlist_folder.parent().map(|x| x.to_path_buf()) {
                                                        while p.starts_with(&cfg_cb.root_folder) {
                                                            if t.nodes.contains_key(&p) {
                                                                dm.insert(p.clone(), Instant::now() + Duration::from_millis(cfg_cb.debounce_ms));
                                                            }
                                                            if p == cfg_cb.root_folder { break; }
                                                            if let Some(np) = p.parent().map(|x| x.to_path_buf()) { p = np; } else { break; }
                                                        }
                                                    }
                                                }
                                                let playlist_name = util::expand_template(&cfg_cb.local_playlist_template, playlist_folder.file_name().and_then(|s| s.to_str()).unwrap_or(""), &playlist_folder.strip_prefix(&cfg_cb.root_folder).unwrap_or(&playlist_folder).display().to_string());
                                                let db_path2 = db_path.clone();
                                                let pname = playlist_name.clone();
                                                let track = track_path.to_string_lossy().to_string();
                                                thread::spawn(move || {
                                                    if let Ok(conn) = db::open_or_create(std::path::Path::new(&db_path2)) {
                                                        if let Err(e) = db::enqueue_event(&conn, &pname, &EventAction::Remove, Some(&track), None) {
                                                            warn!("Failed to enqueue remove event: {}", e);
                                                        }
                                                    }
                                                });
                                            }
                                            LogicalOp::Create { playlist_folder } | LogicalOp::Delete { playlist_folder } => {
                                                if let Ok(mut dm) = debounce_map.lock() {
                                                    dm.insert(playlist_folder.clone(), Instant::now() + Duration::from_millis(cfg_cb.debounce_ms));
                                                    if let Some(mut p) = playlist_folder.parent().map(|x| x.to_path_buf()) {
                                                        while p.starts_with(&cfg_cb.root_folder) {
                                                            if t.nodes.contains_key(&p) {
                                                                dm.insert(p.clone(), Instant::now() + Duration::from_millis(cfg_cb.debounce_ms));
                                                            }
                                                            if p == cfg_cb.root_folder { break; }
                                                            if let Some(np) = p.parent().map(|x| x.to_path_buf()) { p = np; } else { break; }
                                                        }
                                                    }
                                                }
                                            }
                                            LogicalOp::PlaylistRename { from_folder, to_folder } => {
                                                // debounce both source and destination folders and ancestors
                                                if let Ok(mut dm) = debounce_map.lock() {
                                                    dm.insert(from_folder.clone(), Instant::now() + Duration::from_millis(cfg_cb.debounce_ms));
                                                    dm.insert(to_folder.clone(), Instant::now() + Duration::from_millis(cfg_cb.debounce_ms));
                                                    if let Some(mut p) = from_folder.parent().map(|x| x.to_path_buf()) {
                                                        while p.starts_with(&cfg_cb.root_folder) {
                                                            if t.nodes.contains_key(&p) {
                                                                dm.insert(p.clone(), Instant::now() + Duration::from_millis(cfg_cb.debounce_ms));
                                                            }
                                                            if p == cfg_cb.root_folder { break; }
                                                            if let Some(np) = p.parent().map(|x| x.to_path_buf()) { p = np; } else { break; }
                                                        }
                                                    }
                                                    if let Some(mut p) = to_folder.parent().map(|x| x.to_path_buf()) {
                                                        while p.starts_with(&cfg_cb.root_folder) {
                                                            if t.nodes.contains_key(&p) {
                                                                dm.insert(p.clone(), Instant::now() + Duration::from_millis(cfg_cb.debounce_ms));
                                                            }
                                                            if p == cfg_cb.root_folder { break; }
                                                            if let Some(np) = p.parent().map(|x| x.to_path_buf()) { p = np; } else { break; }
                                                        }
                                                    }
                                                }

                                                // enqueue a rename event (playlist rename) into DB: use old playlist name as key
                                                let folder_name_from = from_folder.file_name().and_then(|s| s.to_str()).unwrap_or("");
                                                let rel_from = from_folder.strip_prefix(&cfg_cb.root_folder).unwrap_or(&from_folder).display().to_string();
                                                let playlist_name_from = util::expand_template(&cfg_cb.local_playlist_template, folder_name_from, &rel_from);

                                                let folder_name_to = to_folder.file_name().and_then(|s| s.to_str()).unwrap_or("");
                                                let rel_to = to_folder.strip_prefix(&cfg_cb.root_folder).unwrap_or(&to_folder).display().to_string();
                                                let playlist_name_to = util::expand_template(&cfg_cb.local_playlist_template, folder_name_to, &rel_to);

                                                let extra = match serde_json::json!({"from": playlist_name_from, "to": playlist_name_to}).to_string() {
                                                    s => s,
                                                };

                                                let db_path2 = db_path.clone();
                                                let pname = playlist_name_from.clone();
                                                let extra_clone = extra.clone();
                                                thread::spawn(move || {
                                                    if let Ok(conn) = db::open_or_create(std::path::Path::new(&db_path2)) {
                                                        if let Err(e) = db::enqueue_event(&conn, &pname, &EventAction::Rename { from: playlist_name_from.clone(), to: playlist_name_to.clone() }, None, Some(&extra_clone)) {
                                                            warn!("Failed to enqueue rename event: {}", e);
                                                        }
                                                    }
                                                });
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    Err(e) => {
                        warn!("notify error: {:?}", e);
                    }
                }
            },
            NotifyConfig::default(),
        ) {
            Ok(w) => w,
            Err(e) => {
                warn!("Failed to create file watcher: {}", e);
                // fallthrough; return Ok so watcher process still runs initial playlists
                return Ok(());
            }
        };

        // start watching root folder recursively
        if let Err(e) = watcher.watch(&cfg.root_folder, RecursiveMode::Recursive) {
            warn!("Failed to start watcher for {:?}: {}", cfg.root_folder, e);
        }
        // keep watcher in scope; it will run for the lifetime of this function
    }

    // NOTE: notify watcher integration omitted in this minimal bootstrap; the long-running
    // watcher will initially write playlists and spawn debounce worker above. For full
    // functionality the notify::RecommendedWatcher should be created and events mapped to
    // debounce_map updates.

    Ok(())
}