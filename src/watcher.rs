use log::{info, warn};
use std::sync::{Arc, Mutex};
use crate::config::Config;
use crate::db;
use crate::playlist;
use crate::util;
use crate::models::EventAction;
use anyhow::Context;
use notify::{Config as NotifyConfig, Event as NotifyEvent, EventKind, RecommendedWatcher, RecursiveMode, Result as NotifyResult, Watcher};
use notify::event::RemoveKind;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, Instant};
use walkdir::WalkDir;
use regex::Regex;

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
    /// Optional list of regex patterns applied to folder paths (as strings).
    /// If set, only folders whose full path matches at least one pattern are included.
    pub whitelist: Option<Vec<Regex>>,
}

/// Return true if the given path's extension matches any of the configured
/// file_extensions patterns ("*.mp3", "mp3", ".mp3"), case-insensitive.
fn path_matches_extensions(path: &Path, exts: &[String]) -> bool {
    let ext_os = if let Some(e) = path.extension() {
        e
    } else {
        return false;
    };
    let ext = if let Some(s) = ext_os.to_str() {
        s.to_ascii_lowercase()
    } else {
        return false;
    };
    for pat in exts {
        let mut p = pat.trim();
        if p.is_empty() {
            continue;
        }
        // strip common prefixes: "*." or "."
        if let Some(stripped) = p.strip_prefix("*.") {
            p = stripped;
        } else if let Some(stripped) = p.strip_prefix('.') {
            p = stripped;
        }
        if ext == p.to_ascii_lowercase() {
            return true;
        }
    }
    false
}

/// Return true if the path lies inside a Samba temporary folder such as
/// ".::TMPNAME:...", which should be ignored for playlist purposes.
fn is_smb_temp_path(path: &Path) -> bool {
    use std::path::Component;

    for comp in path.components() {
        if let Component::Normal(os) = comp {
            if let Some(s) = os.to_str() {
                if s.starts_with(".::TMPNAME:") {
                    return true;
                }
            }
        }
    }
    false
}

impl InMemoryTree {
    /// Build the tree by scanning the filesystem under root.
    /// - If `whitelist` is Some, it is treated as a colon-separated list of regex patterns
    ///   evaluated against the full folder path (e.g. "/raid/.../My Folder"). Only
    ///   directories whose path matches at least one pattern are included.
    /// - Only files whose extensions match the optional file_extensions whitelist are kept.
    pub fn build(root: &Path, whitelist: Option<&str>, file_extensions: Option<&[String]>) -> anyhow::Result<Self> {
        let wl = whitelist.map(|s| {
            s.split(':')
                .filter_map(|p| {
                    let pat = p.trim();
                    if pat.is_empty() {
                        return None;
                    }
                    match Regex::new(pat) {
                        Ok(re) => Some(re),
                        Err(e) => {
                            warn!("Invalid whitelist regex pattern {:?}: {}", pat, e);
                            None
                        }
                    }
                })
                .collect::<Vec<_>>()
        });

        let mut nodes: HashMap<PathBuf, FolderNode> = HashMap::new();

        let walker = WalkDir::new(root).follow_links(false).min_depth(0);

        for entry in walker.into_iter().filter_map(|e| e.ok()) {
            let path = entry.path().to_path_buf();
            if is_smb_temp_path(&path) {
                continue;
            }
            if entry.file_type().is_dir() {
                // if whitelist exists, skip dirs whose full path doesn't match any regex
                if let Some(ref wlvec) = wl {
                    let path_str = path.to_string_lossy();
                    if !wlvec.iter().any(|re| re.is_match(&path_str)) {
                        continue;
                    }
                }
                nodes.entry(path.clone()).or_insert_with(|| FolderNode::new(path));
            } else if entry.file_type().is_file() {
                // If a file_extensions whitelist is provided, only include matching media files.
                let allowed = if let Some(exts) = file_extensions {
                    path_matches_extensions(&path, exts)
                } else {
                    true
                };
                if allowed {
                    if let Some(parent) = entry.path().parent() {
                        let parent = parent.to_path_buf();
                        let node = nodes.entry(parent.clone()).or_insert_with(|| FolderNode::new(parent.clone()));
                        node.tracks.insert(path.clone());
                    }
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
        let mut p = if path.is_dir() {
            path.to_path_buf()
        } else {
            path.parent().map(|x| x.to_path_buf())?
        };

        loop {
            // Only consider paths under the configured root.
            if !p.starts_with(&self.root) {
                break;
            }

            // If we already have a node for this folder, use it.
            if self.nodes.contains_key(&p) {
                return Some(p.clone());
            }

            // Otherwise, if this folder is allowed by the whitelist (or there
            // is no whitelist), treat it as a valid playlist folder even if we
            // haven't created a node yet. The caller will then create the node
            // on demand.
            let allowed = if let Some(ref wlvec) = self.whitelist {
                let path_str = p.to_string_lossy();
                wlvec.iter().any(|re| re.is_match(&path_str))
            } else {
                true
            };

            if allowed {
                return Some(p.clone());
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
    let _conn = db::open_or_create(&cfg.db_path)
        .with_context(|| format!("opening or creating DB at {}", cfg.db_path.display()))?;

    // Build initial in-memory tree, respecting optional whitelist and file_extensions
    let tree = InMemoryTree::build(
        &cfg.root_folder,
        if cfg.whitelist.is_empty() { None } else { Some(&cfg.whitelist) },
        Some(&cfg.file_extensions),
    )
    .with_context(|| format!("building in-memory tree from root {}", cfg.root_folder.display()))?;
    info!("Initial scan complete: {} folders", tree.nodes.len());

    // Initial playlist writes (flat mode)
    for (folder, _node) in tree.nodes.iter() {
        let folder_name = folder.file_name().and_then(|s| s.to_str()).unwrap_or("");
        let rel = folder.strip_prefix(&cfg.root_folder).unwrap_or(folder).to_path_buf();

        // Local template uses folder_name and path_to_parent; the logical
        // playlist key used in the DB is the folder path relative to root
        // (rel.display()).
        let path_to_parent = rel.parent().map(|p| p.to_path_buf()).unwrap_or_else(|| std::path::PathBuf::new());
        let path_to_parent_str = if path_to_parent.as_os_str().is_empty() {
            String::new()
        } else {
            let mut s = path_to_parent.display().to_string();
            if !s.ends_with(std::path::MAIN_SEPARATOR) {
                s.push(std::path::MAIN_SEPARATOR);
            }
            s
        };

        let playlist_name = util::expand_template(&cfg.local_playlist_template, folder_name, &path_to_parent_str);
        let playlist_path = folder.join(playlist_name);
        if cfg.playlist_mode == "flat" {
            if let Err(e) = playlist::write_flat_playlist(folder, &playlist_path, &cfg.playlist_order_mode, &cfg.file_extensions) {
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
    let _debounce_ms = cfg.debounce_ms;

    // Wrap in-memory tree in Arc<Mutex<...>> so notify callback can update it concurrently
    let tree = Arc::new(Mutex::new(tree));

    // Spawn debounce worker thread: writes playlists when their debounce timer elapses and enqueues
    // a generic Create event for the playlist.
    {
        let debounce_map = debounce_map.clone();
        let cfg = cfg.clone();
        let db_path = cfg.db_path.clone();
        let _tree = tree.clone();
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
                    let rel = folder.strip_prefix(&cfg.root_folder).unwrap_or(&folder).to_path_buf();

                    let path_to_parent = rel.parent().map(|p| p.to_path_buf()).unwrap_or_else(|| std::path::PathBuf::new());
                    let path_to_parent_str = if path_to_parent.as_os_str().is_empty() {
                        String::new()
                    } else {
                        let mut s = path_to_parent.display().to_string();
                        if !s.ends_with(std::path::MAIN_SEPARATOR) {
                            s.push(std::path::MAIN_SEPARATOR);
                        }
                        s
                    };

                    let playlist_name = util::expand_template(&cfg.local_playlist_template, folder_name, &path_to_parent_str);
                    let playlist_path = folder.join(&playlist_name);

                    // choose playlist mode
                    if cfg.playlist_mode == "flat" {
                        if let Err(e) = playlist::write_flat_playlist(&folder, &playlist_path, &cfg.playlist_order_mode, &cfg.file_extensions) {
                            warn!("Failed to write playlist {:?}: {}", playlist_path, e);
                        }
                    } else {
                        if let Err(e) = playlist::write_linked_playlist(&folder, &playlist_path, &cfg.linked_reference_format, &cfg.local_playlist_template) {
                            warn!("Failed to write linked playlist {:?}: {}", playlist_path, e);
                        }
                    }

                    // enqueue a generic Create event for the playlist into DB
                    // Run DB mutations in a short-lived blocking thread so we don't block the worker loop
                    // Use the folder path relative to root as the logical
                    // playlist key for the event queue.
                    let playlist_name2 = rel.display().to_string();
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
    let debounce_map_cb = debounce_map.clone();
    let tree_cb = tree.clone();
    let cfg_cb = cfg.clone();
    let db_path = cfg_cb.db_path.clone();

    // Create a RecommendedWatcher that will call our closure for each FS event.
    let mut watcher: RecommendedWatcher = match RecommendedWatcher::new(
            move |res: NotifyResult<NotifyEvent>| {
                match res {
                    Ok(ev) => {
                        info!("NotifyEvent received: kind={:?}, paths={:?}, attrs={:?}", ev.kind, ev.paths, ev.attrs);
                        // convert notify::Event into synthetic events and apply
                        let mut synths: Vec<SyntheticEvent> = Vec::new();
                        // If multiple paths provided it's often a rename; try to distinguish
                        // folder vs file rename using the in-memory tree when possible.
                        if ev.paths.len() >= 2 {
                            let from = ev.paths[0].clone();
                            let to = ev.paths[1].clone();

                            // Ignore Samba temporary paths entirely.
                            if is_smb_temp_path(&from) || is_smb_temp_path(&to) {
                                return;
                            }

                            let mut treat_as_folder_rename = false;
                            if let Ok(t) = tree_cb.lock() {
                                if t.nodes.contains_key(&from) || t.nodes.contains_key(&to) {
                                    treat_as_folder_rename = true;
                                }
                            }

                            if treat_as_folder_rename {
                                synths.push(SyntheticEvent::FolderRename { from, to });
                            } else {
                                synths.push(SyntheticEvent::FileRename { from, to });
                            }
                        } else {
                            for path in ev.paths.iter() {
                                if is_smb_temp_path(path) {
                                    continue;
                                }
                                let is_file = path.is_file();
                                let is_dir = path.is_dir();

                                match &ev.kind {
                                    EventKind::Create(_) => {
                                        if is_file {
                                            // Only treat matching media files as track events
                                            if path_matches_extensions(path, &cfg_cb.file_extensions) {
                                                synths.push(SyntheticEvent::FileCreate(path.clone()));
                                            }
                                        } else if is_dir {
                                            synths.push(SyntheticEvent::FolderCreate(path.clone()));
                                        }
                                    }
                                    EventKind::Remove(remove_kind) => {
                                        if is_file {
                                            if path_matches_extensions(path, &cfg_cb.file_extensions) {
                                                synths.push(SyntheticEvent::FileRemove(path.clone()));
                                            }
                                        } else if is_dir {
                                            synths.push(SyntheticEvent::FolderRemove(path.clone()));
                                        } else {
                                            // After a remove, the path may no longer exist on disk,
                                            // so fall back to the RemoveKind from notify.
                                            match remove_kind {
                                                RemoveKind::File | RemoveKind::Any => {
                                                    if path_matches_extensions(path, &cfg_cb.file_extensions) {
                                                        synths.push(SyntheticEvent::FileRemove(path.clone()));
                                                    }
                                                }
                                                RemoveKind::Folder => {
                                                    synths.push(SyntheticEvent::FolderRemove(path.clone()));
                                                }
                                                _ => {}
                                            }
                                        }
                                    }
                                    EventKind::Modify(_) => {
                                        if is_file {
                                            // treat modify as create/update of file
                                            if path_matches_extensions(path, &cfg_cb.file_extensions) {
                                                synths.push(SyntheticEvent::FileCreate(path.clone()));
                                            }
                                        }
                                    }
                                    _ => {}
                                }
                            }
                        }
                        if !synths.is_empty() {
                            info!("Applying {} synthetic event(s) derived from NotifyEvent", synths.len());
                            // apply to in-memory tree and enqueue DB events
                            if let Ok(mut t) = tree_cb.lock() {
                                for s in synths.into_iter() {
                                    let ops = t.apply_synthetic_event(s.clone());
                                    if !ops.is_empty() {
                                        info!("InMemoryTree produced {} logical op(s) for synthetic event {:?}", ops.len(), s);
                                    }
                                    for op in ops {
                                        match op {
                                            LogicalOp::Add { playlist_folder, track_path } => {
                                                // Respect folder whitelist before enqueuing events
                                                if let Some(ref wlvec) = t.whitelist {
                                                    let path_str = playlist_folder.to_string_lossy();
                                                    if !wlvec.iter().any(|re| re.is_match(&path_str)) {
                                                        continue;
                                                    }
                                                }
                                                info!("LogicalOp::Add playlist_folder={:?}, track_path={:?}", playlist_folder, track_path);

                                                // Build the list of playlist folders that should reflect this
                                                // track change: the immediate folder plus any ancestor folders
                                                // that are represented as playlist nodes (so parent playlists
                                                // stay in sync online as well).
                                                let mut target_folders: Vec<std::path::PathBuf> = Vec::new();
                                                target_folders.push(playlist_folder.clone());
                                                if let Some(mut p) = playlist_folder.parent().map(|x| x.to_path_buf()) {
                                                    while p.starts_with(&cfg_cb.root_folder) {
                                                        if t.nodes.contains_key(&p) {
                                                            target_folders.push(p.clone());
                                                        }
                                                        if p == cfg_cb.root_folder { break; }
                                                        if let Some(np) = p.parent().map(|x| x.to_path_buf()) { p = np; } else { break; }
                                                    }
                                                }

                                                // Debounce playlist rewrite for all affected folders
                                                if let Ok(mut dm) = debounce_map_cb.lock() {
                                                    for folder in &target_folders {
                                                        dm.insert(folder.clone(), Instant::now() + Duration::from_millis(cfg_cb.debounce_ms));
                                                    }
                                                }

                                                // Enqueue add events for the immediate folder and all parent
                                                // playlists so that remote parent playlists receive the track
                                                // updates as well.
                                                let db_path2 = db_path.clone();
                                                let root_folder = cfg_cb.root_folder.clone();
                                                let track = track_path.to_string_lossy().to_string();
                                                let playlist_names: Vec<String> = target_folders
                                                    .iter()
                                                    .map(|folder| {
                                                        folder
                                                            .strip_prefix(&root_folder)
                                                            .unwrap_or(folder)
                                                            .display()
                                                            .to_string()
                                                    })
                                                    .collect();
                                                thread::spawn(move || {
                                                    if let Ok(conn) = db::open_or_create(std::path::Path::new(&db_path2)) {
                                                        for pname in playlist_names {
                                                            if let Err(e) = db::enqueue_event(&conn, &pname, &EventAction::Add, Some(&track), None) {
                                                                warn!("Failed to enqueue add event for {}: {}", pname, e);
                                                            }
                                                        }
                                                    }
                                                });
                                            }
                                            LogicalOp::Remove { playlist_folder, track_path } => {
                                                if let Some(ref wlvec) = t.whitelist {
                                                    let path_str = playlist_folder.to_string_lossy();
                                                    if !wlvec.iter().any(|re| re.is_match(&path_str)) {
                                                        continue;
                                                    }
                                                }
                                                info!("LogicalOp::Remove playlist_folder={:?}, track_path={:?}", playlist_folder, track_path);

                                                let mut target_folders: Vec<std::path::PathBuf> = Vec::new();
                                                target_folders.push(playlist_folder.clone());
                                                if let Some(mut p) = playlist_folder.parent().map(|x| x.to_path_buf()) {
                                                    while p.starts_with(&cfg_cb.root_folder) {
                                                        if t.nodes.contains_key(&p) {
                                                            target_folders.push(p.clone());
                                                        }
                                                        if p == cfg_cb.root_folder { break; }
                                                        if let Some(np) = p.parent().map(|x| x.to_path_buf()) { p = np; } else { break; }
                                                    }
                                                }

                                                if let Ok(mut dm) = debounce_map_cb.lock() {
                                                    for folder in &target_folders {
                                                        dm.insert(folder.clone(), Instant::now() + Duration::from_millis(cfg_cb.debounce_ms));
                                                    }
                                                }

                                                let db_path2 = db_path.clone();
                                                let root_folder = cfg_cb.root_folder.clone();
                                                let track = track_path.to_string_lossy().to_string();
                                                let playlist_names: Vec<String> = target_folders
                                                    .iter()
                                                    .map(|folder| {
                                                        folder
                                                            .strip_prefix(&root_folder)
                                                            .unwrap_or(folder)
                                                            .display()
                                                            .to_string()
                                                    })
                                                    .collect();
                                                thread::spawn(move || {
                                                    if let Ok(conn) = db::open_or_create(std::path::Path::new(&db_path2)) {
                                                        for pname in playlist_names {
                                                            if let Err(e) = db::enqueue_event(&conn, &pname, &EventAction::Remove, Some(&track), None) {
                                                                warn!("Failed to enqueue remove event for {}: {}", pname, e);
                                                            }
                                                        }
                                                    }
                                                });
                                            }
                                            LogicalOp::Create { playlist_folder } => {
                                                info!("LogicalOp::Create playlist_folder={:?}", playlist_folder);
                                                if let Ok(mut dm) = debounce_map_cb.lock() {
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
                                            LogicalOp::Delete { playlist_folder } => {
                                                if let Some(ref wlvec) = t.whitelist {
                                                    let path_str = playlist_folder.to_string_lossy();
                                                    if !wlvec.iter().any(|re| re.is_match(&path_str)) {
                                                        continue;
                                                    }
                                                }
                                                info!("LogicalOp::Delete playlist_folder={:?}", playlist_folder);
                                                // For deletes, debounce only ancestor folders (for linked playlists),
                                                // and enqueue a Delete event for the removed playlist itself.
                                                if let Ok(mut dm) = debounce_map_cb.lock() {
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

                                                // Enqueue a Delete event so the worker can eventually delete
                                                // the corresponding remote playlist.
                                                let db_path2 = db_path.clone();
                                                let pname = playlist_folder
                                                    .strip_prefix(&cfg_cb.root_folder)
                                                    .unwrap_or(&playlist_folder)
                                                    .display()
                                                    .to_string();
                                                thread::spawn(move || {
                                                    if let Ok(conn) = db::open_or_create(std::path::Path::new(&db_path2)) {
                                                        if let Err(e) = db::enqueue_event(&conn, &pname, &EventAction::Delete, None, None) {
                                                            warn!("Failed to enqueue delete event: {}", e);
                                                        }
                                                    }
                                                });
                                            }
                                            LogicalOp::PlaylistRename { from_folder, to_folder } => {
                                                // Use the source folder to decide whether this playlist
                                                // should be tracked at all.
                                                if let Some(ref wlvec) = t.whitelist {
                                                    let path_str = from_folder.to_string_lossy();
                                                    if !wlvec.iter().any(|re| re.is_match(&path_str)) {
                                                        continue;
                                                    }
                                                }
                                                info!("LogicalOp::PlaylistRename from_folder={:?}, to_folder={:?}", from_folder, to_folder);

                                                // Rename the local playlist file on disk so that we don't
                                                // leave behind a stale playlist with the old folder name.
                                                let from_rel = from_folder
                                                    .strip_prefix(&cfg_cb.root_folder)
                                                    .unwrap_or(&from_folder)
                                                    .to_path_buf();
                                                let to_rel = to_folder
                                                    .strip_prefix(&cfg_cb.root_folder)
                                                    .unwrap_or(&to_folder)
                                                    .to_path_buf();

                                                let from_folder_name = from_folder.file_name().and_then(|s| s.to_str()).unwrap_or("");
                                                let to_folder_name = to_folder.file_name().and_then(|s| s.to_str()).unwrap_or("");

                                                let from_parent = from_rel.parent().map(|p| p.to_path_buf()).unwrap_or_else(|| std::path::PathBuf::new());
                                                let to_parent = to_rel.parent().map(|p| p.to_path_buf()).unwrap_or_else(|| std::path::PathBuf::new());

                                                let from_parent_str = if from_parent.as_os_str().is_empty() {
                                                    String::new()
                                                } else {
                                                    let mut s = from_parent.display().to_string();
                                                    if !s.ends_with(std::path::MAIN_SEPARATOR) {
                                                        s.push(std::path::MAIN_SEPARATOR);
                                                    }
                                                    s
                                                };

                                                let to_parent_str = if to_parent.as_os_str().is_empty() {
                                                    String::new()
                                                } else {
                                                    let mut s = to_parent.display().to_string();
                                                    if !s.ends_with(std::path::MAIN_SEPARATOR) {
                                                        s.push(std::path::MAIN_SEPARATOR);
                                                    }
                                                    s
                                                };

                                                let from_playlist_name = util::expand_template(&cfg_cb.local_playlist_template, from_folder_name, &from_parent_str);
                                                let to_playlist_name = util::expand_template(&cfg_cb.local_playlist_template, to_folder_name, &to_parent_str);

                                                let from_playlist_path = from_folder.join(&from_playlist_name);
                                                let to_playlist_path = to_folder.join(&to_playlist_name);

                                                if from_playlist_path != to_playlist_path && from_playlist_path.exists() {
                                                    if let Err(e) = std::fs::rename(&from_playlist_path, &to_playlist_path) {
                                                        warn!("Failed to rename playlist file {:?} -> {:?}: {}", from_playlist_path, to_playlist_path, e);
                                                    }
                                                }
                                                // debounce both source and destination folders and ancestors
                                                if let Ok(mut dm) = debounce_map_cb.lock() {
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
                                                let rel_from = from_folder
                                                    .strip_prefix(&cfg_cb.root_folder)
                                                    .unwrap_or(&from_folder)
                                                    .display()
                                                    .to_string();
                                                let playlist_name_from = rel_from.clone();

                                                let rel_to = to_folder
                                                    .strip_prefix(&cfg_cb.root_folder)
                                                    .unwrap_or(&to_folder)
                                                    .display()
                                                    .to_string();
                                                let playlist_name_to = rel_to.clone();

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
    } else {
        info!("File watcher started on root {:?}", cfg.root_folder);
    }
    // keep watcher in scope; it will run for the lifetime of this function

    // Block indefinitely so the watcher process stays alive and can
    // continue receiving filesystem events.
    loop {
        std::thread::sleep(Duration::from_secs(60));
    }
}