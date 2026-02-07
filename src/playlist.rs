use std::path::{Path, PathBuf};
use walkdir::WalkDir;

/// Return true if the given path's extension matches any of the configured
/// file_extensions patterns ("*.mp3", "mp3", ".mp3"), case-insensitive.
fn path_matches_extensions(path: &Path, exts: &[String]) -> bool {
    let ext_os = match path.extension() {
        Some(e) => e,
        None => return false,
    };
    let ext = match ext_os.to_str() {
        Some(s) => s.to_ascii_lowercase(),
        None => return false,
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

/// Write a flat .m3u playlist for folder: all matching media files recursively.
///
/// Behavior is aligned with the original shell script implementation:
/// - Only files whose extensions match `file_extensions` are included.
/// - Files are flattened from the subtree rooted at `target_folder`.
/// - Playlist uses M3U with `#EXTM3U` header and `#EXTINF` metadata lines.
/// - Paths inside the playlist are relative to `target_folder`.
pub fn write_flat_playlist(
    target_folder: &Path,
    playlist_path: &Path,
    order_mode: &str,
    file_extensions: &[String],
) -> anyhow::Result<()> {
    use std::io::Write;

    let mut files: Vec<PathBuf> = WalkDir::new(target_folder)
        .into_iter()
        .filter_map(|e| e.ok())
        .map(|e| e.path().to_path_buf())
        .filter(|p| p.is_file())
        .filter(|p| path_matches_extensions(p, file_extensions))
        .collect();

    if order_mode == "sync_order" {
        // sort by modification time ascending
        files.sort_by_key(|p| {
            std::fs::metadata(p)
                .and_then(|m| m.modified())
                .unwrap_or(std::time::SystemTime::UNIX_EPOCH)
        });
    } else {
        // default: alphabetical
        files.sort();
    }

    let mut file = std::fs::File::create(playlist_path)?;

    // M3U header
    writeln!(file, "#EXTM3U")?;

    for p in files.iter() {
        let title = p
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or("");

        let relpath = pathdiff::diff_paths(p, target_folder).unwrap_or_else(|| p.clone());

        writeln!(file, "#EXTINF:-1,{}", title)?;
        writeln!(file, "{}", relpath.display())?;
    }

    Ok(())
}

/// For linked mode, create a playlist that references direct children playlists (not implemented in prototype)
pub fn write_linked_playlist(target_folder: &Path, playlist_path: &Path, linked_reference_format: &str, local_playlist_template: &str) -> anyhow::Result<()> {
    // write references to immediate child playlists
    let mut file = std::fs::File::create(playlist_path)?;
    let mut children: Vec<std::path::PathBuf> = Vec::new();
    if let Ok(read) = std::fs::read_dir(target_folder) {
        for e in read.filter_map(|r| r.ok()) {
            let p = e.path();
            if p.is_dir() {
                children.push(p);
            }
        }
    }
    children.sort();

    use std::io::Write;
    for child in children.iter() {
        // child playlist filename based on template; for linked playlists,
        // the logical parent is the current target_folder, so path_to_parent
        // is empty and folder_name identifies the child.
        let folder_name = child.file_name().and_then(|s| s.to_str()).unwrap_or("");
        let path_to_parent = String::new();
        let child_playlist_name = crate::util::expand_template(local_playlist_template, folder_name, &path_to_parent);
        let child_playlist_path = child.join(child_playlist_name);
        let line = if linked_reference_format == "absolute" {
            child_playlist_path.display().to_string()
        } else {
            // relative
            let relpath = pathdiff::diff_paths(&child_playlist_path, target_folder).unwrap_or(child_playlist_path.clone());
            relpath.display().to_string()
        };
        writeln!(file, "{}", line)?;
    }
    Ok(())
}