use std::path::Path;
use walkdir::WalkDir;

/// Write a simple flat .m3u playlist for folder: all files recursively (prototype).
pub fn write_flat_playlist(target_folder: &Path, playlist_path: &Path, order_mode: &str) -> anyhow::Result<()> {
    let mut file = std::fs::File::create(playlist_path)?;
    let mut files: Vec<std::path::PathBuf> = WalkDir::new(target_folder)
        .into_iter()
        .filter_map(|e| e.ok())
        .map(|e| e.path().to_path_buf())
        .filter(|p| p.is_file())
        .collect();

    if order_mode == "sync_order" {
        // sort by modification time ascending
        files.sort_by_key(|p| std::fs::metadata(p).and_then(|m| m.modified()).map(|t| t).unwrap_or(std::time::SystemTime::UNIX_EPOCH));
    } else {
        // default: alphabetical
        files.sort();
    }

    use std::io::Write;
    for p in files.iter() {
        writeln!(file, "{}", p.display())?;
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
        // child playlist filename based on template
        let folder_name = child.file_name().and_then(|s| s.to_str()).unwrap_or("");
        let rel = child.display().to_string();
        let child_playlist_name = crate::util::expand_template(local_playlist_template, folder_name, &rel);
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