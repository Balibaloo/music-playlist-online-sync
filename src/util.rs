pub fn expand_template(template: &str, folder_name: &str, path_to_parent: &str) -> String {
    // `path_to_parent` is intended to be the logical path from the
    // configured root to the playlist folder's *parent* (or equivalent
    // logical parent for remote playlists). Callers are responsible for
    // choosing whether this is filesystem-style ("Artist/") or already
    // flattened with the online delimiter ("Syncd| Artist| ").
    //
    // For backwards compatibility, we still expand the legacy
    // "${relative_path}" placeholder as the full logical path to the
    // playlist folder itself, i.e. `path_to_parent + folder_name`.
    let full_path = format!("{}{}", path_to_parent, folder_name);

    template
        .replace("${folder_name}", folder_name)
        .replace("${path_to_parent}", path_to_parent)
        .replace("${relative_path}", &full_path)
}

/// Attempt to extract an ISRC code from the audio file's metadata tags.
/// Returns None if the file has no readable tags or no ISRC field.
pub fn extract_isrc_from_path(path: &std::path::Path) -> Option<String> {
    use lofty::file::TaggedFileExt;
    use lofty::probe::read_from_path;
    use lofty::tag::{ItemKey, Tag};

    let tagged_file = match read_from_path(path) {
        Ok(tf) => tf,
        Err(_) => return None,
    };

    let tag: Option<Tag> = tagged_file
        .primary_tag()
        .cloned()
        .or_else(|| tagged_file.first_tag().cloned());

    let tag = match tag {
        Some(t) => t,
        None => return None,
    };

    tag.get_string(&ItemKey::Isrc).map(|s| s.to_string())
}

/// Compute a lightweight fingerprint for the given file by hashing its
/// entire contents with SHA256.  The value is used to detect whether a
/// playlist file has been modified in a way that might not bump its
/// modification time (e.g. an editor that rewrites the file in place).
///
/// The implementation is intentionally simple in order to minimise
/// dependencies and race conditions; callers can cheaply recompute the
/// hash on every worker run.
pub fn hash_file(path: &std::path::Path) -> anyhow::Result<String> {
    use sha2::{Digest, Sha256};
    use std::fs::File;
    use std::io::{BufReader, Read};

    let file = File::open(path)?;
    let mut reader = BufReader::new(file);
    let mut hasher = Sha256::new();
    let mut buf = [0u8; 8192];
    loop {
        let n = reader.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(format!("{:x}", hasher.finalize()))
}
