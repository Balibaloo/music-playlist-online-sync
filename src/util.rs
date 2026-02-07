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
