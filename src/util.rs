pub fn expand_template(template: &str, folder_name: &str, relative_path: &str) -> String {
    template.replace("${folder_name}", folder_name).replace("${relative_path}", relative_path)
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
