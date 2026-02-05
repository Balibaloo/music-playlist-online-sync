pub fn expand_template(template: &str, folder_name: &str, relative_path: &str) -> String {
    template.replace("${folder_name}", folder_name).replace("${relative_path}", relative_path)
}