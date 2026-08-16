use anyhow::{Context, Result, bail};
use macaw::session::{ProfileId, SessionConfig};
use std::path::Path;

pub fn load_directory(directory: &Path) -> Result<Vec<(ProfileId, SessionConfig)>> {
    let mut paths = std::fs::read_dir(directory)
        .with_context(|| format!("failed to read profiles directory {}", directory.display()))?
        .map(|entry| entry.map(|entry| entry.path()))
        .collect::<Result<Vec<_>, _>>()?;
    paths.sort();

    let mut profiles = Vec::new();
    for path in paths {
        if path.extension().and_then(|extension| extension.to_str()) != Some("toml") {
            continue;
        }
        if !path
            .metadata()
            .with_context(|| format!("failed to inspect profile {}", path.display()))?
            .is_file()
        {
            continue;
        }
        let stem = path
            .file_stem()
            .and_then(|stem| stem.to_str())
            .ok_or_else(|| anyhow::anyhow!("profile filename must be valid UTF-8"))?;
        let id = stem.parse::<ProfileId>().map_err(|error| {
            anyhow::anyhow!("invalid profile filename {}: {error}", path.display())
        })?;
        let content = std::fs::read_to_string(&path)
            .with_context(|| format!("failed to read profile {}", path.display()))?;
        let mut config: SessionConfig = toml::from_str(&content)
            .with_context(|| format!("failed to parse profile {}", path.display()))?;
        config.root = path.parent().unwrap_or_else(|| Path::new("")).to_path_buf();
        if profiles
            .iter()
            .any(|(existing, _): &(ProfileId, SessionConfig)| existing == &id)
        {
            bail!("duplicate profile id {id}");
        }
        profiles.push((id, config));
    }
    Ok(profiles)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(feature = "http")]
    #[test]
    fn loads_sorted_toml_profiles_with_file_relative_roots() {
        let directory = tempfile::tempdir().unwrap();
        std::fs::write(
            directory.path().join("beta.toml"),
            r#"
                [proxies.api]
                type = "http"
                target = "https://example.com"
            "#,
        )
        .unwrap();
        std::fs::write(
            directory.path().join("alpha.toml"),
            r#"
                [proxies.api]
                type = "http"
                target = "https://example.com"
            "#,
        )
        .unwrap();
        std::fs::write(directory.path().join("ignored.txt"), "not TOML").unwrap();

        let profiles = load_directory(directory.path()).unwrap();
        assert_eq!(profiles.len(), 2);
        assert_eq!(profiles[0].0.to_string(), "alpha");
        assert_eq!(profiles[1].0.to_string(), "beta");
        assert_eq!(profiles[0].1.root, directory.path());
    }

    #[test]
    fn rejects_invalid_profile_filenames_and_toml() {
        let directory = tempfile::tempdir().unwrap();
        std::fs::write(directory.path().join("invalid name.toml"), "[proxies]").unwrap();
        assert!(load_directory(directory.path()).is_err());

        std::fs::remove_file(directory.path().join("invalid name.toml")).unwrap();
        std::fs::write(directory.path().join("valid.toml"), "not = [valid").unwrap();
        assert!(load_directory(directory.path()).is_err());
    }

    #[test]
    fn rejects_missing_profile_directory() {
        let directory = tempfile::tempdir().unwrap();
        let missing = directory.path().join("missing");
        let error = load_directory(&missing).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("failed to read profiles directory")
        );
    }
}
