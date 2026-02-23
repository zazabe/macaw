use serde::de::DeserializeOwned;
use std::path::Path;

/// Load and parse a file as JSON or YAML.
pub fn parse_json_or_yaml<T>(path: &Path) -> Result<T, anyhow::Error>
where
    T: DeserializeOwned,
{
    let content = fs_err::read_to_string(path)?;
    let ext = path.extension().and_then(|e| e.to_str());

    let value = match ext {
        Some("json") => serde_json::from_str(&content)?,
        Some("yaml") | Some("yml") => serde_yaml::from_str(&content)?,
        _ => serde_json::from_str(&content).or_else(|_| serde_yaml::from_str(&content))?,
    };

    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;

    #[derive(Debug, Deserialize, PartialEq)]
    struct SimpleConfig {
        key: String,
        value: i32,
    }

    #[test]
    fn test_parse_json_or_yaml_by_extension() {
        let json = r#"{"key": "test", "value": 42}"#;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.json");
        std::fs::write(&path, json).unwrap();

        let config: SimpleConfig = parse_json_or_yaml(&path).unwrap();
        assert_eq!(config.key, "test");
        assert_eq!(config.value, 42);

        let yaml = "key: yaml\nvalue: 20";
        let path = dir.path().join("config.yaml");
        std::fs::write(&path, yaml).unwrap();

        let config: SimpleConfig = parse_json_or_yaml(&path).unwrap();
        assert_eq!(config.key, "yaml");
        assert_eq!(config.value, 20);
    }

    #[test]
    fn test_parse_json_or_yaml_fallback_when_no_extension() {
        let json = r#"{"key": "json", "value": 10}"#;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config");
        std::fs::write(&path, json).unwrap();

        let config: SimpleConfig = parse_json_or_yaml(&path).unwrap();
        assert_eq!(config.key, "json");
        assert_eq!(config.value, 10);

        let yaml = "key: yaml\nvalue: 20";
        std::fs::write(&path, yaml).unwrap();

        let config: SimpleConfig = parse_json_or_yaml(&path).unwrap();
        assert_eq!(config.key, "yaml");
        assert_eq!(config.value, 20);
    }
}
