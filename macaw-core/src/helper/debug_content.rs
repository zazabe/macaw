//! Helper to format content for single-line debug output.

/// Convert content to a single line for debug display.
/// - If valid pretty JSON: parse and output compact (unprettified)
/// - Otherwise: replace newlines with literal "\n"
pub fn to_single_line(s: &str) -> String {
    let trimmed = s.trim();
    if trimmed.is_empty() {
        return s.to_string();
    }
    // Try to parse as JSON - if valid, compact it
    if let Ok(value) = serde_json::from_str::<serde_json::Value>(trimmed)
        && let Ok(compact) = serde_json::to_string(&value)
    {
        return compact;
    }
    // Not valid JSON: replace newlines with literal \n
    s.replace("\r\n", "\\n").replace(['\n', '\r'], "\\n")
}
