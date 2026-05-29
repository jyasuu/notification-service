//! Terminal output helpers.
//!
//! All commands produce either a pretty table (default) or JSON (`--output json`).
//! Both modes write to stdout so results can be piped.

use tabled::{Table, Tabled};

/// Print a slice of tabled structs as a formatted table.
pub fn print_table<T: Tabled>(rows: &[T]) {
    if rows.is_empty() {
        println!("(no results)");
        return;
    }
    println!("{}", Table::new(rows));
}

/// Print a value as pretty-printed JSON.
pub fn print_json<T: serde::Serialize>(value: &T) {
    println!(
        "{}",
        serde_json::to_string_pretty(value).unwrap_or_else(|e| format!("{{\"error\":\"{e}\"}}"))
    );
}

/// Truncate a string for display, appending "…" if it was cut.
///
/// `max` is a **character** count, not a byte count.  Slicing by raw byte
/// index (`&s[..max]`) panics when `max` falls inside a multi-byte UTF-8
/// sequence (e.g. CJK characters, accented letters, emoji in error messages
/// or template payloads).
pub fn truncate(s: &str, max: usize) -> String {
    let mut chars = s.chars();
    let truncated: String = chars.by_ref().take(max).collect();
    if chars.next().is_some() {
        // There were characters beyond `max` — append the ellipsis marker.
        format!("{truncated}…")
    } else {
        truncated
    }
}

/// Format an Option<String> for display, substituting "—" for None.
pub fn opt(s: &Option<String>) -> String {
    s.clone().unwrap_or_else(|| "—".into())
}
