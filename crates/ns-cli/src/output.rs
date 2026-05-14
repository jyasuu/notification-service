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
pub fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_owned()
    } else {
        format!("{}…", &s[..max])
    }
}

/// Format an Option<String> for display, substituting "—" for None.
pub fn opt(s: &Option<String>) -> String {
    s.clone().unwrap_or_else(|| "—".into())
}
