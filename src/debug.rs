//! Debug formatter for recorded events.

use colored::Colorize;
use macaw::core::*;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::io::IsTerminal;
use std::net::SocketAddr;
use tokio::sync::mpsc;

use crate::config::ProxyMap;

const PROXY_COLORS: [colored::Color; 6] = [
    colored::Color::Red,
    colored::Color::Green,
    colored::Color::Yellow,
    colored::Color::Blue,
    colored::Color::Magenta,
    colored::Color::Cyan,
];

fn proxy_color(proxy_id: &str) -> colored::Color {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    proxy_id.hash(&mut hasher);
    let idx = hasher.finish() as usize % PROXY_COLORS.len();
    PROXY_COLORS[idx]
}

fn format_proxy_id(proxy_id: &str, width: usize) -> String {
    let len = proxy_id.chars().count();
    if len > width {
        format!(
            "[{}...]",
            proxy_id
                .chars()
                .take(width.saturating_sub(3))
                .collect::<String>()
        )
    } else {
        format!("[{:<1$}]", proxy_id, width)
    }
}

fn style_part(part: &RecordPart, use_color: bool) -> String {
    if !use_color {
        return part.to_string();
    }
    match part {
        RecordPart::StreamType(s) => s.cyan().to_string(),
        RecordPart::Id(s) => s.yellow().to_string(),
        RecordPart::Meta(s) => s.green().to_string(),
        RecordPart::Content(s) => s.dimmed().to_string(),
    }
}

fn truncate_parts(parts: &[RecordPart], max_width: usize) -> Vec<RecordPart> {
    let mut used_width = 0;
    let mut truncated_parts = Vec::new();
    for part in parts {
        let mut part = part.clone();
        if matches!(part, RecordPart::Id(..)) {
            let part_truncated = part.to_string().chars().take(8).collect::<String>();
            part = part.replace(&part_truncated);
        }
        if used_width + part.len() > max_width {
            let part_len = (max_width - used_width).saturating_sub(3);
            let part_truncated = part.to_string().chars().take(part_len).collect::<String>();
            truncated_parts.push(part.replace(&format!("{}...", part_truncated)));
            break;
        } else {
            used_width += part.len();
            truncated_parts.push(part);
        }
    }
    truncated_parts
}

fn style_parts(parts: &[RecordPart], use_color: bool) -> String {
    parts
        .iter()
        .map(|part| style_part(part, use_color))
        .collect::<Vec<String>>()
        .join(" ")
}

fn style_arraw(arrow: &str) -> String {
    match arrow {
        "→" => "→".blue().to_string(),
        "←" => "←".green().to_string(),
        _ => arrow.dimmed().to_string(),
    }
}

/// Print configuration summary for record mode.
pub fn print_record_summary(proxies: &ProxyMap, bindings: &HashMap<String, SocketAddr>) {
    let proxy_width = proxies.max_name_length().min(12);
    let use_color = std::io::stderr().is_terminal();

    if use_color {
        println!("{}", "Recording mode".blue());
        println!();
        println!("{}", "Proxies:".dimmed());
    } else {
        println!("Recording mode");
        println!();
        println!("Proxies:");
    }
    for (proxy_id, config) in proxies.iter() {
        let proxy_display = format_proxy_id(proxy_id.as_str(), proxy_width);
        let target_str = config.target().unwrap_or("");
        let bind_str = bindings
            .get(proxy_id)
            .map(|s| s.to_string())
            .unwrap_or(config.bind().to_string());
        if use_color {
            println!(
                "{:<14} {:<12}  →  {}",
                proxy_display.color(proxy_color(proxy_id)),
                bind_str.bold(),
                target_str.bold()
            );
        } else {
            println!("{:<14} {:<12}  →  {}", proxy_display, bind_str, target_str);
        }
    }
    println!();
}

/// Print record outcome on exit
pub fn print_record_outcome(outcome: RecorderOutcome) {
    if outcome.total_bytes.is_some() {
        println!(
            "\nRecording saved to: {}",
            outcome.recording_path.display().to_string().green()
        );
    }
    println!(
        "- Total events: {}",
        outcome.total_events.to_string().green()
    );
    if let Some(bytes) = outcome.total_bytes {
        println!(
            "- Recording size: {}",
            bytesize::ByteSize::b(bytes as u64)
                .display()
                .si()
                .to_string()
                .green()
        );
    }
    if let Some(time) = outcome.total_time {
        println!("- Total time: {}", format!("{:.2?}", time).green());
    }
}

/// Print configuration summary for replay mode.
pub fn print_replay_summary(proxies: &ProxyMap, bindings: &HashMap<String, SocketAddr>) {
    let proxy_width = proxies.max_name_length().min(12);
    let use_color = std::io::stderr().is_terminal();
    if use_color {
        println!("{}", "Replaying mode".blue());
        println!();
        println!("{}", "Proxies:".dimmed());
    } else {
        println!("Replaying mode");
        println!();
        println!("Proxies:");
    }
    for (proxy_id, config) in proxies.iter() {
        let proxy_display = format_proxy_id(proxy_id.as_str(), proxy_width);
        let target_str = format!("({})", config.target().unwrap_or(""));
        let bind_str = bindings
            .get(proxy_id)
            .map(|s| s.to_string())
            .unwrap_or(config.bind().to_string());

        if use_color {
            println!(
                "{:<14} {:<12} {}",
                proxy_display.color(proxy_color(proxy_id)),
                bind_str.bold(),
                target_str.bold()
            );
        } else {
            println!(
                "{:<14} {:<12} {}",
                proxy_display,
                bind_str,
                target_str.dimmed()
            );
        }
    }
    println!();
}

/// Format a recorded event for human-readable debug output.
pub fn format_recorded_event(
    proxy_id: &ProxyId,
    event: &dyn macaw::core::RecordEvent,
    proxy_width: usize,
) -> String {
    let formatter = event.format_debug();
    let direction = formatter.direction();
    let parts = formatter.parts();

    let arrow = direction.arrow();
    let proxy_display = format_proxy_id(proxy_id.as_str(), proxy_width);

    let use_color = std::io::stderr().is_terminal();
    let term_width = terminal_size::terminal_size()
        .map(|(w, _)| w.0 as usize)
        .unwrap_or(120);

    let prefix_len = arrow.len() + 1 + proxy_display.len() + 1;
    let msg_max_width = term_width.saturating_sub(prefix_len).saturating_sub(3);
    let truncated_parts = truncate_parts(parts, msg_max_width);

    if !use_color {
        return format!(
            "{} {} {}",
            arrow,
            proxy_display,
            style_parts(&truncated_parts, false)
        );
    }

    format!(
        "{} {} {}",
        style_arraw(arrow),
        proxy_display.color(proxy_color(proxy_id.as_str())),
        style_parts(&truncated_parts, true)
    )
}

/// Spawn a task that receives RecordedEvents and prints them.
pub fn spawn_debug_printer(mut rx: mpsc::UnboundedReceiver<RecordedEvent>, proxy_width: usize) {
    tokio::spawn(async move {
        while let Some(recorded) = rx.recv().await {
            let line =
                format_recorded_event(&recorded.proxy_id, recorded.event.as_ref(), proxy_width);
            eprintln!("{}", line);
        }
    });
}
