mod model;
mod transport;

use anyhow::{Context, Result, bail};
use clap::{Args, Subcommand, ValueEnum};
use colored::Colorize;
use futures::StreamExt;
use macaw::core::{DebugDirection, RecordPart, RecordedEvent};
use macaw::session::SessionState;
use model::{HealthResponse, ProfileResponse, SessionResponse, TrafficStreamEvent};
use serde::Serialize;
use serde_json::{Map, Value, json};
use std::hash::{Hash, Hasher};
use std::io::{IsTerminal, Read, Write};
use std::path::PathBuf;
use std::time::Duration;
use transport::ControlClient;
use unicode_width::{UnicodeWidthChar, UnicodeWidthStr};

const DEFAULT_CONTROL_URL: &str = "http://127.0.0.1:8080";

#[derive(Debug, Args)]
pub struct ClientArgs {
    /// HTTP control server URL
    #[arg(long, value_name = "URL", conflicts_with = "unix")]
    url: Option<String>,

    /// Connect through a Unix domain socket
    #[arg(long, value_name = "PATH", conflicts_with = "url")]
    unix: Option<PathBuf>,

    /// Request timeout in seconds
    #[arg(long, default_value_t = 30)]
    timeout: u64,

    /// Output format
    #[arg(short, long, value_enum, default_value_t = OutputFormat::Human)]
    output: OutputFormat,

    /// Disable terminal colors
    #[arg(long)]
    no_color: bool,

    #[command(subcommand)]
    command: ClientCommand,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum OutputFormat {
    Human,
    Jsonl,
}

#[derive(Debug, Subcommand)]
enum ClientCommand {
    /// Check control server readiness
    Health,
    /// Manage reusable proxy profiles
    Profile {
        #[command(subcommand)]
        command: ProfileCommand,
    },
    /// Manage detached sessions
    Session {
        #[command(subcommand)]
        command: SessionCommand,
    },
    /// Stream traffic from a running session
    Watch(WatchArgs),
    /// Create and supervise a foreground session
    Run {
        #[command(subcommand)]
        command: RunCommand,
    },
}

#[derive(Debug, Subcommand)]
enum ProfileCommand {
    /// Create a profile from a TOML configuration
    Create {
        profile: String,
        #[arg(long, value_name = "PATH")]
        file: String,
        #[arg(long, value_name = "PATH", default_value = ".")]
        root: PathBuf,
    },
    /// List profiles
    List,
    /// Get a profile
    Get { profile: String },
    /// Delete a profile
    Delete { profile: String },
}

#[derive(Debug, Subcommand)]
enum SessionCommand {
    /// Start a detached recording session
    Record {
        profile: String,
        #[arg(long)]
        name: Option<String>,
        #[arg(long, value_name = "PATH")]
        output: PathBuf,
    },
    /// Start a detached replay session
    Replay {
        profile: String,
        #[arg(long)]
        name: Option<String>,
        #[arg(long, value_name = "PATH")]
        recording: PathBuf,
    },
    /// List sessions
    List {
        #[arg(long)]
        profile: Option<String>,
        #[arg(long, value_enum)]
        state: Option<StateFilter>,
    },
    /// Get a session
    Get {
        #[arg(value_name = "SESSION-ID|SESSION-NAME")]
        session: String,
    },
    /// Stop a session and wait for completion
    Stop {
        #[arg(value_name = "SESSION-ID|SESSION-NAME")]
        session: String,
    },
    /// Delete a ready or terminal session
    Delete {
        #[arg(value_name = "SESSION-ID|SESSION-NAME")]
        session: String,
    },
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum StateFilter {
    Starting,
    Ready,
    Running,
    Stopping,
    Stopped,
    Failed,
}

impl From<StateFilter> for SessionState {
    fn from(value: StateFilter) -> Self {
        match value {
            StateFilter::Starting => Self::Starting,
            StateFilter::Ready => Self::Ready,
            StateFilter::Running => Self::Running,
            StateFilter::Stopping => Self::Stopping,
            StateFilter::Stopped => Self::Stopped,
            StateFilter::Failed => Self::Failed,
        }
    }
}

#[derive(Debug, Args)]
struct WatchArgs {
    #[arg(value_name = "SESSION-ID|SESSION-NAME")]
    session: String,
    #[command(flatten)]
    options: WatchOptions,
}

#[derive(Debug, Clone, Args)]
struct WatchOptions {
    /// Include only these proxies
    #[arg(long, value_name = "NAME")]
    proxy: Vec<String>,
    /// Include one traffic direction
    #[arg(long, value_enum, default_value_t = DirectionFilter::Both)]
    direction: DirectionFilter,
    /// Include only these protocols
    #[arg(long, value_name = "PROTOCOL")]
    protocol: Vec<String>,
    /// Body display policy
    #[arg(long, value_enum, default_value_t = BodyDisplay::Preview)]
    body: BodyDisplay,
    /// Display headers exposed by the recorded event
    #[arg(long)]
    headers: bool,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum DirectionFilter {
    Request,
    Response,
    Both,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum BodyDisplay {
    None,
    Preview,
}

#[derive(Debug, Subcommand)]
enum RunCommand {
    /// Record until interrupted
    Record {
        profile: String,
        #[arg(long)]
        name: Option<String>,
        #[arg(long, value_name = "PATH")]
        output: PathBuf,
        #[command(flatten)]
        options: RunOptions,
    },
    /// Replay under foreground supervision
    Replay {
        profile: String,
        #[arg(long)]
        name: Option<String>,
        #[arg(long, value_name = "PATH")]
        recording: PathBuf,
        #[command(flatten)]
        options: RunOptions,
    },
}

#[derive(Debug, Args)]
struct RunOptions {
    /// Do not display live traffic
    #[arg(long)]
    no_watch: bool,
    /// Leave the session running when interrupted
    #[arg(long)]
    keep_running: bool,
    #[command(flatten)]
    watch: WatchOptions,
}

pub async fn run(args: ClientArgs) -> Result<()> {
    let explicit_url = args.url.is_some();
    let unix = args.unix.or_else(|| {
        (!explicit_url)
            .then(|| std::env::var_os("MACAW_CONTROL_UNIX"))
            .flatten()
            .filter(|value| !value.is_empty())
            .map(PathBuf::from)
    });
    let url = args
        .url
        .or_else(|| std::env::var("MACAW_CONTROL_URL").ok())
        .unwrap_or_else(|| DEFAULT_CONTROL_URL.to_owned());
    let client = ControlClient::new(&url, unix.as_deref(), Duration::from_secs(args.timeout))?;
    let use_color =
        !args.no_color && std::env::var_os("NO_COLOR").is_none() && std::io::stdout().is_terminal();

    match args.command {
        ClientCommand::Health => {
            let health = client.get::<HealthResponse>("/v1/health").await?;
            match args.output {
                OutputFormat::Human => println!(
                    "Macaw {} (API {}) — {}",
                    health.package_version,
                    health.api_version,
                    if health.ready { "ready" } else { "not ready" }
                ),
                format => print_serialized(&health, format)?,
            }
        }
        ClientCommand::Profile { command } => run_profile(&client, command, args.output).await?,
        ClientCommand::Session { command } => run_session(&client, command, args.output).await?,
        ClientCommand::Watch(watch) => {
            let session = client
                .get::<SessionResponse>(&format!("/v1/sessions/{}", watch.session))
                .await?;
            let proxy_width = if session.proxies.is_empty() {
                let profile = client
                    .get::<ProfileResponse>(&format!("/v1/profiles/{}", session.profile_id))
                    .await?;
                profile
                    .proxies
                    .keys()
                    .map(|name| UnicodeWidthStr::width(name.as_str()))
                    .max()
                    .unwrap_or(0)
                    .min(12)
            } else {
                proxy_width(&session)
            };
            let final_session = tokio::select! {
                result = watch_until_closed(
                    &client,
                    &watch.session,
                    &watch.options,
                    args.output,
                    use_color,
                    proxy_width,
                    None,
                ) => Some(result?),
                result = tokio::signal::ctrl_c() => {
                    result?;
                    None
                },
            };
            if let Some(session) = final_session {
                print_session(&session, args.output)?;
            }
        }
        ClientCommand::Run { command } => {
            run_foreground(&client, command, args.output, use_color).await?
        }
    }
    Ok(())
}

async fn run_profile(
    client: &ControlClient,
    command: ProfileCommand,
    output: OutputFormat,
) -> Result<()> {
    match command {
        ProfileCommand::Create {
            profile,
            file,
            root,
        } => {
            let body = profile_request(&profile, &file, root)?;
            let profile = client
                .post::<_, ProfileResponse>("/v1/profiles", Some(&body))
                .await?;
            print_profile(&profile, output)?;
        }
        ProfileCommand::List => {
            let profiles = client.get::<Vec<ProfileResponse>>("/v1/profiles").await?;
            match output {
                OutputFormat::Human => {
                    for profile in profiles {
                        println!("{}", profile.id);
                    }
                }
                OutputFormat::Jsonl => {
                    for profile in profiles {
                        print_serialized(&profile, output)?;
                    }
                }
            }
        }
        ProfileCommand::Get { profile } => {
            let profile = client
                .get::<ProfileResponse>(&format!("/v1/profiles/{profile}"))
                .await?;
            print_profile(&profile, output)?;
        }
        ProfileCommand::Delete { profile } => {
            client.delete(&format!("/v1/profiles/{profile}")).await?;
            if matches!(output, OutputFormat::Human) {
                println!("Profile {profile} deleted");
            }
        }
    }
    Ok(())
}

async fn run_session(
    client: &ControlClient,
    command: SessionCommand,
    output: OutputFormat,
) -> Result<()> {
    match command {
        SessionCommand::Record {
            profile,
            name,
            output: path,
        } => {
            let body = json!({"name": name, "mode": {"type": "record", "output": path}});
            let session = create_session(client, &profile, &body).await?;
            let session = start_session(client, &session.id.to_string()).await?;
            print_session(&session, output)?;
        }
        SessionCommand::Replay {
            profile,
            name,
            recording,
        } => {
            let body = json!({"name": name, "mode": {"type": "replay", "recording": recording}});
            let session = create_session(client, &profile, &body).await?;
            let session = start_session(client, &session.id.to_string()).await?;
            print_session(&session, output)?;
        }
        SessionCommand::List { profile, state } => {
            let path = profile
                .map(|profile| format!("/v1/profiles/{profile}/sessions"))
                .unwrap_or_else(|| "/v1/sessions".to_owned());
            let mut sessions = client.get::<Vec<SessionResponse>>(&path).await?;
            if let Some(state) = state {
                let state = SessionState::from(state);
                sessions.retain(|session| session.state == state);
            }
            match output {
                OutputFormat::Human => {
                    for session in sessions {
                        println!(
                            "{}  {:<20}  {:<8}  {}",
                            session.id,
                            session
                                .name
                                .as_ref()
                                .map(ToString::to_string)
                                .unwrap_or_else(|| "-".to_owned()),
                            state_name(session.state),
                            session.profile_id
                        );
                    }
                }
                OutputFormat::Jsonl => {
                    for session in sessions {
                        print_serialized(&session, output)?;
                    }
                }
            }
        }
        SessionCommand::Get { session } => {
            let session = client
                .get::<SessionResponse>(&format!("/v1/sessions/{session}"))
                .await?;
            print_session(&session, output)?;
        }
        SessionCommand::Stop { session } => {
            let session = client
                .post::<(), SessionResponse>(&format!("/v1/sessions/{session}/stop"), None)
                .await?;
            print_session(&session, output)?;
        }
        SessionCommand::Delete { session } => {
            client.delete(&format!("/v1/sessions/{session}")).await?;
            if matches!(output, OutputFormat::Human) {
                println!("Session {session} deleted");
            }
        }
    }
    Ok(())
}

async fn run_foreground(
    client: &ControlClient,
    command: RunCommand,
    output: OutputFormat,
    use_color: bool,
) -> Result<()> {
    let (profile, body, options) = match command {
        RunCommand::Record {
            profile,
            name,
            output,
            options,
        } => (
            profile,
            json!({"name": name, "mode": {"type": "record", "output": output}}),
            options,
        ),
        RunCommand::Replay {
            profile,
            name,
            recording,
            options,
        } => (
            profile,
            json!({"name": name, "mode": {"type": "replay", "recording": recording}}),
            options,
        ),
    };
    let session = create_session(client, &profile, &body).await?;
    let id = session.id.to_string();
    let response = if options.no_watch {
        None
    } else {
        match client.stream(&format!("/v1/sessions/{id}/events")).await {
            Ok(response) => Some(response),
            Err(error) => {
                let _ = client
                    .post::<(), SessionResponse>(&format!("/v1/sessions/{id}/stop"), None)
                    .await;
                return Err(error);
            }
        }
    };
    let session = start_session(client, &id).await?;
    print_session(&session, output)?;
    let proxy_width = proxy_width(&session);

    let observed_final = if options.no_watch {
        tokio::signal::ctrl_c().await?;
        None
    } else {
        tokio::select! {
            result = watch_until_closed(
                client,
                &id,
                &options.watch,
                output,
                use_color,
                proxy_width,
                response,
            ) => {
                Some(result?)
            },
            result = tokio::signal::ctrl_c() => {
                result?;
                None
            },
        }
    };

    if options.keep_running {
        if let Some(session) = observed_final {
            print_session(&session, output)?;
        } else {
            eprintln!("Session {id} left running");
        }
        return Ok(());
    }
    let final_session = match observed_final {
        Some(session) => session,
        None => {
            client
                .post::<(), SessionResponse>(&format!("/v1/sessions/{id}/stop"), None)
                .await?
        }
    };
    print_session(&final_session, output)
}

async fn start_session(client: &ControlClient, id: &str) -> Result<SessionResponse> {
    let session = client
        .post::<(), SessionResponse>(&format!("/v1/sessions/{id}/start"), None)
        .await?;
    if session.state == SessionState::Failed {
        if let Some(error) = &session.error {
            bail!("{:?}: {}", error.code, error.message);
        }
        bail!("session failed to start");
    }
    Ok(session)
}

async fn create_session(
    client: &ControlClient,
    profile: &str,
    body: &Value,
) -> Result<SessionResponse> {
    client
        .post(&format!("/v1/profiles/{profile}/sessions"), Some(body))
        .await
}

fn profile_request(profile: &str, file: &str, root: PathBuf) -> Result<Value> {
    let content = if file == "-" {
        let mut content = String::new();
        std::io::stdin().read_to_string(&mut content)?;
        content
    } else {
        std::fs::read_to_string(file)
            .with_context(|| format!("failed to read profile configuration {file}"))?
    };
    let value: toml::Value = toml::from_str(&content)
        .with_context(|| format!("failed to parse profile configuration {file}"))?;
    let proxies = value
        .get("proxies")
        .and_then(toml::Value::as_table)
        .context("profile configuration must contain a [proxies] table")?;
    let mut wire_proxies = Map::new();
    for (name, proxy) in proxies {
        let mut config = serde_json::to_value(proxy)?
            .as_object()
            .cloned()
            .context("each proxy must be a TOML table")?;
        let implementation = config
            .remove("type")
            .and_then(|value| value.as_str().map(str::to_owned))
            .with_context(|| format!("proxy {name} must define a string type"))?;
        wire_proxies.insert(
            name.clone(),
            json!({"type": implementation, "config": config}),
        );
    }
    Ok(json!({
        "id": profile,
        "config_root": root,
        "proxies": wire_proxies,
    }))
}

async fn watch_until_closed(
    client: &ControlClient,
    session: &str,
    options: &WatchOptions,
    output: OutputFormat,
    use_color: bool,
    proxy_width: usize,
    initial_response: Option<reqwest::Response>,
) -> Result<SessionResponse> {
    let mut after = None;
    let mut response = initial_response;
    loop {
        let response = match response.take() {
            Some(response) => response,
            None => {
                let cursor = after
                    .map(|sequence| format!("?after={sequence}"))
                    .unwrap_or_default();
                match client
                    .stream(&format!("/v1/sessions/{session}/events{cursor}"))
                    .await
                {
                    Ok(response) => response,
                    Err(error) => {
                        let current = client
                            .get::<SessionResponse>(&format!("/v1/sessions/{session}"))
                            .await?;
                        if current.state.is_terminal() {
                            return Ok(current);
                        }
                        tracing::debug!("traffic stream reconnect failed: {error}");
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        continue;
                    }
                }
            }
        };
        consume_traffic_stream(
            response,
            options,
            output,
            use_color,
            proxy_width,
            &mut after,
        )
        .await?;

        let current = client
            .get::<SessionResponse>(&format!("/v1/sessions/{session}"))
            .await?;
        if current.state.is_terminal() {
            return Ok(current);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn consume_traffic_stream(
    response: reqwest::Response,
    options: &WatchOptions,
    output: OutputFormat,
    use_color: bool,
    proxy_width: usize,
    after: &mut Option<u64>,
) -> Result<()> {
    let mut chunks = response.bytes_stream();
    let mut buffer = Vec::new();
    while let Some(chunk) = chunks.next().await {
        let chunk = match chunk {
            Ok(chunk) => chunk,
            Err(error) => {
                tracing::debug!("traffic stream disconnected: {error}");
                return Ok(());
            }
        };
        buffer.extend_from_slice(&chunk);
        while let Some((boundary, delimiter_len)) = sse_frame_boundary(&buffer) {
            let frame = buffer.drain(..boundary).collect::<Vec<_>>();
            buffer.drain(..delimiter_len);
            let frame =
                std::str::from_utf8(&frame).context("traffic stream was not valid UTF-8")?;
            let data = frame
                .lines()
                .map(|line| line.strip_suffix('\r').unwrap_or(line))
                .filter_map(|line| line.strip_prefix("data:"))
                .map(str::trim_start)
                .collect::<Vec<_>>()
                .join("\n");
            if data.is_empty() {
                continue;
            }
            let event: TrafficStreamEvent =
                serde_json::from_str(&data).context("invalid traffic stream event")?;
            print_traffic_event(event, options, output, use_color, proxy_width, after)?;
        }
    }
    Ok(())
}

fn sse_frame_boundary(buffer: &[u8]) -> Option<(usize, usize)> {
    buffer
        .windows(2)
        .position(|window| window == b"\n\n")
        .map(|position| (position, 2))
        .or_else(|| {
            buffer
                .windows(4)
                .position(|window| window == b"\r\n\r\n")
                .map(|position| (position, 4))
        })
}

fn print_traffic_event(
    event: TrafficStreamEvent,
    options: &WatchOptions,
    output: OutputFormat,
    use_color: bool,
    proxy_width: usize,
    after: &mut Option<u64>,
) -> Result<()> {
    match event {
        TrafficStreamEvent::DroppedEvents { count } => {
            if matches!(output, OutputFormat::Jsonl) {
                println!(
                    "{}",
                    serde_json::to_string(&json!({
                        "type": "dropped_events",
                        "count": count,
                    }))?
                );
            } else {
                eprintln!("warning: {count} traffic events were dropped");
            }
        }
        TrafficStreamEvent::Traffic { sequence, event } => {
            *after = Some(sequence);
            if !traffic_matches(&event, options) {
                return Ok(());
            }
            if matches!(output, OutputFormat::Jsonl) {
                println!(
                    "{}",
                    serde_json::to_string(&json!({
                        "type": "traffic",
                        "sequence": sequence,
                        "event": event,
                    }))?
                );
            } else {
                print_pretty_traffic(
                    &event,
                    options.body,
                    options.headers,
                    use_color,
                    proxy_width,
                );
            }
        }
    }
    std::io::stdout().flush()?;
    Ok(())
}

fn traffic_matches(event: &RecordedEvent, options: &WatchOptions) -> bool {
    let proxy_matches = options.proxy.is_empty()
        || options
            .proxy
            .iter()
            .any(|proxy| proxy == event.proxy_id.as_str());
    let formatter = event.event.format_debug();
    let direction_matches = match options.direction {
        DirectionFilter::Both => true,
        DirectionFilter::Request => formatter.direction() == DebugDirection::DownstreamToUpstream,
        DirectionFilter::Response => formatter.direction() == DebugDirection::UpstreamToDownstream,
    };
    let protocol_matches = options.protocol.is_empty()
        || formatter.parts().iter().any(|part| {
            matches!(
                part,
                RecordPart::StreamType(protocol)
                    if options
                        .protocol
                        .iter()
                        .any(|filter| filter.eq_ignore_ascii_case(protocol))
            )
        });
    proxy_matches && direction_matches && protocol_matches
}

const PROXY_COLORS: [colored::Color; 6] = [
    colored::Color::Red,
    colored::Color::Green,
    colored::Color::Yellow,
    colored::Color::Blue,
    colored::Color::Magenta,
    colored::Color::Cyan,
];

fn proxy_width(session: &SessionResponse) -> usize {
    session
        .proxies
        .keys()
        .map(|name| UnicodeWidthStr::width(name.as_str()))
        .max()
        .unwrap_or(0)
        .min(12)
}

fn proxy_color(proxy_id: &str) -> colored::Color {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    proxy_id.hash(&mut hasher);
    PROXY_COLORS[hasher.finish() as usize % PROXY_COLORS.len()]
}

fn truncate_text(text: &str, width: usize) -> String {
    if UnicodeWidthStr::width(text) <= width {
        return text.to_owned();
    }
    if width <= 3 {
        return ".".repeat(width);
    }
    let mut used = 0;
    let content_width = width - 3;
    let mut output = String::new();
    for character in text.chars() {
        let character_width = UnicodeWidthChar::width(character).unwrap_or(0);
        if used + character_width > content_width {
            break;
        }
        used += character_width;
        output.push(character);
    }
    output.push_str("...");
    output
}

fn format_proxy_id(proxy_id: &str, width: usize) -> String {
    let proxy_id = truncate_text(proxy_id, width);
    let padding = width.saturating_sub(UnicodeWidthStr::width(proxy_id.as_str()));
    format!("[{proxy_id}{}]", " ".repeat(padding))
}

fn shorten_ids(parts: &[RecordPart]) -> Vec<RecordPart> {
    parts
        .iter()
        .map(|part| match part {
            RecordPart::Id(id) => part.replace(&id.chars().take(8).collect::<String>()),
            _ => part.clone(),
        })
        .collect()
}

fn truncate_parts(parts: &[RecordPart], max_width: usize) -> Vec<RecordPart> {
    let parts = shorten_ids(parts);
    let mut used = 0;
    let mut output = Vec::new();
    for part in parts {
        let separator = usize::from(!output.is_empty());
        let available = max_width.saturating_sub(used + separator);
        if available == 0 {
            break;
        }
        let text = part.to_string();
        let width = UnicodeWidthStr::width(text.as_str());
        if width > available {
            output.push(part.replace(&truncate_text(&text, available)));
            break;
        }
        used += separator + width;
        output.push(part);
    }
    output
}

fn style_parts(parts: &[RecordPart]) -> String {
    parts
        .iter()
        .map(|part| match part {
            RecordPart::StreamType(value) => value.cyan().to_string(),
            RecordPart::Id(value) => value.yellow().to_string(),
            RecordPart::Meta(value) => value.green().to_string(),
            RecordPart::Content(value) => value.dimmed().to_string(),
        })
        .collect::<Vec<_>>()
        .join(" ")
}

fn style_arrow(arrow: &str) -> String {
    match arrow {
        "→" => arrow.blue().to_string(),
        "←" => arrow.green().to_string(),
        _ => arrow.dimmed().to_string(),
    }
}

fn print_pretty_traffic(
    event: &RecordedEvent,
    body: BodyDisplay,
    headers: bool,
    use_color: bool,
    proxy_width: usize,
) {
    let formatter = event.event.format_debug();
    let parts = formatter
        .parts()
        .iter()
        .filter(|part| {
            !matches!(body, BodyDisplay::None) || !matches!(part, RecordPart::Content(_))
        })
        .cloned()
        .collect::<Vec<_>>();
    let arrow = formatter.direction().arrow();
    let proxy = format_proxy_id(event.proxy_id.as_str(), proxy_width);
    let terminal_width = std::io::stdout()
        .is_terminal()
        .then(terminal_size::terminal_size)
        .flatten()
        .map(|(width, _)| width.0 as usize);
    let prefix_width =
        UnicodeWidthStr::width(arrow) + 1 + UnicodeWidthStr::width(proxy.as_str()) + 1;
    let parts = terminal_width
        .map(|width| truncate_parts(&parts, width.saturating_sub(prefix_width)))
        .unwrap_or_else(|| shorten_ids(&parts));
    if use_color {
        println!(
            "{} {} {}",
            style_arrow(arrow),
            proxy.color(proxy_color(event.proxy_id.as_str())),
            style_parts(&parts)
        );
    } else {
        println!(
            "{} {} {}",
            arrow,
            proxy,
            parts
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(" ")
        );
    }
    if headers {
        for (name, value) in event.event.debug_headers() {
            println!("    {name}: {value}");
        }
    }
}

fn print_profile(profile: &ProfileResponse, output: OutputFormat) -> Result<()> {
    match output {
        OutputFormat::Human => {
            println!("Profile {}", profile.id);
            println!("Root: {}", profile.config_root.display());
            for (name, proxy) in &profile.proxies {
                println!(
                    "{name:<12} {} {}{}{}",
                    proxy.protocol,
                    proxy.bind,
                    proxy
                        .target
                        .as_deref()
                        .map(|target| format!(" → {target}"))
                        .unwrap_or_default(),
                    proxy
                        .overrides
                        .as_deref()
                        .map(|path| format!(" (overrides: {path})"))
                        .unwrap_or_default(),
                );
            }
        }
        format => print_serialized(profile, format)?,
    }
    Ok(())
}

fn print_session(session: &SessionResponse, output: OutputFormat) -> Result<()> {
    match output {
        OutputFormat::Human => {
            println!(
                "Session {}{} {}",
                session.id,
                session
                    .name
                    .as_ref()
                    .map(|name| format!(" ({name})"))
                    .unwrap_or_default(),
                state_name(session.state)
            );
            println!("Profile: {}", session.profile_id);
            for (name, endpoint) in &session.proxies {
                println!("{name:<12} {}", endpoint.url);
            }
            if let Some(outcome) = &session.outcome {
                println!("Outcome: {}", serde_json::to_string(outcome)?);
            }
            if let Some(error) = &session.error {
                println!("Error: {:?}: {}", error.code, error.message);
            }
        }
        format => print_serialized(session, format)?,
    }
    Ok(())
}

fn print_serialized(value: &impl Serialize, output: OutputFormat) -> Result<()> {
    match output {
        OutputFormat::Human => {
            println!("{}", serde_json::to_string_pretty(value)?)
        }
        OutputFormat::Jsonl => println!("{}", serde_json::to_string(value)?),
    }
    Ok(())
}

fn state_name(state: SessionState) -> &'static str {
    match state {
        SessionState::Starting => "starting",
        SessionState::Ready => "ready",
        SessionState::Running => "running",
        SessionState::Stopping => "stopping",
        SessionState::Stopped => "stopped",
        SessionState::Failed => "failed",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn profile_toml_is_converted_to_the_wire_envelope() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("profile.toml");
        std::fs::write(
            &path,
            r#"
                [proxies.api]
                type = "http"
                bind = "127.0.0.1:0"
                target = "https://example.com"
            "#,
        )
        .unwrap();

        let request = profile_request(
            "development",
            path.to_str().unwrap(),
            PathBuf::from("/srv/app"),
        )
        .unwrap();
        assert_eq!(request["id"], "development");
        assert_eq!(request["config_root"], "/srv/app");
        assert_eq!(request["proxies"]["api"]["type"], "http");
        assert_eq!(
            request["proxies"]["api"]["config"]["target"],
            "https://example.com"
        );
        assert!(request["proxies"]["api"]["config"].get("type").is_none());
    }

    #[cfg(feature = "http")]
    #[test]
    fn traffic_filters_are_combined() {
        let event = RecordedEvent::new(
            "api".parse().unwrap(),
            macaw::http::HttpRequestEvent {
                request_id: uuid::Uuid::new_v4(),
                method: hyper::Method::GET,
                uri: "/test".parse().unwrap(),
                version: hyper::Version::HTTP_11,
                headers: Default::default(),
                body: macaw::core::Content::Empty,
            },
        );
        let options = WatchOptions {
            proxy: vec!["api".to_owned()],
            direction: DirectionFilter::Request,
            protocol: vec!["http".to_owned()],
            body: BodyDisplay::None,
            headers: false,
        };
        assert!(traffic_matches(&event, &options));

        let mut mismatch = options.clone();
        mismatch.direction = DirectionFilter::Response;
        assert!(!traffic_matches(&event, &mismatch));
    }

    #[test]
    fn terminal_truncation_uses_display_width() {
        assert_eq!(truncate_text("ab界cd", 6), "ab界cd");
        assert_eq!(truncate_text("ab界cd", 5), "ab...");
        assert_eq!(
            UnicodeWidthStr::width(truncate_text("ab界cd", 5).as_str()),
            5
        );
    }

    #[test]
    fn sse_frames_support_lf_and_crlf_boundaries() {
        assert_eq!(sse_frame_boundary(b"data: one\n\ndata:"), Some((9, 2)));
        assert_eq!(sse_frame_boundary(b"data: two\r\n\r\ndata:"), Some((9, 4)));
    }
}
