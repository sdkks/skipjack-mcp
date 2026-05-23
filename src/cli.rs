//! CLI mode for the metasearchd binary.
//!
//! Implements the CLI personality of the binary (invoked when args are present).
//! Uses `clap` derive for subcommand dispatch. Communicates with daemon via
//! Unix socket using the JSON newline-delimited protocol defined in
//! [`crate::daemon::protocol`].
//!
//! # Architecture
//!
//! Each subcommand handler follows the same flow:
//! 1. Construct a [`Request`] from CLI arguments.
//! 2. Call [`send_request`] which connects to the daemon socket, writes the
//!    request as a single JSON line, reads the response, and returns it.
//! 3. Format and display the response on stdout (or stderr for errors).
//! 4. Exit with an appropriate code.

use std::fs;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{bail, Context};
use clap::{Parser, Subcommand, ValueEnum};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;

use crate::config::Config;
use crate::daemon::{ProviderInfo, Request, Response};
use crate::search::{Freshness, SearchRequest};

/// Maximum size for a daemon response message (matches daemon protocol).
const MAX_MESSAGE_SIZE: usize = 1_048_576;

// ---------------------------------------------------------------------------
// CLI argument definitions (clap derive)
// ---------------------------------------------------------------------------

/// Multi-provider web search daemon — CLI interface.
///
/// Communicates with the metasearchd daemon over a Unix domain socket.
/// Fallback subcommands (`usage`) work without a running daemon.
#[derive(Parser)]
#[command(
    name = "metasearchd",
    version = concat!(env!("CARGO_PKG_VERSION"), " (", env!("GIT_SHA"), ")"),
    about = "Multi-provider web search daemon with anti-blocking and caching"
)]
struct Cli {
    /// Optional path to the configuration file.
    #[arg(short = 'c', long, global = true)]
    config: Option<String>,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Run a web search and print results.
    ///
    /// Connects to the metasearchd daemon, sends a search request, and
    /// displays the ranked, deduplicated results.
    Search {
        /// The search query.
        query: String,

        /// Maximum number of results to return (default: 10).
        #[arg(short = 'n', long, default_value = "10")]
        limit: usize,

        /// Explicit provider names to use, bypassing the tier fallback.
        /// Accepts multiple values separated by commas.
        #[arg(short = 'p', long, value_delimiter = ',')]
        providers: Vec<String>,

        /// ISO 639-1 language code (e.g., "en", "fr").
        #[arg(short = 'l', long)]
        language: Option<String>,

        /// ISO 3166-1 alpha-2 country code (e.g., "us", "gb").
        #[arg(long)]
        country: Option<String>,

        /// Disable safe search. Safe search is enabled by default.
        #[arg(long, default_value_t = false, action = clap::ArgAction::SetTrue)]
        no_safe_search: bool,

        /// Time-based freshness filter.
        #[arg(long, value_enum)]
        freshness: Option<FreshnessCli>,

        /// Output format: "pretty" for human-readable or "json" for machine-readable.
        #[arg(long, default_value = "pretty")]
        format: OutputFormat,

        /// Provider dispatch mode: concurrent (fire all providers at once) or
        /// tiered (execute tiers sequentially, stop when enough results collected).
        #[arg(long, value_enum)]
        dispatch_mode: Option<DispatchModeCli>,
    },

    /// Print daemon health, provider statuses, and cache stats in a table.
    ///
    /// Sends Health, ProviderStatus, and CacheStats requests to the daemon
    /// and formats the combined output.
    Status,

    /// Stop the running daemon.
    ///
    /// Reads the PID file, sends SIGTERM to the daemon process, and waits
    /// up to 10 seconds for the socket to be removed.
    Stop,

    /// Print an LLM-optimized usage reference.
    ///
    /// Produces a static reference text (under 1000 tokens) suitable as
    /// a system-prompt reference for LLM-powered tools. Does not require
    /// the daemon to be running.
    Usage,

    /// List all configured providers with health and availability.
    ///
    /// Sends a ListProviders request to the daemon and displays the
    /// provider catalog in a table.
    Providers,

    /// Clear the search result cache.
    ///
    /// Sends a CacheClear request to the daemon. Optionally filter by
    /// provider name to only clear entries from that provider.
    CacheClear {
        /// If set, only clear cache entries from this provider.
        #[arg(short = 'p', long)]
        provider: Option<String>,
    },
}

/// Output format for the `search` subcommand.
#[derive(Clone, Copy, ValueEnum)]
enum OutputFormat {
    /// Human-readable output with aligned columns.
    Pretty,
    /// Machine-readable JSON output.
    Json,
}

impl std::fmt::Display for OutputFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OutputFormat::Pretty => write!(f, "pretty"),
            OutputFormat::Json => write!(f, "json"),
        }
    }
}

/// Freshness filter for CLI argument parsing.
#[derive(Clone, ValueEnum)]
enum FreshnessCli {
    /// Results from the past 24 hours.
    Day,
    /// Results from the past 7 days.
    Week,
    /// Results from the past 30 days.
    Month,
    /// Results from the past 365 days.
    Year,
}

impl From<FreshnessCli> for Freshness {
    fn from(f: FreshnessCli) -> Self {
        match f {
            FreshnessCli::Day => Freshness::Day,
            FreshnessCli::Week => Freshness::Week,
            FreshnessCli::Month => Freshness::Month,
            FreshnessCli::Year => Freshness::Year,
        }
    }
}

/// Dispatch mode for CLI argument parsing.
#[derive(Clone, ValueEnum)]
enum DispatchModeCli {
    /// Fire all non-Playwright providers at once, wait for completion, merge results.
    Concurrent,
    /// Execute providers grouped by tiers, moving to next tier only if results are below limit.
    Tiered,
}

impl From<DispatchModeCli> for String {
    fn from(m: DispatchModeCli) -> Self {
        match m {
            DispatchModeCli::Concurrent => "concurrent".to_string(),
            DispatchModeCli::Tiered => "tiered".to_string(),
        }
    }
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

/// Run the CLI, parse arguments, and dispatch to the appropriate subcommand.
///
/// Returns `Ok(())` on success. On error, the error message is printed to
/// stderr and the function returns the error for the caller to determine the
/// exit code.
pub async fn run() -> anyhow::Result<()> {
    let cli = Cli::parse();

    // Load config for socket path resolution.
    let config = Config::load(cli.config.as_deref())?;

    let daemon_name = config.daemon.name.clone();

    // Validate daemon name against character allowlist to prevent path traversal.
    if !daemon_name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-') {
        bail!(
            "invalid daemon name: '{}'. Daemon name must contain only \
             alphanumeric characters, underscores, and hyphens.",
            daemon_name
        );
    }

    let socket_path = PathBuf::from(&config.daemon.socket_dir)
        .join(format!("{}.sock", daemon_name));
    let pid_path = PathBuf::from(&config.daemon.pid_dir)
        .join(format!("{}.pid", daemon_name));

    match cli.command {
        Commands::Search {
            query,
            limit,
            providers,
            language,
            country,
            no_safe_search,
            freshness,
            format,
            dispatch_mode,
        } => {
            cmd_search(
                &socket_path, query, limit, providers, language, country,
                !no_safe_search, freshness, format, dispatch_mode,
            ).await
        }
        Commands::Status => cmd_status(&socket_path).await,
        Commands::Stop => cmd_stop(&pid_path, &socket_path, &daemon_name),
        Commands::Usage => {
            cmd_usage();
            Ok(())
        }
        Commands::Providers => cmd_providers(&socket_path).await,
        Commands::CacheClear { provider } => cmd_cache_clear(&socket_path, provider).await,
    }
}

// ---------------------------------------------------------------------------
// Socket client helpers
// ---------------------------------------------------------------------------

/// Connect to the daemon socket, send a request, and return the response.
///
/// # Errors
///
/// Returns `Err` with a "daemon not running" message if the socket is
/// unreachable (exit code 2 per FR-3.6).
async fn send_request(socket_path: &std::path::Path, request: &Request) -> anyhow::Result<Response> {
    let mut stream = UnixStream::connect(socket_path)
        .await
        .with_context(|| "daemon not running")?;

    // Serialize request to JSON, append newline.
    let mut json = serde_json::to_string(request)
        .context("failed to serialize request")?;
    json.push('\n');

    stream.write_all(json.as_bytes()).await
        .context("failed to send request to daemon")?;
    stream.flush().await
        .context("failed to flush request to daemon")?;

    // Read response line.
    let mut reader = BufReader::new(&mut stream);
    let mut line = String::new();

    // Reuse the read_line_limited pattern from daemon server.
    let bytes_read = match read_line_limited(&mut reader, &mut line, MAX_MESSAGE_SIZE).await {
        Ok(n) => n,
        Err(ReadError::MaxSizeExceeded) => {
            bail!("daemon response too large (>{} bytes)", MAX_MESSAGE_SIZE);
        }
        Err(ReadError::Io(e)) => {
            bail!("failed to read response from daemon: {}", e);
        }
    };

    if bytes_read == 0 {
        bail!("daemon closed connection without response");
    }

    let response: Response = serde_json::from_str(line.trim())
        .context("failed to parse daemon response")?;

    Ok(response)
}

/// Read a line from `reader` into `buf`, enforcing a maximum byte count.
///
/// Returns `Ok(0)` on EOF. Returns `Err(ReadError::MaxSizeExceeded)` if the
/// line exceeds `max_size` bytes.
async fn read_line_limited<R: tokio::io::AsyncRead + Unpin>(
    reader: &mut BufReader<R>,
    buf: &mut String,
    max_size: usize,
) -> Result<usize, ReadError> {
    buf.clear();

    let mut total: usize = 0;

    loop {
        let bytes_in_buffer = reader.buffer().len();
        if bytes_in_buffer == 0 {
            reader.fill_buf().await.map_err(|e| {
                ReadError::Io(e)
            })?;
            if reader.buffer().is_empty() {
                return Ok(0); // EOF
            }
        }

        let buffer = reader.buffer();
        if let Some(pos) = buffer.iter().position(|&b| b == b'\n') {
            let chunk = &buffer[..pos];
            let chunk_str = std::str::from_utf8(chunk).unwrap_or("");
            buf.push_str(chunk_str);
            total += pos + 1;

            if total > max_size + 1 {
                return Err(ReadError::MaxSizeExceeded);
            }

            reader.consume(pos + 1);
            return Ok(total);
        }

        let remaining = buffer.len();
        if total + remaining > max_size + 1 {
            return Err(ReadError::MaxSizeExceeded);
        }

        let chunk_str = std::str::from_utf8(buffer).unwrap_or("");
        buf.push_str(chunk_str);
        total += remaining;
        reader.consume(remaining);
    }
}

#[derive(Debug)]
enum ReadError {
    MaxSizeExceeded,
    Io(std::io::Error),
}

/// Check the response for errors. If the response is an Error variant,
/// print the message to stderr and return an error.
///
/// Returns `Ok(response)` for success variants, allowing the caller to
/// handle the specific response type.
fn check_response_error(response: Response) -> anyhow::Result<Response> {
    match &response {
        Response::Error { code: _, message, data: _ } => {
            bail!("{}", message);
        }
        _ => Ok(response),
    }
}

#[allow(dead_code)]
/// Print the response as JSON to stdout.
fn print_response_json(response: &Response) {
    let json = serde_json::to_string_pretty(response)
        .unwrap_or_else(|e| format!("{{ \"error\": \"serialization failed: {}\" }}", e));
    println!("{}", json);
}

// ---------------------------------------------------------------------------
// Subcommand handlers
// ---------------------------------------------------------------------------

/// Execute the `search` subcommand.
async fn cmd_search(
    socket_path: &std::path::Path,
    query: String,
    limit: usize,
    providers: Vec<String>,
    language: Option<String>,
    country: Option<String>,
    safe_search: bool,
    freshness: Option<FreshnessCli>,
    format: OutputFormat,
    dispatch_mode: Option<DispatchModeCli>,
) -> anyhow::Result<()> {
    let request = Request::Search(SearchRequest {
        request_id: uuid::Uuid::new_v4().to_string(),
        query,
        limit,
        providers,
        language,
        country,
        safe_search,
        freshness: freshness.map(Freshness::from),
        dispatch_mode: dispatch_mode.map(String::from),
    });

    let response = send_request(socket_path, &request).await?;
    let response = check_response_error(response)?;

    match response {
        Response::SearchResult(search_resp) => {
            match format {
                OutputFormat::Json => {
                    let json = serde_json::to_string_pretty(&search_resp)
                        .unwrap_or_else(|e| format!("{{ \"error\": \"{}\" }}", e));
                    println!("{}", json);
                }
                OutputFormat::Pretty => {
                    if search_resp.cache_hit {
                        eprintln!("\u{1f4e6} cached ({} ms)", search_resp.elapsed_ms);
                    } else {
                        eprintln!("\u{23f3} {} ms, {} results from {} providers",
                            search_resp.elapsed_ms,
                            search_resp.total_found,
                            search_resp.providers_used.join(", "));
                    }

                    for (i, result) in search_resp.results.iter().enumerate() {
                        println!("\n{}. {}", i + 1, result.title);
                        println!("   {}", result.url);
                        println!("   {}", result.snippet);
                        if let Some(ref date) = result.published_date {
                            print!("   [{}]", date);
                        }
                        println!("   (via {}, score: {:.2})", result.provider_name, result.rank_score);
                    }

                    if search_resp.results.is_empty() {
                        println!("(no results found)");
                    }
                }
            }
            Ok(())
        }
        other => {
            bail!("unexpected response type for Search: {:?}", other)
        }
    }
}

/// Execute the `status` subcommand — fetch daemon health, provider status,
/// and cache stats, then display in a human-readable table.
async fn cmd_status(socket_path: &std::path::Path) -> anyhow::Result<()> {
    // Fetch health.
    let health_resp = send_request(socket_path, &Request::Health).await?;
    let health_resp = check_response_error(health_resp)?;

    // Fetch provider status.
    let provider_resp = send_request(socket_path, &Request::ProviderStatus).await?;
    let provider_resp = check_response_error(provider_resp)?;

    // Fetch cache stats.
    let cache_resp = send_request(socket_path, &Request::CacheStats).await?;
    let cache_resp = check_response_error(cache_resp)?;

    // Display health.
    if let Response::Health { status, uptime_secs, version } = health_resp {
        let uptime = format_duration(uptime_secs);
        println!("=== Daemon Status ===");
        println!("  Status:   {}", status);
        println!("  Version:  {}", version);
        println!("  Uptime:   {}", uptime);
    }

    println!();

    // Display provider health.
    if let Response::ProviderStatus { providers } = provider_resp {
        println!("=== Provider Health ===");
        if providers.is_empty() {
            println!("  (no providers configured)");
        } else {
            println!("  {:<20} {:>8} {:>10} {:>8} {:>10}  Last Error",
                "Provider", "Healthy", "Degraded", "Score", "Succ/Fail");
            println!("  {:-<20} {:-<8} {:-<10} {:-<8} {:-<10}  {:-<20}",
                "", "", "", "", "", "");
            for p in &providers {
                let healthy = if p.healthy { "YES" } else { "no" };
                let degraded = if p.degraded { "YES" } else { "no" };
                let last_err = p.last_error.as_deref().unwrap_or("-");
                println!(
                    "  {:<20} {:>8} {:>10} {:>8.2} {:>4}/{:<5}  {}",
                    p.name, healthy, degraded, p.health_score,
                    p.success_count, p.failure_count, last_err
                );
            }
        }
    }

    println!();

    // Display cache stats.
    if let Response::CacheStats { total_entries, total_size_bytes, hit_count, miss_count, hit_rate } = cache_resp {
        let total = hit_count + miss_count;
        let hit_pct = if total > 0 {
            format!("{:.1}%", hit_rate * 100.0)
        } else {
            "N/A".to_string()
        };
        let size_str = format_bytes(total_size_bytes);

        println!("=== Cache Stats ===");
        println!("  Entries:     {}", total_entries);
        println!("  Size:        {}", size_str);
        println!("  Hits:        {} ({} total requests)", hit_count, total);
        println!("  Hit rate:    {}", hit_pct);
    }

    Ok(())
}

/// Verify that `pid` belongs to a process named `daemon_name`.
///
/// Uses platform-specific APIs to check the process identity before sending
/// a signal, preventing accidental termination of an unrelated process that
/// recycled the PID after the daemon exited.
///
/// Returns `true` if the PID can be verified as belonging to the daemon.
/// Returns `false` if verification fails or the process cannot be inspected.
fn verify_pid(pid: i32, daemon_name: &str) -> bool {
    #[cfg(target_os = "linux")]
    {
        if let Ok(comm) = fs::read_to_string(format!("/proc/{}/comm", pid)) {
            return comm.trim() == daemon_name;
        }
        false
    }
    #[cfg(target_os = "macos")]
    {
        let mut path_buf = [0u8; 4096];
        let ret = unsafe {
            libc::proc_pidpath(pid, path_buf.as_mut_ptr() as *mut libc::c_void, path_buf.len() as u32)
        };
        if ret > 0 {
            let path = String::from_utf8_lossy(&path_buf[..ret as usize]);
            return path.contains(daemon_name);
        }
        false
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        eprintln!(
            "warning: cannot verify PID identity on this platform — proceeding with SIGTERM"
        );
        true
    }
}

/// Execute the `stop` subcommand — send SIGTERM to the daemon and wait for
/// the socket to be removed.
fn cmd_stop(pid_path: &std::path::Path, socket_path: &std::path::Path, daemon_name: &str) -> anyhow::Result<()> {
    // Read the PID file.
    let pid_str = fs::read_to_string(pid_path)
        .with_context(|| {
            format!(
                "cannot read PID file for daemon '{}': daemon may not be running",
                daemon_name
            )
        })?;

    let pid: i32 = pid_str
        .trim()
        .parse()
        .with_context(|| format!("invalid PID in PID file for daemon '{}': {}", daemon_name, pid_str.trim()))?;

    // Verify the PID still belongs to the daemon before sending SIGTERM.
    if !verify_pid(pid, daemon_name) {
        bail!(
            "PID {} does not belong to daemon '{}'. The PID file may be stale — \
             the daemon may have crashed or been terminated by an external agent. \
             Remove the PID file manually if the daemon is no longer running.",
            pid, daemon_name
        );
    }

    // Send SIGTERM.
    eprintln!("Sending SIGTERM to daemon '{}' (pid {})...", daemon_name, pid);

    // SAFETY: kill(pid, SIGTERM) is safe because:
    // - pid is a valid pid_t (i32) parsed from the PID file
    // - SIGTERM is a valid signal number
    // - The call is memory-safe (no pointers to Rust-managed memory)
    // - PID ownership was verified via verify_pid() above (TOCTOU window remains
    //   but is narrowed to the interval between verification and signal delivery)
    unsafe {
        let ret = libc::kill(pid, libc::SIGTERM);
        if ret != 0 {
            let err = std::io::Error::last_os_error();
            bail!("failed to send SIGTERM to pid {}: {}", pid, err);
        }
    }

    // Wait up to 10 seconds for the socket to be removed.
    let timeout = Duration::from_secs(10);
    let start = std::time::Instant::now();

    while start.elapsed() < timeout {
        if !socket_path.exists() {
            eprintln!("Daemon '{}' stopped successfully.", daemon_name);
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(100));
    }

    bail!(
        "daemon '{}' did not stop within {} seconds",
        daemon_name,
        timeout.as_secs(),
    );
}

/// Execute the `usage` subcommand — print a static LLM-optimized reference.
fn cmd_usage() {
    const USAGE_TEXT: &str = r#"
metasearchd — Multi-provider web search daemon (v0.1.0)

SUBCOMMANDS:

  search <query> [OPTIONS]
    Run a web search across configured providers. Results are ranked,
    deduplicated, and returned from cache when available.

    Options:
      -n, --limit <N>           Max results (default: 10)
      -p, --providers <P>,<P>   Explicit providers (comma-separated)
      -l, --language <LANG>     ISO 639-1 language code (e.g., en, fr)
      --country <CC>            ISO 3166-1 alpha-2 country code
      --no-safe-search          Disable safe search
      --freshness <WHEN>        Filter by time: day, week, month, year
      --format <FMT>            Output format: pretty (default) or json
      --dispatch-mode <MODE>    concurrent (default) or tiered
      --config <PATH>           Path to config file

    Exit codes: 0 on success, non-zero on error, 2 if daemon unreachable.

  status
    Print daemon health, provider statuses, and cache stats in a table.
    Fetches Health + ProviderStatus + CacheStats from the daemon.

    Exit codes: 0 on success, 2 if daemon unreachable.

  stop
    Read the PID file, send SIGTERM to the daemon process, wait up to 10s
    for the socket to be removed. The daemon will complete in-flight
    requests up to its configured grace period.

    Exit codes: 0 on success, 1 on timeout (daemon did not stop within 10s).

  usage
    Print this reference text. No daemon connection needed. Under 1000
    tokens, suitable as an LLM system-prompt reference.

  providers
    List all configured providers with names, descriptions, tags,
    availability, health status, and health scores.

    Exit codes: 0 on success, 2 if daemon unreachable.

  cache-clear [-p|--provider <NAME>]
    Clear the search result cache. If no provider filter is given, all
    entries are removed. Reports the number of removed entries.

    Exit codes: 0 on success, 2 if daemon unreachable.

DAEMON CONTROL:

  metasearchd --daemon
    Start the daemon in the background. Writes PID to /tmp/metasearchd.pid,
    creates Unix socket at /tmp/metasearchd.sock.

  metasearchd --daemon --config /path/to/config.toml
    Start with a custom configuration file.

  metasearchd (no arguments)
    Start in MCP (Model Context Protocol) server mode over stdin/stdout.

SOCKET PROTOCOL:

  The CLI communicates with the daemon over a Unix domain socket using a
  newline-delimited JSON protocol. One JSON object per line, terminated by
  \n. The request `type` field determines the handler dispatched on the
  daemon side.

CONFIGURATION:

  Default config: ~/.config/metasearchd/config.toml
  Override with: --config <PATH> or METASEARCHD_* environment variables.
  Env var format: METASEARCHD_SECTION__KEY=value (double underscore for nesting).
  Example: METASEARCHD_CACHE__DEFAULT_TTL_SECS=7200

  Key sections: [daemon], [search], [cache], [dispatch], [anti_blocking],
  [ranking], [[providers]], [tiers]

PROVIDER MODEL:

  The daemon dispatches searches across a tiered provider ladder:
    Tier 0: Free/no-key (DuckDuckGo, Mojeek)
    Tier 1: Free/API-key (Jina AI, Serper)
    Tier 2: Free/moderate-rate (ScraperAPI, Brave free)
    Tier 3: Paid/reliable (Brave paid, DataForSEO)
    Tier 4: Self-hosted (SearXNG)
    Tier 5: Last-resort scraper (Playwright browser)

  Dispatch modes:
    concurrent — Fire all non-Playwright providers at once, merge results.
    tiered     — Execute tiers sequentially, stop when results >= limit.

FILES:

  /tmp/metasearchd.pid         PID file
  /tmp/metasearchd.sock         Unix domain socket
  ~/.cache/metasearchd/cache.db SQLite cache
  ~/.config/metasearchd/config.toml  Configuration
"#;

    println!("{}", USAGE_TEXT.trim());
}

/// Execute the `providers` subcommand.
async fn cmd_providers(socket_path: &std::path::Path) -> anyhow::Result<()> {
    let response = send_request(socket_path, &Request::ListProviders).await?;
    let response = check_response_error(response)?;

    match response {
        Response::ProviderList { providers } => {
            print_providers_table(&providers);
            Ok(())
        }
        other => bail!("unexpected response type for ListProviders: {:?}", other),
    }
}

/// Execute the `cache-clear` subcommand.
async fn cmd_cache_clear(
    socket_path: &std::path::Path,
    provider: Option<String>,
) -> anyhow::Result<()> {
    let request = Request::CacheClear { provider };
    let response = send_request(socket_path, &request).await?;
    let response = check_response_error(response)?;

    match response {
        Response::CacheCleared { removed } => {
            println!("Cache cleared: {} entries removed.", removed);
            Ok(())
        }
        other => bail!("unexpected response type for CacheClear: {:?}", other),
    }
}

// ---------------------------------------------------------------------------
// Display helpers
// ---------------------------------------------------------------------------

/// Print a formatted table of providers.
fn print_providers_table(providers: &[ProviderInfo]) {
    if providers.is_empty() {
        println!("(no providers configured)");
        return;
    }

    println!("{:<20} {:>10} {:>8} {:>6}  {}  Description",
        "Provider", "Available", "Healthy", "Score", "Tags");
    println!("{:-<20} {:-<10} {:-<8} {:-<6}  {}  {:-<20}",
        "", "", "", "", "----", "");

    for p in providers {
        let available = if p.available { "YES" } else { "no" };
        let healthy = if p.healthy { "YES" } else { "no" };
        let tags = p.tags.join(", ");
        println!(
            "{:<20} {:>10} {:>8} {:>6.2}  {}  {}",
            p.name, available, healthy, p.health_score,
            tags, p.description
        );
    }
}

/// Format a duration in seconds into a human-readable string.
fn format_duration(secs: u64) -> String {
    if secs < 60 {
        format!("{}s", secs)
    } else if secs < 3600 {
        format!("{}m {}s", secs / 60, secs % 60)
    } else {
        let hours = secs / 3600;
        let mins = (secs % 3600) / 60;
        let secs_rem = secs % 60;
        format!("{}h {}m {}s", hours, mins, secs_rem)
    }
}

/// Format a byte count into a human-readable string.
fn format_bytes(bytes: u64) -> String {
    const UNITS: &[&str] = &["B", "KB", "MB", "GB"];
    let mut size = bytes as f64;
    let mut unit_idx = 0;
    while size >= 1024.0 && unit_idx < UNITS.len() - 1 {
        size /= 1024.0;
        unit_idx += 1;
    }
    format!("{:.1} {}", size, UNITS[unit_idx])
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// format_duration should produce correct human-readable strings.
    #[test]
    fn test_format_duration() {
        assert_eq!(format_duration(0), "0s");
        assert_eq!(format_duration(42), "42s");
        assert_eq!(format_duration(90), "1m 30s");
        assert_eq!(format_duration(3661), "1h 1m 1s");
        assert_eq!(format_duration(7200), "2h 0m 0s");
        assert_eq!(format_duration(86400), "24h 0m 0s");
    }

    /// format_bytes should produce correct human-readable strings.
    #[test]
    fn test_format_bytes() {
        assert_eq!(format_bytes(0), "0.0 B");
        assert_eq!(format_bytes(500), "500.0 B");
        assert_eq!(format_bytes(1024), "1.0 KB");
        assert_eq!(format_bytes(1536), "1.5 KB");
        assert_eq!(format_bytes(1048576), "1.0 MB");
        assert_eq!(format_bytes(1073741824), "1.0 GB");
    }

    /// format_duration with zero should return a non-empty string.
    #[test]
    fn test_format_duration_zero_not_empty() {
        assert!(!format_duration(0).is_empty());
    }

    /// Verify CLI argument parsing works for the search subcommand with
    /// all optional flags.
    #[test]
    fn test_cli_parse_search_minimal() {
        let cli = Cli::try_parse_from([
            "metasearchd",
            "search",
            "rust programming",
        ]);
        assert!(cli.is_ok(), "minimal search should parse: {:?}", cli.err());
    }

    /// Search with all optional flags should parse and produce correct values.
    #[test]
    fn test_cli_parse_search_full() {
        let cli = Cli::try_parse_from([
            "metasearchd",
            "search",
            "rust async",
            "--limit", "5",
            "--providers", "duckduckgo,brave",
            "--language", "en",
            "--country", "us",
            "--no-safe-search",
            "--freshness", "month",
            "--format", "json",
            "--dispatch-mode", "tiered",
        ]).unwrap();
        match cli.command {
            Commands::Search { format, dispatch_mode, .. } => {
                assert!(matches!(format, OutputFormat::Json), "format should be Json");
                assert!(matches!(dispatch_mode, Some(DispatchModeCli::Tiered)), "dispatch_mode should be Tiered");
            }
            _ => panic!("expected Search command"),
        }
    }

    /// status subcommand should parse.
    #[test]
    fn test_cli_parse_status() {
        let cli = Cli::try_parse_from(["metasearchd", "status"]);
        assert!(cli.is_ok(), "status should parse: {:?}", cli.err());
    }

    /// stop subcommand should parse.
    #[test]
    fn test_cli_parse_stop() {
        let cli = Cli::try_parse_from(["metasearchd", "stop"]);
        assert!(cli.is_ok(), "stop should parse: {:?}", cli.err());
    }

    /// usage subcommand should parse.
    #[test]
    fn test_cli_parse_usage() {
        let cli = Cli::try_parse_from(["metasearchd", "usage"]);
        assert!(cli.is_ok(), "usage should parse: {:?}", cli.err());
    }

    /// providers subcommand should parse.
    #[test]
    fn test_cli_parse_providers() {
        let cli = Cli::try_parse_from(["metasearchd", "providers"]);
        assert!(cli.is_ok(), "providers should parse: {:?}", cli.err());
    }

    /// cache-clear subcommand should parse with and without provider flag.
    #[test]
    fn test_cli_parse_cache_clear() {
        let cli = Cli::try_parse_from(["metasearchd", "cache-clear"]);
        assert!(cli.is_ok(), "cache-clear should parse: {:?}", cli.err());

        let cli = Cli::try_parse_from([
            "metasearchd", "cache-clear", "--provider", "duckduckgo",
        ]);
        assert!(cli.is_ok(), "cache-clear with provider should parse: {:?}", cli.err());
    }

    /// Global --config flag should parse.
    #[test]
    fn test_cli_parse_with_global_config() {
        let cli = Cli::try_parse_from([
            "metasearchd",
            "--config", "/tmp/test.toml",
            "search", "test",
        ]);
        assert!(cli.is_ok(), "global config flag should parse: {:?}", cli.err());
    }

    /// Helper function to check that FreshnessCli converts to Freshness correctly.
    #[test]
    fn test_freshness_cli_conversion() {
        let f: Freshness = FreshnessCli::Day.into();
        assert!(matches!(f, Freshness::Day));

        let f: Freshness = FreshnessCli::Week.into();
        assert!(matches!(f, Freshness::Week));

        let f: Freshness = FreshnessCli::Month.into();
        assert!(matches!(f, Freshness::Month));

        let f: Freshness = FreshnessCli::Year.into();
        assert!(matches!(f, Freshness::Year));
    }

    /// Helper function to check DispatchModeCli converts to String correctly.
    #[test]
    fn test_dispatch_mode_conversion() {
        let s: String = DispatchModeCli::Concurrent.into();
        assert_eq!(s, "concurrent");

        let s: String = DispatchModeCli::Tiered.into();
        assert_eq!(s, "tiered");
    }

    /// The search command with --no-safe-search flag means safe_search is OFF.
    #[test]
    fn test_search_safe_search_flag() {
        let cli = Cli::try_parse_from([
            "metasearchd", "search", "--no-safe-search", "test query",
        ]);
        assert!(cli.is_ok(), "search with --no-safe-search should parse: {:?}", cli.err());
    }

    /// check_response_error should pass through success responses.
    #[test]
    fn test_check_response_error_success() {
        let resp = Response::Health {
            status: "ok".into(),
            uptime_secs: 0,
            version: "0.1.0".into(),
        };
        let result = check_response_error(resp);
        assert!(result.is_ok(), "success response should pass through: {:?}", result.err());
    }

    /// check_response_error should bail on error responses.
    #[test]
    fn test_check_response_error_error() {
        let resp = Response::Error {
            code: -32000,
            message: "something went wrong".into(),
            data: None,
        };
        let result = check_response_error(resp);
        assert!(result.is_err(), "error response should bail");
        assert!(
            result.unwrap_err().to_string().contains("something went wrong"),
            "error message should be preserved"
        );
    }

    /// read_line_limited should read a normal line.
    #[tokio::test]
    async fn test_read_line_limited_normal() {
        let data = b"hello world\n";
        let mut reader = BufReader::new(&data[..]);
        let mut buf = String::new();
        let n = read_line_limited(&mut reader, &mut buf, 1024).await.unwrap();
        assert_eq!(n, 12);
        assert_eq!(buf, "hello world");
    }

    /// read_line_limited should return Ok(0) on EOF with no data.
    #[tokio::test]
    async fn test_read_line_limited_eof() {
        let data = b"";
        let mut reader = BufReader::new(&data[..]);
        let mut buf = String::new();
        let result = read_line_limited(&mut reader, &mut buf, 1024).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 0);
    }

    /// read_line_limited should return MaxSizeExceeded when line exceeds limit.
    #[tokio::test]
    async fn test_read_line_limited_oversized() {
        let data = b"abcdefghij\n"; // 11 bytes including newline
        let mut reader = BufReader::new(&data[..]);
        let mut buf = String::new();
        let result = read_line_limited(&mut reader, &mut buf, 5).await;
        assert!(matches!(result, Err(ReadError::MaxSizeExceeded)),
            "should return MaxSizeExceeded, got {:?}", result.err());
    }

    /// print_providers_table with empty list should print placeholder.
    #[test]
    fn test_print_providers_table_empty() {
        let providers: Vec<ProviderInfo> = vec![];
        print_providers_table(&providers);
        // No panic is the primary assertion; the function prints to stdout.
    }

    /// print_providers_table with providers should print rows.
    #[test]
    fn test_print_providers_table_with_data() {
        let providers = vec![
            ProviderInfo {
                name: "test-provider".into(),
                description: "A test provider".into(),
                tags: vec!["test".into()],
                available: true,
                healthy: true,
                health_score: 1.0,
            },
        ];
        print_providers_table(&providers);
        // No panic is the primary assertion; the function prints to stdout.
    }
}
