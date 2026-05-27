//! Connection handler for the daemon's Unix domain socket.
//!
//! Each incoming connection is handled by [`handle_connection`], which
//! reads newline-delimited JSON frames, deserializes them as [`Request`],
//! dispatches to the appropriate handler, and writes the [`Response`] back
//! as a single JSON line.
//!
//! # Framing
//!
//! The wire format is newline-delimited JSON (one message per line).
//! Frame boundaries are determined by the `\n` byte; there are no
//! content-length headers. Messages over [`MAX_MESSAGE_SIZE`] (1 MB) are
//! rejected before deserialization.
//!
//! # Connection lifecycle
//!
//! A single connection can handle multiple requests in sequence. The handler
//! loops until the client disconnects, a read error occurs, or a message
//! exceeds the size limit. After a Shutdown request the daemon signals
//! shutdown externally; the connection itself just returns the ShutdownAck.

use std::net::ToSocketAddrs;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;

use super::manager::{HealthStatus, Manager};
use super::protocol::{
    ProviderHealth, ProviderInfo, Request, Response, MAX_FETCH_TIMEOUT_SECS, MAX_MESSAGE_SIZE,
};
use crate::anti_blocking::{build_shuffled_tls_config, UserAgentPool};
use crate::config::Config;
use ipnet::IpNet;

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Handle a single client connection on the Unix domain socket.
///
/// Reads newline-delimited JSON requests in a loop until the client
/// disconnects or an unrecoverable framing error occurs. Each request
/// produces exactly one response.
///
/// # Errors
///
/// - Returns an error if the initial read fails (socket closed, etc.).
/// - Framing errors (message over 1 MB) result in an Error response and
///   connection close.
/// - Malformed JSON results in an Error response but the connection stays
///   open for subsequent requests.
pub async fn handle_connection(
    stream: UnixStream,
    config: Arc<Config>,
    manager: Arc<Manager>,
) -> anyhow::Result<()> {
    let (reader, mut writer) = tokio::io::split(stream);
    let mut buf_reader = BufReader::new(reader);
    let mut line = String::new();

    loop {
        line.clear();

        // Read until newline, enforcing the 1 MB size limit.
        let bytes_read = read_line_limited(&mut buf_reader, &mut line, MAX_MESSAGE_SIZE).await;

        match bytes_read {
            Ok(0) => {
                // EOF — client disconnected cleanly.
                tracing::debug!("connection closed by client");
                return Ok(());
            }
            Ok(_) => {
                // Got a line; parse and dispatch.
                match serde_json::from_str::<Request>(line.trim()) {
                    Ok(request) => {
                        let response = dispatch_request(request, &config, &manager).await;
                        write_response(&mut writer, &response).await?;
                    }
                    Err(e) => {
                        // Malformed JSON — respond with parse error and continue.
                        tracing::warn!(error = %e, "failed to parse request JSON");
                        let response = Response::Error {
                            code: -32700,
                            message: format!("Parse error: {}", e),
                            data: None,
                        };
                        // Writing the error response may fail if the client has
                        // disconnected; log and continue.
                        if let Err(write_err) = write_response(&mut writer, &response).await {
                            tracing::debug!(error = %write_err, "failed to write parse error response");
                            return Ok(());
                        }
                    }
                }
            }
            Err(ReadError::Io(e)) => {
                // I/O error (client disconnect, etc.) — drop silently.
                tracing::debug!(error = %e, "I/O error reading from socket");
                return Ok(());
            }
            Err(ReadError::MaxSizeExceeded) => {
                // Message exceeded size limit — respond with error and close.
                tracing::warn!("request body exceeded 1 MB limit, closing connection");
                let response = Response::Error {
                    code: -32600,
                    message: "Request too large: maximum message size is 1 MB".into(),
                    data: None,
                };
                let _ = write_response(&mut writer, &response).await;
                return Ok(());
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/// Read a line from `reader` into `buf`, enforcing a maximum byte count.
///
/// Returns `Ok(0)` on EOF. Returns `Err(ReadError::MaxSizeExceeded)` if the line
/// (including delimiter) exceeds `max_size` bytes, or `Err(ReadError::Io(...))`
/// for I/O errors. The `buf` is cleared before reading.
async fn read_line_limited<R: tokio::io::AsyncRead + Unpin>(
    reader: &mut BufReader<R>,
    buf: &mut String,
    max_size: usize,
) -> Result<usize, ReadError> {
    buf.clear();

    // Track total bytes read including the eventual newline.
    let mut total: usize = 0;

    loop {
        let bytes_in_buffer = reader.buffer().len();
        if bytes_in_buffer == 0 {
            // Refill the internal buffer.
            reader.fill_buf().await.map_err(|e| {
                tracing::debug!(error = %e, "read error on socket");
                ReadError::Io(e)
            })?;
            if reader.buffer().is_empty() {
                return Ok(0); // EOF
            }
        }

        // Search for newline in current buffer contents.
        let buffer = reader.buffer();
        if let Some(pos) = buffer.iter().position(|&b| b == b'\n') {
            let chunk = &buffer[..pos];
            let chunk_str = std::str::from_utf8(chunk).unwrap_or("");
            buf.push_str(chunk_str);
            total += pos + 1; // include the newline

            if total > max_size + 1 {
                return Err(ReadError::MaxSizeExceeded);
            }

            // Consume past the newline.
            reader.consume(pos + 1);
            return Ok(total);
        }

        // No newline yet — check if we've exceeded the limit.
        let remaining = buffer.len();
        if total + remaining > max_size + 1 {
            return Err(ReadError::MaxSizeExceeded);
        }

        // Append current buffer contents to buf.
        let chunk_str = std::str::from_utf8(buffer).unwrap_or("");
        buf.push_str(chunk_str);
        total += remaining;
        reader.consume(remaining);
    }
}

/// Error type for `read_line_limited`, distinguishing I/O errors from
/// size-limit breaches.
#[derive(Debug)]
enum ReadError {
    MaxSizeExceeded,
    Io(std::io::Error),
}

/// Maximum size of a fetched HTML body, in bytes (5 MB).
const MAX_FETCH_BODY_SIZE: usize = 5 * 1024 * 1024;

/// Maximum number of HTTP redirects to follow.
const MAX_REDIRECTS: usize = 5;

/// SPA detection warning block appended to markdown when heuristics trigger.
const SPA_WARNING: &str = "\n\n<!-- SKIPJACKD_SPA_DETECTED: This page appears to require JavaScript rendering. The\n     content above may be incomplete. Consider using a JS-capable fetch tool (e.g.,\n     Playwright MCP) to retrieve the full rendered content. -->\n";

// ---------------------------------------------------------------------------
// SSRF protection
// ---------------------------------------------------------------------------

/// Check if a hostname string is in the DNS blocklist.
fn is_hostname_blocked(host: &str) -> bool {
    let host_lower = host.to_lowercase();
    host_lower == "localhost"
        || host_lower.ends_with(".local")
        || host_lower == "metadata.google.internal"
}

/// Check if an IP address falls within a private/loopback/link-local range.
fn is_ip_blocked(ip: &std::net::IpAddr) -> bool {
    let blocked: &[&str] = &[
        "127.0.0.0/8",    // loopback
        "10.0.0.0/8",     // RFC 1918 private
        "172.16.0.0/12",  // RFC 1918 private
        "192.168.0.0/16", // RFC 1918 private
        "169.254.0.0/16", // link-local / cloud metadata
        "::1/128",        // IPv6 loopback
        "fc00::/7",       // IPv6 unique local
        "fe80::/10",      // IPv6 link-local
        "0.0.0.0/8",      // current network
    ];

    for range in blocked {
        if let Ok(net) = range.parse::<IpNet>() {
            if net.contains(ip) {
                return true;
            }
        }
    }
    false
}

/// Validate a URL for SSRF protection.
///
/// Checks:
/// 1. URL is well-formed
/// 2. Scheme is http or https only
/// 3. Hostname is not in DNS blocklist (localhost, *.local, metadata.google.internal)
/// 4. Resolved IP addresses are not in private/loopback/link-local ranges
///
/// Returns the parsed URL on success. Error messages are sanitized to avoid
/// leaking internal network information.
fn validate_url(url_str: &str) -> Result<url::Url, String> {
    let parsed = url::Url::parse(url_str).map_err(|_| "Invalid URL".to_string())?;

    // Check scheme.
    let scheme = parsed.scheme();
    if scheme != "http" && scheme != "https" {
        return Err(format!("Unsupported URL scheme: {}", scheme));
    }

    // Validate host using the url::Host enum which distinguishes IPv4, IPv6, and domain.
    match parsed.host() {
        Some(url::Host::Ipv4(ip)) => {
            if is_ip_blocked(&std::net::IpAddr::V4(ip)) {
                return Err("URL resolves to a restricted network address".to_string());
            }
        }
        Some(url::Host::Ipv6(ip)) => {
            if is_ip_blocked(&std::net::IpAddr::V6(ip)) {
                return Err("URL resolves to a restricted network address".to_string());
            }
        }
        Some(url::Host::Domain(domain)) => {
            if is_hostname_blocked(domain) {
                return Err("URL resolves to a restricted network address".to_string());
            }
            // Resolve hostname to IP addresses and check all of them.
            let port = parsed
                .port()
                .unwrap_or(if scheme == "https" { 443 } else { 80 });
            let sock_addrs = (domain, port)
                .to_socket_addrs()
                .map_err(|_| "DNS resolution failed".to_string())?;
            for addr in sock_addrs {
                if is_ip_blocked(&addr.ip()) {
                    return Err("URL resolves to a restricted network address".to_string());
                }
            }
        }
        None => return Err("URL has no host".to_string()),
    }

    Ok(parsed)
}

/// Fetch a URL and return its content as markdown.
///
/// Builds a standalone reqwest client with shuffled TLS ciphers and a random
/// User-Agent, validates the URL for SSRF protection, GETs the URL with redirect
/// following (each redirect validated), caps the body at
/// [`MAX_FETCH_BODY_SIZE`], converts HTML to markdown, and runs SPA detection
/// heuristics.
async fn handle_fetch(url: &str, timeout_secs: u64) -> Response {
    // Validate timeout.
    if timeout_secs == 0 || timeout_secs > MAX_FETCH_TIMEOUT_SECS {
        return Response::FetchResult {
            url: url.to_string(),
            markdown: String::new(),
            status: "error".into(),
            spa_detected: false,
            http_status: 0,
            error: Some(format!(
                "Timeout must be between 1 and {} seconds",
                MAX_FETCH_TIMEOUT_SECS
            )),
        };
    }

    // Validate URL for SSRF protection.
    let _parsed_url = match validate_url(url) {
        Ok(u) => u,
        Err(e) => {
            tracing::warn!(url = %url, error = %e, "URL validation failed");
            return Response::FetchResult {
                url: url.to_string(),
                markdown: String::new(),
                status: "error".into(),
                spa_detected: false,
                http_status: 0,
                error: Some(e),
            };
        }
    };

    // Build HTTP client with anti-blocking TLS configuration.
    let tls_config = match build_shuffled_tls_config() {
        Ok(cfg) => cfg,
        Err(e) => {
            tracing::error!(error = %e, "failed to build TLS config for fetch");
            return Response::FetchResult {
                url: url.to_string(),
                markdown: String::new(),
                status: "error".into(),
                spa_detected: false,
                http_status: 0,
                error: Some(format!("TLS config error: {}", e)),
            };
        }
    };

    let ua = UserAgentPool::random_ua();

    let client = match reqwest::Client::builder()
        .use_preconfigured_tls(tls_config)
        .user_agent(ua)
        .redirect(reqwest::redirect::Policy::custom(|attempt| {
            // Enforce redirect limit.
            if attempt.previous().len() >= MAX_REDIRECTS {
                return attempt.stop();
            }
            // Validate each redirect target for SSRF protection.
            match validate_url(attempt.url().as_str()) {
                Ok(_) => attempt.follow(),
                Err(_) => attempt.stop(),
            }
        }))
        .timeout(Duration::from_secs(timeout_secs))
        .build()
    {
        Ok(c) => c,
        Err(e) => {
            tracing::error!(error = %e, "failed to build HTTP client for fetch");
            return Response::FetchResult {
                url: url.to_string(),
                markdown: String::new(),
                status: "error".into(),
                spa_detected: false,
                http_status: 0,
                error: Some(format!("Client build error: {}", e)),
            };
        }
    };

    // Execute the GET request.
    let response = match client.get(url).send().await {
        Ok(resp) => resp,
        Err(e) => {
            tracing::warn!(url = %url, error = %e, "fetch request failed");
            let error_msg = if e.is_timeout() {
                format!("Request timed out after {}s", timeout_secs)
            } else if e.is_redirect() {
                "Too many redirects".to_string()
            } else {
                format!("Request failed: {}", e)
            };
            return Response::FetchResult {
                url: url.to_string(),
                markdown: String::new(),
                status: "error".into(),
                spa_detected: false,
                http_status: 0,
                error: Some(error_msg),
            };
        }
    };

    let final_url = response.url().to_string();
    let http_status = response.status().as_u16();

    // Read body, capped at MAX_FETCH_BODY_SIZE.
    let body_bytes = match read_body_capped(response, MAX_FETCH_BODY_SIZE).await {
        Ok(bytes) => bytes,
        Err(e) => {
            tracing::warn!(url = %url, error = %e, "failed to read fetch response body");
            return Response::FetchResult {
                url: final_url,
                markdown: String::new(),
                status: "error".into(),
                spa_detected: false,
                http_status,
                error: Some(format!("Body read error: {}", e)),
            };
        }
    };

    let html = match std::str::from_utf8(&body_bytes) {
        Ok(s) => s.to_string(),
        Err(e) => {
            return Response::FetchResult {
                url: final_url,
                markdown: String::new(),
                status: "error".into(),
                spa_detected: false,
                http_status,
                error: Some(format!("Invalid UTF-8: {}", e)),
            };
        }
    };

    // Detect SPA before HTML-to-markdown conversion.
    let (is_spa, spa_md) = detect_spa(&html);

    // Convert HTML to markdown.
    let markdown = match html_to_markdown_rs::convert(&html, None) {
        Ok(result) => result.content.unwrap_or_default(),
        Err(e) => {
            tracing::warn!(error = %e, "HTML to markdown conversion failed");
            String::new()
        }
    };

    // Append SPA warning if detected.
    let final_markdown = if is_spa { markdown + &spa_md } else { markdown };

    let status = if is_spa { "spa_detected" } else { "ok" };

    tracing::info!(
        url = %final_url,
        http_status = http_status,
        body_len = body_bytes.len(),
        spa_detected = is_spa,
        "fetch completed"
    );

    Response::FetchResult {
        url: final_url,
        markdown: final_markdown,
        status: status.to_string(),
        spa_detected: is_spa,
        http_status,
        error: None,
    }
}

/// Read the response body incrementally, enforcing a byte cap.
///
/// Checks Content-Length header first (if present) and rejects oversized
/// responses before reading. Then reads the body in chunks via
/// `response.chunk()`, aborting as soon as the accumulated size exceeds
/// `max_bytes`. This prevents buffering the entire body in memory before
/// the cap check for chunked/streaming responses.
async fn read_body_capped(
    mut response: reqwest::Response,
    max_bytes: usize,
) -> Result<bytes::Bytes, String> {
    // Check Content-Length header proactively.
    if let Some(cl) = response.content_length() {
        if cl as usize > max_bytes {
            return Err(format!(
                "Response body ({cl} bytes) exceeds {} MB limit",
                max_bytes / (1024 * 1024)
            ));
        }
    }

    let mut body = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|e| format!("Failed to read response body: {e}"))?
    {
        body.extend_from_slice(&chunk);
        if body.len() > max_bytes {
            return Err(format!(
                "Response body exceeds {} MB limit",
                max_bytes / (1024 * 1024)
            ));
        }
    }

    Ok(bytes::Bytes::from(body))
}

/// Run SPA detection heuristics on the raw HTML.
///
/// Returns `(is_spa, warning_markdown)` where `warning_markdown` is the
/// machine-readable warning block to append if SPA is detected.
fn detect_spa(html: &str) -> (bool, String) {
    // Strip script, style, and noscript content for body-text-length check.
    let stripped = strip_tags(html, &["script", "style", "noscript"]);
    let body_text_len = stripped.trim().len();

    let mut flags: Vec<&str> = Vec::new();

    // Heuristic 1: Body text < 512 bytes after stripping.
    if body_text_len < 512 {
        flags.push("body_text_too_short");
    }

    // Heuristic 2: No semantic content elements.
    let stripped_lower = stripped.to_lowercase();
    if !has_semantic_elements(&stripped_lower) {
        flags.push("no_semantic_elements");
    }

    // Heuristic 3: Known SPA root elements.
    if contains_spa_root(&html.to_lowercase()) {
        flags.push("spa_root_element");
    }

    if flags.is_empty() {
        return (false, String::new());
    }

    tracing::debug!(
        flags = ?flags,
        body_text_len = body_text_len,
        "SPA detection triggered"
    );

    (true, SPA_WARNING.to_string())
}

/// Strip all content between opening and closing tags for the given tag names.
///
/// This is a simple string-based removal; it does not handle nested tags of the
/// same name correctly, but that is acceptable for SPA heuristic purposes.
fn strip_tags(html: &str, tags: &[&str]) -> String {
    let mut result = html.to_string();
    for tag in tags {
        let open = format!("<{}", tag);
        let close = format!("</{}>", tag);
        loop {
            let lower = result.to_lowercase();
            let open_pos = match lower.find(&open) {
                Some(p) => p,
                None => break,
            };
            let close_pos = match lower[open_pos..].find(&close) {
                Some(p) => open_pos + p + close.len(),
                None => break,
            };
            result.replace_range(open_pos..close_pos, "");
        }
    }
    result
}

/// Check if the stripped HTML contains any semantic content elements.
fn has_semantic_elements(html_lower: &str) -> bool {
    html_lower.contains("<p ")
        || html_lower.contains("<p>")
        || html_lower.contains("<h1")
        || html_lower.contains("<h2")
        || html_lower.contains("<h3")
        || html_lower.contains("<h4")
        || html_lower.contains("<h5")
        || html_lower.contains("<h6")
        || html_lower.contains("<article")
        || html_lower.contains("<main")
        || html_lower.contains("<section")
}

/// Check if the HTML contains known SPA root container elements.
fn contains_spa_root(html_lower: &str) -> bool {
    html_lower.contains(r#"<div id="root""#)
        || html_lower.contains("<div id=\"root\"")
        || html_lower.contains("<div id='root'")
        || html_lower.contains(r#"<div id="app""#)
        || html_lower.contains("<div id=\"app\"")
        || html_lower.contains("<div id='app'")
        || html_lower.contains(r#"<div id="__next""#)
        || html_lower.contains("<div id=\"__next\"")
        || html_lower.contains("<div id='__next'")
        || html_lower.contains(r#"<div id="__nuxt""#)
        || html_lower.contains("<div id=\"__nuxt\"")
        || html_lower.contains("<div id='__nuxt'")
}

/// Dispatch a [`Request`] to the appropriate handler and produce a [`Response`].
///
/// This function is the central routing point for all daemon requests.
/// As daemon subsystems (cache, provider manager, etc.) are implemented,
/// the placeholder stubs here are replaced with real implementations.
async fn dispatch_request(request: Request, _config: &Config, manager: &Manager) -> Response {
    match request {
        Request::Search(search_req) => match manager.search(&search_req).await {
            Ok(response) => Response::SearchResult(response),
            Err(e) => Response::Error {
                code: -32603,
                message: format!("Search failed: {}", e),
                data: None,
            },
        },
        Request::ListProviders => {
            let health_snapshot = manager.health_snapshot().await;
            let providers = manager
                .catalog()
                .all_providers()
                .iter()
                .map(|p| {
                    let name = p.name().to_string();
                    let health = health_snapshot.get(&name);
                    ProviderInfo {
                        name: name.clone(),
                        description: p.description().to_string(),
                        tags: p.tags().iter().map(|t| format!("{:?}", t)).collect(),
                        available: p.is_available(),
                        healthy: health.map(|h| h.is_healthy()).unwrap_or(true),
                        health_score: health.map(|h| h.health_score()).unwrap_or(1.0),
                    }
                })
                .collect();
            Response::ProviderList { providers }
        }
        Request::CacheStats => {
            let stats = manager.cache_stats();
            Response::CacheStats {
                total_entries: stats.total_entries,
                total_size_bytes: stats.total_size_bytes,
                hit_count: stats.hit_count,
                miss_count: stats.miss_count,
                hit_rate: stats.hit_rate,
            }
        }
        Request::ProviderStatus => {
            let health_snapshot = manager.health_snapshot().await;
            let providers = health_snapshot
                .iter()
                .map(|(name, state)| ProviderHealth {
                    name: name.clone(),
                    healthy: state.is_healthy(),
                    degraded: state.status() == HealthStatus::Degraded,
                    health_score: state.health_score(),
                    success_count: 0,
                    failure_count: state.consecutive_failures() as u64,
                    last_error: None,
                })
                .collect();
            Response::ProviderStatus { providers }
        }
        Request::CacheClear { provider } => match manager.clear_cache(provider.as_deref()) {
            Ok(removed) => Response::CacheCleared { removed },
            Err(e) => Response::Error {
                code: -32603,
                message: format!("Failed to clear cache: {}", e),
                data: None,
            },
        },
        Request::Fetch { url, timeout_secs } => handle_fetch(&url, timeout_secs).await,
        Request::Health => Response::Health {
            status: "ok".into(),
            uptime_secs: 0,
            version: env!("CARGO_PKG_VERSION").into(),
        },
        Request::Shutdown => {
            tracing::info!("shutdown requested via socket");
            Response::ShutdownAck
        }
    }
}

/// Serialize a [`Response`] as a single JSON line and write it to the socket.
async fn write_response<W: AsyncWrite + Unpin>(
    writer: &mut W,
    response: &Response,
) -> std::io::Result<()> {
    let mut json = serde_json::to_string(response).map_err(|e| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("serialization error: {}", e),
        )
    })?;
    json.push('\n');
    writer.write_all(json.as_bytes()).await?;
    writer.flush().await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::search::SearchRequest;
    use std::sync::Mutex;
    use tokio::io::AsyncWriteExt;
    use tokio::net::UnixStream;

    /// Serialize access to the temp directory so SQLite doesn't clash.
    static DB_MUTEX: Mutex<()> = Mutex::new(());

    /// Create a test config pointing at a temp database.
    fn test_config(dir: &tempfile::TempDir) -> Config {
        let db_path = dir.path().join("test.db").to_string_lossy().to_string();
        let mut config = Config::default();
        config.cache.db_path = db_path;
        config
    }

    /// Helper: spawn a daemon-side handler on a socket pair and return the
    /// client-side stream.
    async fn spawn_handler(
        config: Arc<Config>,
        manager: Arc<Manager>,
    ) -> (UnixStream, tokio::task::JoinHandle<anyhow::Result<()>>) {
        let (server, client) = UnixStream::pair().expect("create socket pair");
        let config_clone = Arc::clone(&config);
        let handle =
            tokio::spawn(async move { handle_connection(server, config_clone, manager).await });
        (client, handle)
    }

    /// The Health request should return a valid Health response.
    #[tokio::test]
    async fn health_request_returns_ok() {
        let dir = tempfile::tempdir().expect("create temp dir");
        let _guard = DB_MUTEX.lock().unwrap_or_else(|e| e.into_inner());
        let config = Arc::new(test_config(&dir));
        let manager = Arc::new(Manager::new(&config).await.expect("create manager"));
        let (mut client, _handle) = spawn_handler(config, manager).await;

        let req_json = r#"{"type":"Health"}"#;
        client.write_all(req_json.as_bytes()).await.unwrap();
        client.write_all(b"\n").await.unwrap();

        let mut reader = BufReader::new(&mut client);
        let mut response_line = String::new();
        read_line_limited(&mut reader, &mut response_line, MAX_MESSAGE_SIZE)
            .await
            .expect("read response line");

        let response: Response =
            serde_json::from_str(response_line.trim()).expect("parse response");
        match response {
            Response::Health { status, .. } => assert_eq!(status, "ok"),
            other => panic!("expected Health response, got: {:?}", other),
        }
    }

    /// An unknown or malformed request should return an Error response.
    #[tokio::test]
    async fn malformed_request_returns_error() {
        let dir = tempfile::tempdir().expect("create temp dir");
        let _guard = DB_MUTEX.lock().unwrap_or_else(|e| e.into_inner());
        let config = Arc::new(test_config(&dir));
        let manager = Arc::new(Manager::new(&config).await.expect("create manager"));
        let (mut client, _handle) = spawn_handler(config, manager).await;

        // Send invalid JSON
        client.write_all(b"{bad json\n").await.unwrap();

        let mut reader = BufReader::new(&mut client);
        let mut response_line = String::new();
        read_line_limited(&mut reader, &mut response_line, MAX_MESSAGE_SIZE)
            .await
            .expect("read response line");

        let response: Response =
            serde_json::from_str(response_line.trim()).expect("parse response");
        match response {
            Response::Error { code, .. } => {
                assert_eq!(code, -32700, "expected parse error code");
            }
            other => panic!("expected Error response, got: {:?}", other),
        }
    }

    /// A Search request against an empty catalog should return a valid SearchResult
    /// with zero results (no providers configured).
    #[tokio::test]
    async fn search_request_empty_catalog_returns_empty_results() {
        let dir = tempfile::tempdir().expect("create temp dir");
        let _guard = DB_MUTEX.lock().unwrap_or_else(|e| e.into_inner());
        let config = Arc::new(test_config(&dir));
        let manager = Arc::new(Manager::new(&config).await.expect("create manager"));
        let (mut client, _handle) = spawn_handler(config, manager).await;

        let request = Request::Search(SearchRequest {
            request_id: "test".into(),
            query: "rust".into(),
            limit: 5,
            providers: vec![],
            language: None,
            country: None,
            safe_search: true,
            freshness: None,
            dispatch_mode: None,
        });

        let mut req_json = serde_json::to_string(&request).unwrap();
        req_json.push('\n');
        client.write_all(req_json.as_bytes()).await.unwrap();

        let mut reader = BufReader::new(&mut client);
        let mut response_line = String::new();
        read_line_limited(&mut reader, &mut response_line, MAX_MESSAGE_SIZE)
            .await
            .expect("read response line");

        let response: Response =
            serde_json::from_str(response_line.trim()).expect("parse response");
        match response {
            Response::SearchResult(sr) => {
                assert_eq!(sr.results.len(), 0);
                assert_eq!(sr.total_found, 0);
            }
            other => panic!("expected SearchResult response, got: {:?}", other),
        }
    }

    /// Sending zero bytes (immediate EOF) should be handled cleanly.
    #[tokio::test]
    async fn client_disconnect_without_data() {
        let dir = tempfile::tempdir().expect("create temp dir");
        let _guard = DB_MUTEX.lock().unwrap_or_else(|e| e.into_inner());
        let config = Arc::new(test_config(&dir));
        let manager = Arc::new(Manager::new(&config).await.expect("create manager"));
        let (client, handle) = spawn_handler(config, manager).await;

        // Drop the client immediately, signaling EOF.
        drop(client);

        let result = handle
            .await
            .expect("handler task panicked")
            .expect("handle_connection failed");
        // handle_connection should return Ok on clean EOF
        assert!(matches!(result, ()));
    }

    /// Multiple requests on the same connection should all receive responses.
    #[tokio::test]
    async fn multiple_requests_on_same_connection() {
        let dir = tempfile::tempdir().expect("create temp dir");
        let _guard = DB_MUTEX.lock().unwrap_or_else(|e| e.into_inner());
        let config = Arc::new(test_config(&dir));
        let manager = Arc::new(Manager::new(&config).await.expect("create manager"));
        let (mut client, _handle) = spawn_handler(config, manager).await;

        for _ in 0..3 {
            client.write_all(b"{\"type\":\"Health\"}\n").await.unwrap();

            let mut reader = BufReader::new(&mut client);
            let mut response_line = String::new();
            read_line_limited(&mut reader, &mut response_line, MAX_MESSAGE_SIZE)
                .await
                .expect("read response line");

            let response: Response =
                serde_json::from_str(response_line.trim()).expect("parse response");
            assert!(matches!(response, Response::Health { .. }));
        }
    }

    /// Shutdown request should return ShutdownAck.
    #[tokio::test]
    async fn shutdown_returns_ack() {
        let dir = tempfile::tempdir().expect("create temp dir");
        let _guard = DB_MUTEX.lock().unwrap_or_else(|e| e.into_inner());
        let config = Arc::new(test_config(&dir));
        let manager = Arc::new(Manager::new(&config).await.expect("create manager"));
        let (mut client, _handle) = spawn_handler(config, manager).await;

        client
            .write_all(b"{\"type\":\"Shutdown\"}\n")
            .await
            .unwrap();

        let mut reader = BufReader::new(&mut client);
        let mut response_line = String::new();
        read_line_limited(&mut reader, &mut response_line, MAX_MESSAGE_SIZE)
            .await
            .expect("read response line");

        let response: Response =
            serde_json::from_str(response_line.trim()).expect("parse response");
        assert!(matches!(response, Response::ShutdownAck));
    }

    /// SPA detection: combined flags (body short + no semantic + spa root).
    #[test]
    fn detect_spa_combined_flags() {
        let html = "<html><body><div id=\"root\">Loading...</div></body></html>";
        let (is_spa, warning) = detect_spa(html);
        assert!(
            is_spa,
            "SPA with short body, spa root, and no semantics should be detected"
        );
        assert!(warning.contains("SKIPJACKD_SPA_DETECTED"));
    }

    /// SPA detection: normal HTML page with paragraphs should not be flagged.
    #[test]
    fn detect_spa_normal_page_not_flagged() {
        let long_text = "x".repeat(1024);
        let html = format!(
            "<html><body><p>{}</p><section><h1>Title</h1><article>Content</article></section></body></html>",
            long_text
        );
        let (is_spa, _) = detect_spa(&html);
        assert!(
            !is_spa,
            "Normal page with semantic content should not be flagged as SPA"
        );
    }

    /// SPA detection: body text too short after stripping.
    #[test]
    fn detect_spa_body_text_too_short() {
        let html =
            "<html><body><script>lots of noise here but stripped</script> short </body></html>";
        let (is_spa, warning) = detect_spa(html);
        assert!(is_spa);
        // body_text_too_short is a debug-log flag, not in the user-facing warning.
        assert!(warning.contains("SKIPJACKD_SPA_DETECTED"));
    }

    /// SPA detection: no semantic elements.
    #[test]
    fn detect_spa_no_semantic_elements() {
        let long_text = "x".repeat(1024);
        let html = format!("<html><body><div>{}</div></body></html>", long_text);
        let (is_spa, _) = detect_spa(&html);
        assert!(
            is_spa,
            "Page with no p/h/article/main/section tags should be flagged"
        );
    }

    /// SPA detection: known SPA root elements trigger detection.
    #[test]
    fn detect_spa_root_elements() {
        let roots = [
            "<div id=\"root\">",
            "<div id='app'>",
            r#"<div id="__next">"#,
            "<div id='__nuxt'>",
        ];
        let long_text = "x".repeat(1024);
        for root in &roots {
            let html = format!("<html><body>{}<p>{}</p></body></html>", root, long_text);
            let (is_spa, _) = detect_spa(&html);
            assert!(
                is_spa,
                "SPA root element '{}' should trigger detection",
                root
            );
        }
    }

    /// strip_tags: removes script, style, and noscript content.
    #[test]
    fn strip_tags_removes_targeted_tags() {
        let html = "<html><head><style>body { color: red; }</style></head><body><script>alert(1)</script><p>Hello</p><noscript>No JS</noscript></body></html>";
        let result = strip_tags(html, &["script", "style", "noscript"]);
        assert!(!result.contains("<script"));
        assert!(!result.contains("alert(1)"));
        assert!(!result.contains("<style"));
        assert!(!result.contains("color: red"));
        assert!(!result.contains("<noscript"));
        assert!(!result.contains("No JS"));
        assert!(result.contains("<p>Hello</p>"));
    }

    /// strip_tags: leaving targeted tags out leaves content intact.
    #[test]
    fn strip_tags_leaves_other_content_intact() {
        let html = "<html><body><p>Para</p><div>Div</div></body></html>";
        let result = strip_tags(html, &["script", "style"]);
        assert!(result.contains("<p>Para</p>"));
        assert!(result.contains("<div>Div</div>"));
    }

    // -----------------------------------------------------------------------
    // validate_url tests
    // -----------------------------------------------------------------------

    /// Valid https URL should pass validation.
    #[test]
    fn validate_url_accepts_https() {
        assert!(validate_url("https://example.com").is_ok());
    }

    /// Valid http URL should pass validation.
    #[test]
    fn validate_url_accepts_http() {
        assert!(validate_url("http://example.com").is_ok());
    }

    /// file:// scheme should be rejected.
    #[test]
    fn validate_url_rejects_file_scheme() {
        let result = validate_url("file:///etc/passwd");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Unsupported URL scheme"));
    }

    /// ftp:// scheme should be rejected.
    #[test]
    fn validate_url_rejects_ftp_scheme() {
        let result = validate_url("ftp://example.com");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Unsupported URL scheme"));
    }

    /// localhost hostname should be rejected.
    #[test]
    fn validate_url_rejects_localhost() {
        let result = validate_url("http://localhost:8080/admin");
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err(),
            "URL resolves to a restricted network address"
        );
    }

    /// 127.0.0.1 loopback IP should be rejected.
    #[test]
    fn validate_url_rejects_loopback_ip() {
        let result = validate_url("http://127.0.0.1:6379/");
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err(),
            "URL resolves to a restricted network address"
        );
    }

    /// 127.x.x.x loopback range should be rejected.
    #[test]
    fn validate_url_rejects_loopback_range() {
        let result = validate_url("http://127.99.0.1/");
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err(),
            "URL resolves to a restricted network address"
        );
    }

    /// 192.168.x.x private IP should be rejected.
    #[test]
    fn validate_url_rejects_private_192_168() {
        let result = validate_url("http://192.168.1.1/");
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err(),
            "URL resolves to a restricted network address"
        );
    }

    /// 10.x.x.x private IP should be rejected.
    #[test]
    fn validate_url_rejects_private_10() {
        let result = validate_url("http://10.0.0.1/admin");
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err(),
            "URL resolves to a restricted network address"
        );
    }

    /// 172.16.x.x private IP should be rejected.
    #[test]
    fn validate_url_rejects_private_172_16() {
        let result = validate_url("http://172.16.0.1/");
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err(),
            "URL resolves to a restricted network address"
        );
    }

    /// 169.254.x.x link-local / cloud metadata IP should be rejected.
    #[test]
    fn validate_url_rejects_link_local() {
        let result = validate_url("http://169.254.169.254/latest/meta-data/");
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err(),
            "URL resolves to a restricted network address"
        );
    }

    /// 0.0.0.0/8 current network range should be rejected.
    #[test]
    fn validate_url_rejects_current_network() {
        let result = validate_url("http://0.0.0.0:9200/");
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err(),
            "URL resolves to a restricted network address"
        );
    }

    /// IPv6 loopback ::1 should be rejected.
    #[test]
    fn validate_url_rejects_ipv6_loopback() {
        let result = validate_url("http://[::1]:8080/admin");
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err(),
            "URL resolves to a restricted network address"
        );
    }

    /// IPv6 link-local fe80:: should be rejected.
    #[test]
    fn validate_url_rejects_ipv6_link_local() {
        let result = validate_url("http://[fe80::1]:8080/");
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err(),
            "URL resolves to a restricted network address"
        );
    }

    /// metadata.google.internal hostname should be rejected.
    #[test]
    fn validate_url_rejects_gcp_metadata_hostname() {
        let result = validate_url("http://metadata.google.internal/computeMetadata/v1/");
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err(),
            "URL resolves to a restricted network address"
        );
    }

    /// Malformed URL should return parse error.
    #[test]
    fn validate_url_rejects_malformed() {
        let result = validate_url("not a url at all");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Invalid URL"));
    }

    /// URL with no host (e.g., http:///) should be rejected.
    #[test]
    fn validate_url_rejects_no_host() {
        let result = validate_url("http://");
        assert!(result.is_err());
    }

    /// Sending a message over 1MB should return an Error with code -32600.
    #[tokio::test]
    async fn oversized_message_returns_error() {
        let dir = tempfile::tempdir().expect("create temp dir");
        let _guard = DB_MUTEX.lock().unwrap_or_else(|e| e.into_inner());
        let config = Arc::new(test_config(&dir));
        let manager = Arc::new(Manager::new(&config).await.expect("create manager"));
        let (mut client, _handle) = spawn_handler(config, manager).await;

        // Build a JSON message where the body exceeds 1MB.
        let padding = "A".repeat(MAX_MESSAGE_SIZE + 1024);
        let req_json = format!("{{\"type\":\"Health\",\"pad\":\"{}\"}}\n", padding);
        client.write_all(req_json.as_bytes()).await.unwrap();

        let mut reader = BufReader::new(&mut client);
        let mut response_line = String::new();
        read_line_limited(&mut reader, &mut response_line, MAX_MESSAGE_SIZE)
            .await
            .expect("read response line");

        let response: Response =
            serde_json::from_str(response_line.trim()).expect("parse response");
        match response {
            Response::Error { code, message, .. } => {
                assert_eq!(code, -32600, "expected invalid request code -32600");
                assert!(
                    message.to_lowercase().contains("too large"),
                    "expected 'too large' message, got: {}",
                    message
                );
            }
            other => panic!("expected Error response, got: {:?}", other),
        }
    }
}
