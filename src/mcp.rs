//! MCP server integration (STORY-0007 / FR-2.1 through FR-2.8).
//!
//! The MCP server listens on stdio and proxies requests to the metasearch daemon
//! via its Unix domain socket. It exposes three tools:
//!
//! - `search`       -- execute a multi-provider search
//! - `list_providers` -- list all configured providers with health status
//! - `cache_stats`   -- retrieve cache hit/miss statistics
//!
//! The server survives daemon restarts: each tool call opens a fresh connection
//! to the daemon socket and reports `-32000` (daemon unreachable) when the
//! connection cannot be established.

use std::sync::Arc;
use std::time::Duration;

use rmcp::{
    handler::server::router::Router,
    handler::server::tool::{ToolCallContext, ToolRoute},
    model::{CallToolResult, ErrorCode, Implementation, ServerInfo, Tool},
    serve_server, ErrorData, ServerHandler,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;

use crate::config::Config;
use crate::daemon::protocol::{Request, Response, MAX_MESSAGE_SIZE};
use crate::search::SearchRequest;

// ---------------------------------------------------------------------------
// Tool parameter structs
// ---------------------------------------------------------------------------

/// Parameters for the `search` tool.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SearchParams {
    /// The search query string (required).
    pub query: String,
    /// Maximum number of results to return (default 10, max 100).
    #[serde(default = "default_limit")]
    pub limit: usize,
    /// Specific provider names to use. Empty means auto-tier dispatch.
    #[serde(default)]
    pub providers: Vec<String>,
    /// ISO 639-1 language code (e.g., "en", "fr").
    pub language: Option<String>,
    /// ISO 3166-1 alpha-2 country code (e.g., "us", "gb").
    pub country: Option<String>,
    /// Whether to enable safe search filtering (default true).
    #[serde(default = "default_safe_search")]
    pub safe_search: bool,
    /// Time-based freshness filter: "Day", "Week", "Month", or "Year".
    pub freshness: Option<String>,
    /// Dispatch mode override: "concurrent" or "tiered". None uses config default.
    pub dispatch_mode: Option<String>,
}

fn default_limit() -> usize {
    10
}
fn default_safe_search() -> bool {
    true
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Convert a `serde_json::Value` object to a `serde_json::Map` for use with
/// `Tool::new` which requires `Into<Arc<JsonObject>>`.
fn to_json_object(value: serde_json::Value) -> serde_json::Map<String, serde_json::Value> {
    match value {
        serde_json::Value::Object(map) => map,
        _ => serde_json::Map::new(),
    }
}

/// Map a daemon-level error code to the appropriate MCP error code.
///
/// - `-32602` (invalid params) passes through unchanged.
/// - `-32601` (not implemented) downgrades to `-32000` (daemon issue).
/// - All other daemon codes (e.g., `-32002` "no providers available") pass through.
fn map_daemon_error(code: i32) -> i32 {
    match code {
        -32602 => -32602,
        -32601 => -32000,
        _ => code,
    }
}

/// Sanitize a daemon error message before forwarding to the MCP client.
///
/// Logs the raw daemon error at `WARN` level and returns a generic message so
/// internal details (paths, provider config, partial stack traces) are not
/// exposed to the MCP client or the LLM it serves.
fn sanitize_daemon_error(raw_message: &str) -> String {
    tracing::warn!(raw_message, "daemon error forwarded to MCP client (sanitized)");
    "An internal daemon error occurred".to_string()
}

// ---------------------------------------------------------------------------
// Daemon client
// ---------------------------------------------------------------------------

/// A lightweight client that connects to the metasearch daemon via Unix domain
/// socket, sends a [`Request`], and reads back a [`Response`].
///
/// The connection is opened fresh for each request so that the MCP server
/// survives daemon restarts without state management.
struct DaemonClient {
    socket_path: String,
    timeout: Duration,
}

impl DaemonClient {
    /// Build a client from configuration.
    fn new(config: &Config) -> Self {
        let socket_path = format!("{}/{}.sock", config.daemon.socket_dir, config.daemon.name);
        let timeout = Duration::from_secs(config.search.request_timeout_secs);
        Self {
            socket_path,
            timeout,
        }
    }

    /// Open a connection to the daemon, send `request` as a JSON line, and
    /// return the parsed response.
    ///
    /// # Errors
    ///
    /// Returns [`ErrorData`] with:
    /// - `-32000` when the daemon socket cannot be reached,
    /// - `-32001` when the request times out,
    /// - `-32602` when the request cannot be serialized or the response cannot
    ///   be parsed.
    async fn send_request(&self, request: &Request) -> Result<Response, ErrorData> {
        // Connect to daemon socket.
        let stream = UnixStream::connect(&self.socket_path)
            .await
            .map_err(|e| {
                ErrorData::new(
                    ErrorCode(-32000),
                    format!("Daemon unreachable: {}", e),
                    None,
                )
            })?;

        let (reader, mut writer) = tokio::io::split(stream);
        let mut buf_reader = BufReader::new(reader);

        // Serialize and send request.
        let mut req_json =
            serde_json::to_string(request).map_err(|e| {
                ErrorData::invalid_params(format!("Failed to serialize request: {}", e), None)
            })?;
        req_json.push('\n');
        writer
            .write_all(req_json.as_bytes())
            .await
            .map_err(|e| {
                ErrorData::new(
                    ErrorCode(-32000),
                    format!("Failed to send request: {}", e),
                    None,
                )
            })?;
        writer
            .flush()
            .await
            .map_err(|e| {
                ErrorData::new(
                    ErrorCode(-32000),
                    format!("Failed to flush: {}", e),
                    None,
                )
            })?;

        // Read response with a timeout.
        let read_future = async {
            let mut line = String::new();
            read_line_limited(&mut buf_reader, &mut line).await?;
            let response = serde_json::from_str::<Response>(line.trim()).map_err(|e| {
                ErrorData::new(
                    ErrorCode(-32000),
                    format!("Failed to parse daemon response: {}", e),
                    None,
                )
            })?;
            Ok(response)
        };

        match tokio::time::timeout(self.timeout, read_future).await {
            Ok(result) => result,
            Err(_elapsed) => Err(ErrorData::new(
                ErrorCode(-32001),
                format!(
                    "Search request timed out after {}s",
                    self.timeout.as_secs()
                ),
                None,
            )),
        }
    }
}

/// Read a newline-terminated line from `reader` into `buf`, enforcing the
/// daemon's 1 MB message limit.
///
/// Returns an [`ErrorData`] with code `-32000` on size-limit breach or I/O
/// error so the MCP client sees "daemon communication error".
async fn read_line_limited<R: tokio::io::AsyncRead + Unpin>(
    reader: &mut BufReader<R>,
    buf: &mut String,
) -> Result<(), ErrorData> {
    buf.clear();
    let mut total: usize = 0;

    loop {
        let bytes_in_buffer = reader.buffer().len();
        if bytes_in_buffer == 0 {
            reader.fill_buf().await.map_err(|e| {
                ErrorData::new(
                    ErrorCode(-32000),
                    format!("Read error from daemon: {}", e),
                    None,
                )
            })?;
            if reader.buffer().is_empty() {
                // EOF before any data — daemon closed connection.
                return Err(ErrorData::new(
                    ErrorCode(-32000),
                    "Daemon closed connection before sending response".to_string(),
                    None,
                ));
            }
        }

        let buffer = reader.buffer();
        if let Some(pos) = buffer.iter().position(|&b| b == b'\n') {
            let chunk = &buffer[..pos];
            let chunk_str = std::str::from_utf8(chunk).unwrap_or("");
            buf.push_str(chunk_str);
            total += pos + 1;

            if total > MAX_MESSAGE_SIZE + 1 {
                return Err(ErrorData::new(
                    ErrorCode(-32000),
                    "Daemon response exceeded 1 MB limit".to_string(),
                    None,
                ));
            }

            reader.consume(pos + 1);
            return Ok(());
        }

        let remaining = buffer.len();
        if total + remaining > MAX_MESSAGE_SIZE + 1 {
            return Err(ErrorData::new(
                ErrorCode(-32000),
                "Daemon response exceeded 1 MB limit".to_string(),
                None,
            ));
        }

        let chunk_str = std::str::from_utf8(buffer).unwrap_or("");
        buf.push_str(chunk_str);
        total += remaining;
        reader.consume(remaining);
    }
}

// ---------------------------------------------------------------------------
// MCP Server handler
// ---------------------------------------------------------------------------

/// The MCP server implementation.
///
/// Implements [`ServerHandler`] via the default trait methods, with tool
/// dispatch handled by a [`Router`].
struct MetasearchServer {
    client: DaemonClient,
}

impl ServerHandler for MetasearchServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(Default::default())
            .with_server_info(Implementation::new(
                "metasearchd",
                env!("CARGO_PKG_VERSION"),
            ))
            .with_instructions(
                "Multi-provider web search daemon with anti-blocking, caching, and MCP integration",
            )
    }
}

impl MetasearchServer {
    /// Handle the `search` tool: parse params, dispatch to daemon, return results.
    async fn handle_search(
        &self,
        ctx: ToolCallContext<'_, MetasearchServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let args = ctx.arguments.unwrap_or_default();
        let params: SearchParams =
            serde_json::from_value(serde_json::Value::Object(args)).map_err(|e| {
                ErrorData::invalid_params(format!("Invalid search parameters: {}", e), None)
            })?;

        if params.query.trim().is_empty() {
            return Err(ErrorData::invalid_params(
                "Query must not be empty",
                None,
            ));
        }

        let freshness = match params.freshness.as_deref() {
            Some("Day") => Some(crate::search::Freshness::Day),
            Some("Week") => Some(crate::search::Freshness::Week),
            Some("Month") => Some(crate::search::Freshness::Month),
            Some("Year") => Some(crate::search::Freshness::Year),
            None => None,
            Some(other) => {
                return Err(ErrorData::invalid_params(
                    format!(
                        "Invalid freshness value '{}'. Must be one of: Day, Week, Month, Year",
                        other
                    ),
                    None,
                ));
            }
        };

        if let Some(ref mode) = params.dispatch_mode {
            if mode != "concurrent" && mode != "tiered" {
                return Err(ErrorData::invalid_params(
                    format!(
                        "Invalid dispatch_mode '{}'. Must be 'concurrent' or 'tiered'",
                        mode
                    ),
                    None,
                ));
            }
        }

        if params.limit == 0 || params.limit > 100 {
            return Err(ErrorData::invalid_params(
                format!(
                    "limit must be between 1 and 100, got {}",
                    params.limit
                ),
                None,
            ));
        }

        let search_req = SearchRequest {
            request_id: uuid::Uuid::new_v4().to_string(),
            query: params.query,
            limit: params.limit,
            providers: params.providers,
            language: params.language,
            country: params.country,
            safe_search: params.safe_search,
            freshness,
            dispatch_mode: params.dispatch_mode,
        };

        let response = self
            .client
            .send_request(&Request::Search(search_req))
            .await?;

        match response {
            Response::SearchResult(search_response) => {
                let json = serde_json::to_string(&search_response).map_err(|e| {
                    ErrorData::new(
                        ErrorCode(-32000),
                        format!("Failed to serialize search response: {}", e),
                        None,
                    )
                })?;
                Ok(CallToolResult::success(vec![rmcp::model::Content::text(
                    json,
                )]))
            }
            Response::Error { code, message, .. } => {
                Err(ErrorData::new(
                    ErrorCode(map_daemon_error(code)),
                    sanitize_daemon_error(&message),
                    None,
                ))
            }
            other => Err(ErrorData::new(
                ErrorCode(-32000),
                format!("Unexpected daemon response type for search: {:?}", other),
                None,
            )),
        }
    }

    /// Handle the `list_providers` tool.
    async fn handle_list_providers(&self) -> Result<CallToolResult, ErrorData> {
        let response = self.client.send_request(&Request::ListProviders).await?;

        match response {
            Response::ProviderList { providers } => {
                if providers.is_empty() {
                    return Ok(CallToolResult::success(vec![rmcp::model::Content::text(
                        "[]".to_string(),
                    )]));
                }
                let json = serde_json::to_string(&providers).map_err(|e| {
                    ErrorData::new(
                        ErrorCode(-32000),
                        format!("Failed to serialize provider list: {}", e),
                        None,
                    )
                })?;
                Ok(CallToolResult::success(vec![rmcp::model::Content::text(
                    json,
                )]))
            }
            Response::Error { code, message, .. } => {
                Err(ErrorData::new(
                    ErrorCode(map_daemon_error(code)),
                    sanitize_daemon_error(&message),
                    None,
                ))
            }
            other => Err(ErrorData::new(
                ErrorCode(-32000),
                format!(
                    "Unexpected daemon response type for list_providers: {:?}",
                    other
                ),
                None,
            )),
        }
    }

    /// Handle the `cache_stats` tool.
    async fn handle_cache_stats(&self) -> Result<CallToolResult, ErrorData> {
        let response = self.client.send_request(&Request::CacheStats).await?;

        match response {
            Response::CacheStats {
                total_entries,
                total_size_bytes,
                hit_count,
                miss_count,
                hit_rate,
            } => {
                let stats = serde_json::json!({
                    "total_entries": total_entries,
                    "total_size_bytes": total_size_bytes,
                    "hit_count": hit_count,
                    "miss_count": miss_count,
                    "hit_rate": hit_rate,
                });
                Ok(CallToolResult::success(vec![rmcp::model::Content::text(
                    stats.to_string(),
                )]))
            }
            Response::Error { code, message, .. } => {
                Err(ErrorData::new(
                    ErrorCode(map_daemon_error(code)),
                    sanitize_daemon_error(&message),
                    None,
                ))
            }
            other => Err(ErrorData::new(
                ErrorCode(-32000),
                format!(
                    "Unexpected daemon response type for cache_stats: {:?}",
                    other
                ),
                None,
            )),
        }
    }
}

// ---------------------------------------------------------------------------
// Public entry point
// ---------------------------------------------------------------------------

/// Start the MCP server on stdio.
///
/// This function blocks until the MCP transport is closed (i.e., the client
/// disconnects or stdin/stdout are closed).
///
/// # Errors
///
/// Returns an error if:
/// - The MCP transport cannot be created (unlikely with stdio).
/// - The MCP initialization handshake fails (e.g., protocol version mismatch).
pub async fn run(config: Arc<Config>) -> anyhow::Result<()> {
    let client = DaemonClient::new(&config);
    let server = Arc::new(MetasearchServer { client });
    let router = build_router(&server);

    tracing::info!("MCP server starting on stdio");
    serve_server(router, (tokio::io::stdin(), tokio::io::stdout())).await?;
    tracing::info!("MCP server stopped");
    Ok(())
}

/// Build the [`Router`] for the MCP server.
///
/// The router's inner service is a clone of the provided `server` so that
/// `get_info()` returns the correct server metadata. Tool handlers capture
/// their own `Arc<MetasearchServer>` pointing to the real daemon client.
fn build_router(server: &Arc<MetasearchServer>) -> Router<MetasearchServer> {
    let s = Arc::clone(server);

    let search_tool = Tool::new(
        "search",
        "Execute a multi-provider web search with caching, deduplication, and ranking",
        to_json_object(serde_json::json!({
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "The search query string"
                },
                "limit": {
                    "type": "integer",
                    "description": "Maximum number of results (default 10, max 100)",
                    "default": 10,
                    "minimum": 1,
                    "maximum": 100
                },
                "providers": {
                    "type": "array",
                    "description": "Specific provider names to use. Empty for auto-tier dispatch.",
                    "items": { "type": "string" },
                    "default": []
                },
                "language": {
                    "type": "string",
                    "description": "ISO 639-1 language code (e.g., 'en', 'fr')"
                },
                "country": {
                    "type": "string",
                    "description": "ISO 3166-1 alpha-2 country code (e.g., 'us', 'gb')"
                },
                "safe_search": {
                    "type": "boolean",
                    "description": "Enable safe search filtering (default true)",
                    "default": true
                },
                "freshness": {
                    "type": "string",
                    "description": "Time-based filter: 'Day', 'Week', 'Month', or 'Year'",
                    "enum": ["Day", "Week", "Month", "Year"]
                },
                "dispatch_mode": {
                    "type": "string",
                    "description": "Dispatch mode: 'concurrent' or 'tiered' (uses config default if unset)",
                    "enum": ["concurrent", "tiered"]
                }
            },
            "required": ["query"]
        })),
    );

    let search_handler = {
        let s = Arc::clone(&s);
        ToolRoute::new_dyn(search_tool, move |ctx: ToolCallContext<'_, MetasearchServer>| {
            let s = Arc::clone(&s);
            Box::pin(async move { s.handle_search(ctx).await })
        })
    };

    let list_providers_tool = Tool::new(
        "list_providers",
        "List all configured search providers with descriptions, tags, and health status",
        to_json_object(serde_json::json!({
            "type": "object",
            "properties": {}
        })),
    );

    let list_providers_handler = {
        let s = Arc::clone(&s);
        ToolRoute::new_dyn(
            list_providers_tool,
            move |_ctx: ToolCallContext<'_, MetasearchServer>| {
                let s = Arc::clone(&s);
                Box::pin(async move { s.handle_list_providers().await })
            },
        )
    };

    let cache_stats_tool = Tool::new(
        "cache_stats",
        "Retrieve cache statistics including hit/miss counts and entry count",
        to_json_object(serde_json::json!({
            "type": "object",
            "properties": {}
        })),
    );

    let cache_stats_handler = {
        let s = Arc::clone(&s);
        ToolRoute::new_dyn(
            cache_stats_tool,
            move |_ctx: ToolCallContext<'_, MetasearchServer>| {
                let s = Arc::clone(&s);
                Box::pin(async move { s.handle_cache_stats().await })
            },
        )
    };

    Router::new(s.as_ref().clone())
        .with_tool(search_handler)
        .with_tool(list_providers_handler)
        .with_tool(cache_stats_handler)
}

// Add Clone for MetasearchServer since Router needs to own it.
impl Clone for DaemonClient {
    fn clone(&self) -> Self {
        Self {
            socket_path: self.socket_path.clone(),
            timeout: self.timeout,
        }
    }
}

impl Clone for MetasearchServer {
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// The `SearchParams` struct should deserialize from JSON correctly.
    #[test]
    fn search_params_deserialize_full() {
        let json = serde_json::json!({
            "query": "rust async",
            "limit": 20,
            "providers": ["duckduckgo", "brave"],
            "language": "en",
            "country": "us",
            "safe_search": false,
            "freshness": "Week",
            "dispatch_mode": "concurrent"
        });
        let params: SearchParams = serde_json::from_value(json).unwrap();
        assert_eq!(params.query, "rust async");
        assert_eq!(params.limit, 20);
        assert_eq!(params.providers.len(), 2);
        assert_eq!(params.language.as_deref(), Some("en"));
        assert_eq!(params.country.as_deref(), Some("us"));
        assert!(!params.safe_search);
        assert_eq!(params.freshness.as_deref(), Some("Week"));
        assert_eq!(params.dispatch_mode.as_deref(), Some("concurrent"));
    }

    /// The `SearchParams` struct should use defaults for missing fields.
    #[test]
    fn search_params_deserialize_minimal() {
        let json = serde_json::json!({
            "query": "hello"
        });
        let params: SearchParams = serde_json::from_value(json).unwrap();
        assert_eq!(params.query, "hello");
        assert_eq!(params.limit, 10); // default
        assert!(params.providers.is_empty());
        assert!(params.language.is_none());
        assert!(params.country.is_none());
        assert!(params.safe_search); // default
        assert!(params.freshness.is_none());
        assert!(params.dispatch_mode.is_none());
    }

    /// DaemonClient construction should derive the correct socket path from config.
    #[test]
    fn daemon_client_socket_path_from_config() {
        let mut config = Config::default();
        config.daemon.socket_dir = "/var/run".into();
        config.daemon.name = "searchd".into();
        config.search.request_timeout_secs = 45;

        let client = DaemonClient::new(&config);
        assert_eq!(client.socket_path, "/var/run/searchd.sock");
        assert_eq!(client.timeout, Duration::from_secs(45));
    }

    /// Serialize then deserialize -- params should round-trip.
    #[test]
    fn search_params_roundtrip() {
        let original = SearchParams {
            query: "test query".into(),
            limit: 5,
            providers: vec!["brave".into()],
            language: Some("de".into()),
            country: Some("de".into()),
            safe_search: true,
            freshness: Some("Month".into()),
            dispatch_mode: Some("tiered".into()),
        };
        let json = serde_json::to_value(&original).unwrap();
        let parsed: SearchParams = serde_json::from_value(json).unwrap();
        assert_eq!(parsed.query, original.query);
        assert_eq!(parsed.limit, original.limit);
        assert_eq!(parsed.freshness, original.freshness);
        assert_eq!(parsed.dispatch_mode, original.dispatch_mode);
    }
}
