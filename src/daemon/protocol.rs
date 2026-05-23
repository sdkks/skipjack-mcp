//! Daemon wire protocol types — JSON newline-delimited messages over Unix domain socket.
//!
//! Every message on the wire is a single line of JSON terminated by `\n`.
//! Both [`Request`] and [`Response`] use `#[serde(tag = "type")]` so the
//! variant is determined by the `"type"` field in the JSON object.
//!
//! # Frame limits
//!
//! Request bodies larger than 1 MB are rejected before parsing. The 1 MB limit
//! is enforced at the socket read layer (see [`super::server`]).

use serde::{Deserialize, Serialize};

use crate::search::{SearchRequest, SearchResponse};

/// Maximum size of a single JSON message over the wire, in bytes.
///
/// Messages exceeding this limit are rejected before deserialization.
/// This matches the termin8r protocol limit and NFR-1.1 expectations.
pub const MAX_MESSAGE_SIZE: usize = 1_048_576; // 1 MB

// ---------------------------------------------------------------------------
// Request enum
// ---------------------------------------------------------------------------

/// A request sent from the MCP server or CLI client to the daemon.
///
/// Variants are discriminated by the `"type"` field in the JSON object.
/// The `Search` variant wraps the canonical [`SearchRequest`] directly.
///
/// # Wire format examples
///
/// ```json
/// {"type":"Search","request_id":"...","query":"rust async","limit":10, ...}
/// {"type":"Health"}
/// {"type":"Shutdown"}
/// ```
///
/// # Adding new variants
///
/// To add a new request type, add a variant to this enum and handle it in
/// [`super::server::handle_request`]. The `#[serde(tag = "type")]` attribute
/// ensures the variant name (e.g. `"Search"`) appears as the `"type"` field
/// in the serialized JSON, and that incoming JSON is dispatched to the correct
/// variant during deserialization.
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum Request {
    /// Execute a search across configured providers.
    #[serde(rename = "Search")]
    Search(SearchRequest),

    /// List all configured providers with descriptions, tags, and health.
    #[serde(rename = "ListProviders")]
    ListProviders,

    /// Retrieve cache statistics (hit rate, entry count, total size).
    #[serde(rename = "CacheStats")]
    CacheStats,

    /// Get detailed provider health statuses.
    #[serde(rename = "ProviderStatus")]
    ProviderStatus,

    /// Clear cache entries, optionally filtered by provider name.
    #[serde(rename = "CacheClear")]
    CacheClear {
        /// If `Some`, only clear entries from this provider.
        /// If `None`, clear all cache entries.
        provider: Option<String>,
    },

    /// Health check — returns daemon status, uptime, and version.
    #[serde(rename = "Health")]
    Health,

    /// Gracefully shut down the daemon.
    #[serde(rename = "Shutdown")]
    Shutdown,
}

// ---------------------------------------------------------------------------
// Response enum
// ---------------------------------------------------------------------------

/// A response sent from the daemon to the MCP server or CLI client.
///
/// Every [`Request`] produces exactly one [`Response`]. Errors are
/// communicated via the [`Response::Error`] variant, not by closing the
/// connection. The connection remains open for additional requests after
/// any response (including errors).
///
/// # Wire format examples
///
/// ```json
/// {"type":"SearchResult","request_id":"...","results":[...],"total_found":5,...}
/// {"type":"Health","status":"ok","uptime_secs":3600,"version":"0.1.0"}
/// {"type":"Error","code":-32601,"message":"Not yet implemented","data":null}
/// ```
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum Response {
    /// Successful search response with ranked, deduplicated results.
    #[serde(rename = "SearchResult")]
    SearchResult(SearchResponse),

    /// List of all configured providers with metadata.
    #[serde(rename = "ProviderList")]
    ProviderList {
        /// Configured providers with current availability and health.
        providers: Vec<ProviderInfo>,
    },

    /// Cache statistics.
    #[serde(rename = "CacheStats")]
    CacheStats {
        /// Total number of entries in the cache.
        total_entries: u64,
        /// Approximate total size of cached responses in bytes.
        total_size_bytes: u64,
        /// Cumulative cache hit count since daemon start.
        hit_count: u64,
        /// Cumulative cache miss count since daemon start.
        miss_count: u64,
        /// Cache hit rate as a fraction (0.0–1.0).
        hit_rate: f64,
    },

    /// Detailed provider health information.
    #[serde(rename = "ProviderStatus")]
    ProviderStatus {
        /// Health status for each configured provider.
        providers: Vec<ProviderHealth>,
    },

    /// Result of a cache-clear operation.
    #[serde(rename = "CacheCleared")]
    CacheCleared {
        /// Number of cache entries that were removed.
        removed: u64,
    },

    /// Daemon health check response.
    #[serde(rename = "Health")]
    Health {
        /// Always `"ok"` when the daemon is running.
        status: String,
        /// Seconds since the daemon started.
        uptime_secs: u64,
        /// Daemon version string (from `CARGO_PKG_VERSION`).
        version: String,
    },

    /// An error occurred while processing the request.
    #[serde(rename = "Error")]
    Error {
        /// JSON-RPC-compatible error code (negative for standard errors).
        code: i32,
        /// Human-readable error message.
        message: String,
        /// Optional additional error data.
        data: Option<serde_json::Value>,
    },

    /// Acknowledgment that the daemon is shutting down.
    #[serde(rename = "ShutdownAck")]
    ShutdownAck,
}

// ---------------------------------------------------------------------------
// Auxiliary types
// ---------------------------------------------------------------------------

/// Information about a single search provider, returned by [`Request::ListProviders`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderInfo {
    /// Unique provider name (matches the key in the provider catalog).
    pub name: String,
    /// Human-readable description of the provider.
    pub description: String,
    /// Semantic tags categorizing the provider's capabilities.
    pub tags: Vec<String>,
    /// Whether the provider is configured and available for use.
    pub available: bool,
    /// Whether the provider is currently healthy (not degraded or unhealthy).
    pub healthy: bool,
    /// Current health score (0.0–1.0), based on recent success/failure ratio.
    pub health_score: f64,
}

/// Detailed health information for a single provider, returned by [`Request::ProviderStatus`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderHealth {
    /// Unique provider name.
    pub name: String,
    /// Whether the provider is healthy (success rate above threshold).
    pub healthy: bool,
    /// Whether the provider is degraded (success rate below threshold but not yet unhealthy).
    pub degraded: bool,
    /// Current health score (0.0–1.0).
    pub health_score: f64,
    /// Total successful requests since daemon start (or last reset).
    pub success_count: u64,
    /// Total failed requests since daemon start (or last reset).
    pub failure_count: u64,
    /// The last error message, if any.
    pub last_error: Option<String>,
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// The `type` field must round-trip correctly for every Request variant.
    #[test]
    fn request_roundtrip_search() {
        let req = Request::Search(SearchRequest {
            request_id: "test-id".into(),
            query: "hello".into(),
            limit: 5,
            providers: vec![],
            language: None,
            country: None,
            safe_search: true,
            freshness: None,
            dispatch_mode: None,
        });

        let json = serde_json::to_string(&req).expect("serialize");
        let parsed: Request = serde_json::from_str(&json).expect("deserialize");

        match parsed {
            Request::Search(sr) => {
                assert_eq!(sr.query, "hello");
                assert_eq!(sr.limit, 5);
            }
            _ => panic!("expected Search variant"),
        }
    }

    /// All Request variants must serialize with the correct `type` field.
    #[test]
    fn request_type_field_values() {
        let test_cases: Vec<(&str, Request)> = vec![
            (
                "Search",
                Request::Search(SearchRequest {
                    request_id: "id".into(),
                    query: "q".into(),
                    limit: 1,
                    providers: vec![],
                    language: None,
                    country: None,
                    safe_search: true,
                    freshness: None,
                    dispatch_mode: None,
                }),
            ),
            ("ListProviders", Request::ListProviders),
            ("CacheStats", Request::CacheStats),
            ("ProviderStatus", Request::ProviderStatus),
            (
                "CacheClear",
                Request::CacheClear {
                    provider: Some("test".into()),
                },
            ),
            ("Health", Request::Health),
            ("Shutdown", Request::Shutdown),
        ];

        for (expected_type, req) in test_cases {
            let json = serde_json::to_string(&req).expect("serialize");
            let value: serde_json::Value =
                serde_json::from_str(&json).expect("parse as generic JSON");
            assert_eq!(
                value["type"].as_str().unwrap(),
                expected_type,
                "expected type={} in: {}",
                expected_type,
                json
            );
        }
    }

    /// All Response variants must round-trip through JSON.
    #[test]
    fn response_roundtrip_all_variants() {
        // Health
        let resp = Response::Health {
            status: "ok".into(),
            uptime_secs: 42,
            version: "0.1.0".into(),
        };
        let json = serde_json::to_string(&resp).expect("serialize Health");
        let parsed: Response = serde_json::from_str(&json).expect("deserialize Health");
        match parsed {
            Response::Health { status, .. } => assert_eq!(status, "ok"),
            _ => panic!("expected Health"),
        }

        // Error
        let resp = Response::Error {
            code: -32600,
            message: "bad request".into(),
            data: None,
        };
        let json = serde_json::to_string(&resp).expect("serialize Error");
        let parsed: Response = serde_json::from_str(&json).expect("deserialize Error");
        match parsed {
            Response::Error { code, .. } => assert_eq!(code, -32600),
            _ => panic!("expected Error"),
        }

        // ShutdownAck
        let resp = Response::ShutdownAck;
        let json = serde_json::to_string(&resp).expect("serialize ShutdownAck");
        let parsed: Response = serde_json::from_str(&json).expect("deserialize ShutdownAck");
        assert!(matches!(parsed, Response::ShutdownAck));

        // CacheCleared
        let resp = Response::CacheCleared { removed: 5 };
        let json = serde_json::to_string(&resp).expect("serialize CacheCleared");
        let parsed: Response = serde_json::from_str(&json).expect("deserialize CacheCleared");
        match parsed {
            Response::CacheCleared { removed } => assert_eq!(removed, 5),
            _ => panic!("expected CacheCleared"),
        }
    }

    /// Deserializing a Response from JSON should infer the variant from the `type` field.
    #[test]
    fn deserialize_response_from_type_field() {
        let json = r#"{"type":"Error","code":-32601,"message":"not found","data":null}"#;
        let resp: Response = serde_json::from_str(json).expect("parse Error response");
        match resp {
            Response::Error { code, message, .. } => {
                assert_eq!(code, -32601);
                assert_eq!(message, "not found");
            }
            _ => panic!("expected Error variant"),
        }
    }

    /// ProviderInfo should serialize and deserialize correctly.
    #[test]
    fn provider_info_roundtrip() {
        let info = ProviderInfo {
            name: "duckduckgo".into(),
            description: "Privacy-focused search engine".into(),
            tags: vec!["Web".into(), "Privacy".into()],
            available: true,
            healthy: true,
            health_score: 0.95,
        };

        let json = serde_json::to_string(&info).expect("serialize");
        let parsed: ProviderInfo = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(parsed.name, "duckduckgo");
        assert_eq!(parsed.tags.len(), 2);
        assert!(parsed.healthy);
    }

    /// ProviderHealth should serialize and deserialize correctly.
    #[test]
    fn provider_health_roundtrip() {
        let health = ProviderHealth {
            name: "brave".into(),
            healthy: false,
            degraded: true,
            health_score: 0.4,
            success_count: 10,
            failure_count: 15,
            last_error: Some("rate limited".into()),
        };

        let json = serde_json::to_string(&health).expect("serialize");
        let parsed: ProviderHealth = serde_json::from_str(&json).expect("deserialize");
        assert!(!parsed.healthy);
        assert!(parsed.degraded);
        assert_eq!(parsed.failure_count, 15);
        assert_eq!(parsed.last_error.as_deref(), Some("rate limited"));
    }
}
