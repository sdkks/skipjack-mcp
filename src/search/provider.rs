use std::time::Duration;

use async_trait::async_trait;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use super::request::{SearchRequest, SearchResponse};

// ---------------------------------------------------------------------------
// ProviderClientConfig — minimal, avoids circular dependency with config.rs
// ---------------------------------------------------------------------------

/// Minimal client configuration passed into [`Provider::build_client`].
///
/// This avoids a circular dependency between the `search` and `config` modules.
/// The full `ProviderConfig` from `config.rs` is converted into this struct
/// before being handed to a provider.
#[derive(Debug, Clone)]
pub struct ProviderClientConfig {
    /// Whether to randomize TLS cipher suite order (anti-fingerprinting).
    /// Maps to `tls_shuffle_ciphers` in the provider config TOML section.
    pub tls_shuffle_ciphers: bool,
    /// IP rotation strategy: `"static"`, `"ipv6_pool"`, or `"proxy_pool"`.
    pub ip_rotation_strategy: Option<String>,
    /// IPv6 subnet in CIDR notation (e.g., `"2001:db8::/64"`) for `ipv6_pool` strategy.
    pub ipv6_subnet: Option<String>,
    /// SOCKS5/HTTP proxy URLs for `proxy_pool` strategy.
    pub proxies: Option<Vec<String>>,
    /// Per-provider request timeout override in seconds. Falls back to the
    /// global default (30 s) when `None`.
    pub timeout_secs: Option<u64>,
}

// ---------------------------------------------------------------------------
// Tag enum
// ---------------------------------------------------------------------------

/// Semantic tags describing a provider's capabilities and data domain.
///
/// Tags are used by the provider catalog to filter and select providers
/// based on the type of search requested, and to assign providers to
/// tiers in the fallback ladder.
///
/// # Example
///
/// ```
/// use skipjackd::search::Tag;
///
/// let tag: Tag = serde_json::from_str(r#""Web""#).unwrap();
/// assert_eq!(tag, Tag::Web);
///
/// let tag: Tag = serde_json::from_str(r#""Knowledge""#).unwrap();
/// assert_eq!(tag, Tag::Knowledge);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Tag {
    /// General web search.
    Web,
    /// News articles and current events.
    News,
    /// Academic papers, journals, and scholarly sources.
    Academic,
    /// Source code, documentation, and developer resources.
    Code,
    /// Privacy-respecting search engines (no tracking, no logs).
    Privacy,
    /// Financial data, stock quotes, market information.
    Finance,
    /// Image search results.
    Images,
    /// Video search results.
    Video,
    /// Shopping and product search.
    Shopping,
    /// Encyclopedic knowledge, facts, and reference material.
    Knowledge,
    /// Browser-automation scraping (Playwright-based). Tier-5 only; excluded
    /// from concurrent dispatch per DR-001.
    Playwright,
}

// ---------------------------------------------------------------------------
// ProviderError enum
// ---------------------------------------------------------------------------

/// Errors that can occur during a provider search.
///
/// Implements `std::error::Error` via `thiserror`. Used uniformly across all
/// provider implementations so the dispatch engine can handle errors
/// consistently.
///
/// # Example
///
/// ```
/// use skipjackd::search::ProviderError;
///
/// let err = ProviderError::HttpError {
///     status: 503,
///     body: "Service Unavailable".into(),
/// };
/// assert_eq!(err.to_string(), "HTTP error: status 503");
///
/// let timeout = ProviderError::Timeout { elapsed_secs: 30 };
/// assert_eq!(timeout.to_string(), "Timeout after 30s");
///
/// let rate = ProviderError::RateLimited { retry_after: Some(60) };
/// assert!(matches!(rate, ProviderError::RateLimited { .. }));
/// ```
#[derive(Debug, Clone, Error, Serialize, Deserialize)]
pub enum ProviderError {
    /// An HTTP error response (e.g., 500, 503). Retryable depending on status.
    #[error("HTTP error: status {status}")]
    HttpError {
        /// The HTTP status code returned by the provider.
        status: u16,
        /// The response body, truncated for logging.
        body: String,
    },

    /// Rate limit hit (HTTP 429).
    #[error("Rate limited (429)")]
    RateLimited {
        /// Value of the `Retry-After` header in seconds, if the provider sent one.
        retry_after: Option<u64>,
    },

    /// Access denied (HTTP 403). Terminal — not retried.
    #[error("Access denied (403)")]
    AccessDenied,

    /// A CAPTCHA challenge was detected in the provider's response.
    #[error("Captcha detected")]
    CaptchaDetected {
        /// Name of the provider that returned the CAPTCHA.
        provider: String,
    },

    /// The request exceeded the configured timeout.
    #[error("Timeout after {elapsed_secs}s")]
    Timeout {
        /// Elapsed wall-clock time in seconds.
        elapsed_secs: u64,
    },

    /// Failed to parse the provider's response into the canonical format.
    #[error("Parse error: {0}")]
    ParseError(String),

    /// The provider is referenced in the catalog or request but is not
    /// configured (missing API key, missing URL, etc.).
    #[error("Not configured: {0}")]
    NotConfigured(String),

    /// An internal error within the provider implementation.
    #[error("Internal: {0}")]
    Internal(String),
}

// ---------------------------------------------------------------------------
// Provider trait
// ---------------------------------------------------------------------------

/// The core trait that every search provider must implement.
///
/// The trait is object-safe (`Send + Sync`) so providers can be stored in a
/// `Box<dyn Provider>` inside the [`super::ProviderCatalog`] and dispatched
/// concurrently via `tokio::spawn`.
///
/// # Implementing a new provider
///
/// 1. Define a struct for the provider (e.g., `struct DuckDuckGo { ... }`).
/// 2. Implement [`Provider`] for it, providing at minimum: [`name`](Provider::name),
///    [`tags`](Provider::tags), [`description`](Provider::description),
///    [`is_available`](Provider::is_available), and [`search`](Provider::search).
/// 3. Register it in the provider catalog at startup.
///
/// The default implementation of [`build_client`](Provider::build_client)
/// constructs a real `reqwest::Client` using the supplied [`ProviderClientConfig`].
/// Most providers can use the default; only providers with unusual TLS or proxy
/// requirements need to override it.
///
/// # Example
///
/// ```ignore
/// use skipjackd::search::{Provider, ProviderError, Tag, SearchRequest, SearchResponse};
/// use skipjackd::search::provider::ProviderClientConfig;
/// use async_trait::async_trait;
///
/// struct MyProvider {
///     available: bool,
/// }
///
/// #[async_trait]
/// impl Provider for MyProvider {
///     fn name(&self) -> &str {
///         "my_provider"
///     }
///
///     fn tags(&self) -> &[Tag] {
///         &[Tag::Web, Tag::Privacy]
///     }
///
///     fn description(&self) -> &str {
///         "A hypothetical search provider."
///     }
///
///     fn is_available(&self) -> bool {
///         self.available
///     }
///
///     async fn search(
///         &self,
///         _request: &SearchRequest,
///     ) -> Result<SearchResponse, ProviderError> {
///         Err(ProviderError::Internal("not implemented".into()))
///     }
/// }
/// ```
#[async_trait]
pub trait Provider: Send + Sync {
    /// Unique provider identifier (e.g., `"duckduckgo"`, `"brave"`).
    ///
    /// This name is used as the key in the [`super::ProviderCatalog`] and
    /// appears in `providers_used` in [`SearchResponse`].
    fn name(&self) -> &str;

    /// Semantic tags describing this provider's data domain and capabilities.
    ///
    /// Used for catalog filtering and tier assignment. The returned slice
    /// should be static or have a lifetime tied to the provider instance.
    fn tags(&self) -> &[Tag];

    /// Human-readable description of the provider, shown in `status` and
    /// `providers` CLI output as well as the MCP `list_providers` tool.
    fn description(&self) -> &str;

    /// Whether the provider is configured and ready to serve search requests.
    ///
    /// Returns `false` if:
    /// - The provider is explicitly disabled in configuration,
    /// - Required API keys are missing,
    /// - The provider's endpoint is known to be unreachable.
    ///
    /// This is a synchronous check; runtime health tracking (degraded/unhealthy
    /// state) is managed externally by the health-tracking subsystem.
    fn is_available(&self) -> bool;

    /// Execute a search against this provider.
    ///
    /// # Arguments
    ///
    /// * `request` — The search parameters including query, limit, language,
    ///   country, safe search, and freshness filter.
    ///
    /// # Errors
    ///
    /// Returns [`ProviderError`] on any failure: HTTP errors, timeouts, rate
    /// limiting, CAPTCHA detection, parse failures, or internal errors.
    async fn search(&self, request: &SearchRequest) -> Result<SearchResponse, ProviderError>;

    /// Build a `reqwest::Client` configured according to the supplied config.
    ///
    /// The default implementation handles:
    /// - Timeout configuration (`connect_timeout` and `timeout`)
    /// - Connection pool tuning
    /// - TLS setup via rustls
    /// - SOCKS5/HTTP proxy configuration (first proxy in the list)
    ///
    /// Providers with unusual connection requirements (custom TLS config,
    /// per-request proxy rotation, IPv6 binding via `socket2`) should override
    /// this method.
    ///
    /// # Errors
    ///
    /// Returns [`ProviderError::Internal`] if the client cannot be constructed
    /// (e.g., invalid proxy URL, TLS initialization failure).
    async fn build_client(&self, config: &ProviderClientConfig) -> Result<Client, ProviderError> {
        let mut builder = Client::builder();

        // Timeout: use per-provider override or default to 30 seconds.
        let timeout_secs = config.timeout_secs.unwrap_or(30);
        builder = builder
            .timeout(Duration::from_secs(timeout_secs))
            .connect_timeout(Duration::from_secs(10))
            .pool_max_idle_per_host(2)
            .pool_idle_timeout(Duration::from_secs(90));

        if config.tls_shuffle_ciphers {
            let tls_config = crate::anti_blocking::build_shuffled_tls_config()
                .map_err(|e| ProviderError::Internal(format!("TLS shuffle failed: {}", e)))?;
            builder = builder.use_preconfigured_tls(tls_config);
        }

        // Configure proxy: use the first proxy from the pool. Proxy rotation
        // (round-robin or random across the full list) is handled at the
        // anti-blocking layer, not by building a new `Client` per request.
        if let Some(ref proxies) = config.proxies {
            if let Some(first_proxy) = proxies.first() {
                let proxy = reqwest::Proxy::all(first_proxy).map_err(|e| {
                    ProviderError::Internal(format!("invalid proxy URL '{}': {}", first_proxy, e))
                })?;
                builder = builder.proxy(proxy);
            }
        }

        builder
            .build()
            .map_err(|e| ProviderError::Internal(format!("failed to build HTTP client: {}", e)))
    }
}
