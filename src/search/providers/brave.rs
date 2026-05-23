use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use reqwest::Client;
use serde::Deserialize;

use crate::anti_blocking::RateLimiter;
use crate::search::provider::{Provider, ProviderClientConfig, ProviderError};
use crate::search::{SearchRequest, SearchResponse, SearchResult, Tag};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Default Brave Search API base URL (web search endpoint).
const BRAVE_API_BASE_URL: &str = "https://api.search.brave.com/res/v1/web/search";

/// Default requests-per-minute rate limit for Brave free tier.
#[cfg_attr(not(test), allow(dead_code))]
const DEFAULT_BRAVE_RPM: u32 = 20;

// ---------------------------------------------------------------------------
// Brave API response types (minimal — only the fields we need)
// ---------------------------------------------------------------------------

/// Top-level Brave web search API response.
#[derive(Debug, Clone, Deserialize)]
struct BraveWebResponse {
    web: BraveWebSection,
}

/// The `web` section of a Brave API response.
#[derive(Debug, Clone, Deserialize)]
struct BraveWebSection {
    results: Vec<BraveWebResult>,
}

/// A single web result from the Brave Search API.
#[derive(Debug, Clone, Deserialize)]
struct BraveWebResult {
    title: String,
    url: String,
    description: String,
    /// Optional publication date in ISO 8601 format.
    #[serde(rename = "page_age")]
    page_age: Option<String>,
}

// ---------------------------------------------------------------------------
// BraveProvider
// ---------------------------------------------------------------------------

/// Brave Search API provider.
///
/// Uses the Brave Search REST API (`/res/v1/web/search`). Requires an API key
/// configured via `api_key` or `api_key_env` (typically `BRAVE_API_KEY`).
/// Requests are authenticated with the `X-Subscription-Token` header.
///
/// Rate limiting is enforced via an embedded [`RateLimiter`] reference so that
/// multiple concurrent searches do not exceed the configured RPM (default 20 RPM
/// for the free tier).
///
/// # Example
///
/// ```ignore
/// use std::sync::Arc;
/// use metasearchd::anti_blocking::RateLimiter;
/// use metasearchd::search::provider::ProviderClientConfig;
/// use metasearchd::search::providers::brave::BraveProvider;
///
/// let config = ProviderClientConfig {
///     tls_shuffle_ciphers: false,
///     ip_rotation_strategy: None,
///     ipv6_subnet: None,
///     proxies: None,
///     timeout_secs: Some(15),
/// };
/// let limiter = Arc::new(RateLimiter::new());
/// let provider = BraveProvider::new(
///     &config,
///     limiter,
///     20,
///     Some("my-api-key".into()),
/// ).unwrap();
///
/// assert!(provider.is_available());
/// assert_eq!(provider.name(), "brave");
/// ```
pub struct BraveProvider {
    /// Whether this provider is configured and has a valid API key.
    available: bool,
    /// Pre-built HTTP client with timeout configuration.
    client: Client,
    /// Base URL for the Brave web search API endpoint.
    base_url: String,
    /// API key used in the `X-Subscription-Token` header.
    api_key: Option<String>,
    /// Shared rate limiter used across all providers.
    rate_limiter: Arc<RateLimiter>,
    /// Requests-per-minute cap for this provider.
    rpm: u32,
}

impl BraveProvider {
    /// Create a new `BraveProvider`.
    ///
    /// The HTTP client is built eagerly from the supplied `client_config`.
    /// The provider is marked available only when `api_key` is `Some(_)`.
    ///
    /// # Errors
    ///
    /// Returns [`ProviderError::Internal`] if the `reqwest::Client` cannot be
    /// constructed.
    pub fn new(
        client_config: &ProviderClientConfig,
        rate_limiter: Arc<RateLimiter>,
        rpm: u32,
        api_key: Option<String>,
    ) -> Result<Self, ProviderError> {
        let timeout_secs = client_config.timeout_secs.unwrap_or(30);

        let mut builder = Client::builder()
            .timeout(std::time::Duration::from_secs(timeout_secs))
            .connect_timeout(std::time::Duration::from_secs(10))
            .pool_max_idle_per_host(2)
            .pool_idle_timeout(std::time::Duration::from_secs(90));

        // Brave is a REST API provider — TLS cipher shuffling is not typically
        // needed for API-based providers with bearer/API-key auth.
        if client_config.tls_shuffle_ciphers {
            builder = builder.use_rustls_tls();
        }

        let client = builder
            .build()
            .map_err(|e| ProviderError::Internal(format!("failed to build HTTP client: {}", e)))?;

        let available = api_key.is_some();

        Ok(BraveProvider {
            available,
            client,
            base_url: BRAVE_API_BASE_URL.to_string(),
            api_key,
            rate_limiter,
            rpm,
        })
    }

    /// Build the `safe_search` query parameter value for the Brave API.
    ///
    /// Brave uses strings: `"strict"` (images + text), `"moderate"` (images only),
    /// or `"off"`. We map the boolean `safe_search` flag to `"strict"` when `true`
    /// and `"off"` when `false`, matching the conservative default from the spec.
    fn safe_search_param(safe_search: bool) -> &'static str {
        if safe_search {
            "strict"
        } else {
            "off"
        }
    }

    /// Map the canonical [`Freshness`] enum to the Brave API `freshness` parameter.
    ///
    /// Brave uses: `pd` (past day), `pw` (past week), `pm` (past month),
    /// `py` (past year).
    fn freshness_param(freshness: &crate::search::Freshness) -> &'static str {
        match freshness {
            crate::search::Freshness::Day => "pd",
            crate::search::Freshness::Week => "pw",
            crate::search::Freshness::Month => "pm",
            crate::search::Freshness::Year => "py",
        }
    }

    /// Parse the Brave JSON response into canonical [`SearchResult`] entries.
    ///
    /// Falls back gracefully if the response body is empty or malformed.
    fn parse_results(body: &str) -> Result<Vec<SearchResult>, ProviderError> {
        if body.trim().is_empty() {
            return Ok(Vec::new());
        }

        let parsed: BraveWebResponse = serde_json::from_str(body).map_err(|e| {
            ProviderError::ParseError(format!("failed to parse Brave API response: {}", e))
        })?;

        let results = parsed
            .web
            .results
            .into_iter()
            .enumerate()
            .filter(|(_, r)| !r.title.is_empty() && !r.url.is_empty())
            .map(|(i, r)| {
                // Convert page_age (ISO 8601) to date-only for published_date.
                let published_date = r.page_age.and_then(|age| {
                    // Brave returns page_age as an ISO 8601 datetime string.
                    // Extract just the date portion (YYYY-MM-DD).
                    age.split('T').next().map(|date| date.to_string())
                });

                SearchResult {
                    title: r.title,
                    url: r.url,
                    snippet: r.description,
                    published_date,
                    provider_name: "brave".to_string(),
                    rank_score: 1.0 / ((i + 1) as f64),
                }
            })
            .collect();

        Ok(results)
    }
}

// ---------------------------------------------------------------------------
// Provider trait implementation
// ---------------------------------------------------------------------------

#[async_trait]
impl Provider for BraveProvider {
    fn name(&self) -> &str {
        "brave"
    }

    fn tags(&self) -> &[Tag] {
        &[Tag::Web]
    }

    fn description(&self) -> &str {
        "Brave Search API (API key required, free tier: 20 RPM)"
    }

    fn is_available(&self) -> bool {
        self.available
    }

    async fn search(&self, request: &SearchRequest) -> Result<SearchResponse, ProviderError> {
        let start = Instant::now();

        // Verify API key is present before making the request.
        let api_key = self
            .api_key
            .as_deref()
            .ok_or_else(|| ProviderError::NotConfigured("Brave API key not set".into()))?;

        // Acquire rate-limit token before making the request.
        self.rate_limiter.acquire("brave", self.rpm).await;

        // Build query parameters.
        let mut params: Vec<(&str, String)> = vec![("q", request.query.clone())];

        // Limit: cap at 20 (Brave API max per page).
        let count = request.limit.min(20).max(1);
        params.push(("count", count.to_string()));

        // Safe search passthrough.
        params.push((
            "safesearch",
            Self::safe_search_param(request.safe_search).to_string(),
        ));

        // Optional language parameter.
        if let Some(ref lang) = request.language {
            params.push(("search_lang", lang.clone()));
        }

        // Optional country parameter.
        if let Some(ref country) = request.country {
            params.push(("country", country.clone()));
        }

        // Optional freshness filter.
        if let Some(ref freshness) = request.freshness {
            params.push((
                "freshness",
                Self::freshness_param(freshness).to_string(),
            ));
        }

        let response = self
            .client
            .get(&self.base_url)
            .header("X-Subscription-Token", api_key)
            .header("Accept", "application/json")
            .query(&params)
            .send()
            .await
            .map_err(|e| {
                if e.is_timeout() {
                    ProviderError::Timeout {
                        elapsed_secs: start.elapsed().as_secs(),
                    }
                } else {
                    ProviderError::Internal(format!("HTTP request failed: {}", e))
                }
            })?;

        let status = response.status();

        // HTTP 401 (Unauthorized) or 403 (Forbidden) — terminal error, no retry.
        if status == reqwest::StatusCode::UNAUTHORIZED
            || status == reqwest::StatusCode::FORBIDDEN
        {
            return Err(ProviderError::AccessDenied);
        }

        // HTTP 429 (Too Many Requests) — retry with backoff.
        if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
            let retry_after = response
                .headers()
                .get("Retry-After")
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.parse::<u64>().ok());
            return Err(ProviderError::RateLimited { retry_after });
        }

        if !status.is_success() {
            return Err(ProviderError::HttpError {
                status: status.as_u16(),
                body: format!("{}", status.canonical_reason().unwrap_or("Unknown")),
            });
        }

        let body = response.text().await.map_err(|e| {
            ProviderError::Internal(format!("failed to read response body: {}", e))
        })?;

        let results = Self::parse_results(&body)?;
        let total_found = results.len();
        let elapsed_ms = start.elapsed().as_millis() as u64;

        Ok(SearchResponse {
            request_id: request.request_id.clone(),
            results,
            total_found,
            providers_used: vec!["brave".to_string()],
            cache_hit: false,
            elapsed_ms,
        })
    }

    async fn build_client(&self, config: &ProviderClientConfig) -> Result<Client, ProviderError> {
        let timeout_secs = config.timeout_secs.unwrap_or(30);

        let mut builder = Client::builder()
            .timeout(std::time::Duration::from_secs(timeout_secs))
            .connect_timeout(std::time::Duration::from_secs(10))
            .pool_max_idle_per_host(2)
            .pool_idle_timeout(std::time::Duration::from_secs(90));

        if config.tls_shuffle_ciphers {
            builder = builder.use_rustls_tls();
        }

        builder
            .build()
            .map_err(|e| ProviderError::Internal(format!("failed to build HTTP client: {}", e)))
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -----------------------------------------------------------------------
    // Fixture parsing
    // -----------------------------------------------------------------------

    /// Parse the Brave API JSON fixture and verify title, URL, and snippet extraction.
    #[test]
    fn parses_fixture_into_search_results() {
        let json = include_str!("../../../tests/fixtures/brave/search.json");
        let results = BraveProvider::parse_results(json).expect("parse fixture JSON");

        assert_eq!(results.len(), 3, "expected 3 results from fixture");

        // First result: rust-lang.org
        assert_eq!(results[0].title, "Rust Programming Language");
        assert_eq!(results[0].url, "https://www.rust-lang.org/");
        assert!(results[0].snippet.contains("reliable and efficient"));
        // published_date: fixture has no page_age field, so this should be None
        assert!(results[0].published_date.is_none());

        // Second result: Wikipedia
        assert_eq!(results[1].title, "Rust (programming language) - Wikipedia");
        assert_eq!(
            results[1].url,
            "https://en.wikipedia.org/wiki/Rust_(programming_language)"
        );
        assert!(results[1].snippet.contains("general-purpose"));

        // Third result: Rust Book
        assert_eq!(
            results[2].title,
            "The Rust Programming Language - The Rust Programming Language"
        );
        assert_eq!(results[2].url, "https://doc.rust-lang.org/book/");
        assert!(results[2].snippet.contains("Steve Klabnik"));

        // All results should have the correct provider name.
        for result in &results {
            assert_eq!(result.provider_name, "brave");
            assert!(result.rank_score > 0.0);
            assert!(result.rank_score <= 1.0);
        }

        // Rank scores should decrease with position.
        assert!(results[0].rank_score > results[1].rank_score);
        assert!(results[1].rank_score > results[2].rank_score);
    }

    /// Parsing with page_age produces a published_date.
    #[test]
    fn parses_page_age_into_published_date() {
        let json = r#"{
            "web": {
                "results": [
                    {
                        "title": "Test",
                        "url": "https://example.com",
                        "description": "A test result.",
                        "page_age": "2025-01-15T10:30:00Z"
                    }
                ]
            }
        }"#;

        let results = BraveProvider::parse_results(json).expect("parse JSON with page_age");
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].published_date.as_deref(), Some("2025-01-15"));
    }

    /// An empty body produces zero results.
    #[test]
    fn parse_results_empty_body_yields_empty_vec() {
        let results = BraveProvider::parse_results("").expect("parse empty body");
        assert!(results.is_empty());
    }

    /// Results with an empty title are filtered out.
    #[test]
    fn parse_results_skips_entries_with_empty_title() {
        let json = r#"{
            "web": {
                "results": [
                    {
                        "title": "",
                        "url": "https://example.com",
                        "description": "No title here."
                    },
                    {
                        "title": "Valid Title",
                        "url": "https://valid.com",
                        "description": "Valid snippet."
                    }
                ]
            }
        }"#;

        let results = BraveProvider::parse_results(json).expect("parse JSON with empty title");
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].title, "Valid Title");
        assert_eq!(results[0].url, "https://valid.com");
    }

    /// Results with an empty URL are filtered out.
    #[test]
    fn parse_results_skips_entries_with_empty_url() {
        let json = r#"{
            "web": {
                "results": [
                    {
                        "title": "No URL",
                        "url": "",
                        "description": "Missing URL."
                    },
                    {
                        "title": "Valid",
                        "url": "https://valid.com",
                        "description": "Has URL."
                    }
                ]
            }
        }"#;

        let results = BraveProvider::parse_results(json).expect("parse JSON with empty URL");
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].title, "Valid");
    }

    /// Invalid JSON produces a ParseError.
    #[test]
    fn parse_results_invalid_json_produces_parse_error() {
        let result = BraveProvider::parse_results("not valid json {{{");
        assert!(result.is_err());
        match result {
            Err(ProviderError::ParseError(_)) => {} // expected
            other => panic!("expected ParseError, got {:?}", other),
        }
    }

    // -----------------------------------------------------------------------
    // Parameter mapping
    // -----------------------------------------------------------------------

    /// `safe_search` flag maps correctly to Brave API values.
    #[test]
    fn safe_search_param_maps_correctly() {
        assert_eq!(BraveProvider::safe_search_param(true), "strict");
        assert_eq!(BraveProvider::safe_search_param(false), "off");
    }

    /// Freshness enum maps correctly to Brave API values.
    #[test]
    fn freshness_param_maps_correctly() {
        use crate::search::Freshness;
        assert_eq!(BraveProvider::freshness_param(&Freshness::Day), "pd");
        assert_eq!(BraveProvider::freshness_param(&Freshness::Week), "pw");
        assert_eq!(BraveProvider::freshness_param(&Freshness::Month), "pm");
        assert_eq!(BraveProvider::freshness_param(&Freshness::Year), "py");
    }

    // -----------------------------------------------------------------------
    // Trait methods
    // -----------------------------------------------------------------------

    /// `name()` returns "brave".
    #[test]
    fn name_returns_brave() {
        let config = ProviderClientConfig {
            tls_shuffle_ciphers: false,
            ip_rotation_strategy: None,
            ipv6_subnet: None,
            proxies: None,
            timeout_secs: Some(10),
        };
        let limiter = Arc::new(RateLimiter::new());
        let provider =
            BraveProvider::new(&config, limiter, DEFAULT_BRAVE_RPM, Some("key123".into()))
                .expect("build provider");

        assert_eq!(provider.name(), "brave");
    }

    /// `is_available()` is true only when an API key is configured.
    #[test]
    fn is_available_true_only_with_api_key() {
        let config = ProviderClientConfig {
            tls_shuffle_ciphers: false,
            ip_rotation_strategy: None,
            ipv6_subnet: None,
            proxies: None,
            timeout_secs: Some(10),
        };
        let limiter = Arc::new(RateLimiter::new());

        let with_key =
            BraveProvider::new(&config, limiter.clone(), DEFAULT_BRAVE_RPM, Some("key".into()))
                .expect("build");
        assert!(with_key.is_available());

        let without_key =
            BraveProvider::new(&config, limiter, DEFAULT_BRAVE_RPM, None).expect("build");
        assert!(!without_key.is_available());
    }

    /// `tags()` returns Web.
    #[test]
    fn tags_returns_web() {
        let config = ProviderClientConfig {
            tls_shuffle_ciphers: false,
            ip_rotation_strategy: None,
            ipv6_subnet: None,
            proxies: None,
            timeout_secs: Some(10),
        };
        let limiter = Arc::new(RateLimiter::new());
        let provider =
            BraveProvider::new(&config, limiter, DEFAULT_BRAVE_RPM, Some("key".into()))
                .expect("build");

        let tags = provider.tags();
        assert_eq!(tags.len(), 1);
        assert!(tags.contains(&Tag::Web));
    }

    /// `description()` returns a non-empty description string.
    #[test]
    fn description_is_non_empty() {
        let config = ProviderClientConfig {
            tls_shuffle_ciphers: false,
            ip_rotation_strategy: None,
            ipv6_subnet: None,
            proxies: None,
            timeout_secs: Some(10),
        };
        let limiter = Arc::new(RateLimiter::new());
        let provider =
            BraveProvider::new(&config, limiter, DEFAULT_BRAVE_RPM, Some("key".into()))
                .expect("build");

        assert!(!provider.description().is_empty());
        assert!(provider.description().contains("Brave"));
    }

    /// `build_client` creates a working reqwest `Client`.
    #[test]
    fn build_client_returns_configured_client() {
        let config = ProviderClientConfig {
            tls_shuffle_ciphers: false,
            ip_rotation_strategy: None,
            ipv6_subnet: None,
            proxies: None,
            timeout_secs: Some(10),
        };
        let limiter = Arc::new(RateLimiter::new());
        let provider =
            BraveProvider::new(&config, limiter, DEFAULT_BRAVE_RPM, Some("key".into()))
                .expect("build");

        let rt = tokio::runtime::Runtime::new().expect("create runtime");
        let client = rt
            .block_on(provider.build_client(&config))
            .expect("build_client should succeed");

        assert!(client.get("http://example.com").build().is_ok());
    }

    /// `search()` returns AccessDenied when no API key is configured.
    #[tokio::test]
    async fn search_without_api_key_returns_access_denied() {
        let config = ProviderClientConfig {
            tls_shuffle_ciphers: false,
            ip_rotation_strategy: None,
            ipv6_subnet: None,
            proxies: None,
            timeout_secs: Some(10),
        };
        let limiter = Arc::new(RateLimiter::new());
        let provider =
            BraveProvider::new(&config, limiter, DEFAULT_BRAVE_RPM, None).expect("build");

        let request = SearchRequest {
            request_id: "test-id-1".into(),
            query: "rust programming".into(),
            limit: 10,
            providers: vec![],
            language: None,
            country: None,
            safe_search: true,
            freshness: None,
            dispatch_mode: None,
        };

        let result = provider.search(&request).await;
        assert!(result.is_err());
        match result {
            Err(ProviderError::NotConfigured(_)) => {} // expected
            other => panic!("expected NotConfigured, got {:?}", other),
        }
    }
}
