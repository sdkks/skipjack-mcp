use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use reqwest::Client;
use serde::Deserialize;

use crate::anti_blocking::{RateLimiter, UserAgentPool};
use crate::search::provider::{Provider, ProviderClientConfig, ProviderError};
use crate::search::{Freshness, SearchRequest, SearchResponse, SearchResult, Tag};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Default requests-per-minute rate limit for SearXNG (self-hosted, generous).
#[cfg_attr(not(test), allow(dead_code))]
const DEFAULT_RPM: u32 = 60;

// ---------------------------------------------------------------------------
// SearXNG JSON API response types
// ---------------------------------------------------------------------------

/// Raw result entry from SearXNG's JSON API.
#[derive(Debug, Clone, Deserialize)]
struct SearxngResult {
    url: String,
    title: String,
    content: String,
    score: Option<f64>,
    #[serde(rename = "publishedDate")]
    published_date: Option<String>,
}

/// SearXNG JSON search API top-level response.
#[derive(Debug, Clone, Deserialize)]
#[allow(dead_code)]
struct SearxngResponse {
    query: String,
    number_of_results: u64,
    results: Vec<SearxngResult>,
}

// ---------------------------------------------------------------------------
// SearxngProvider
// ---------------------------------------------------------------------------

/// SearXNG search provider that connects to a self-hosted external instance.
///
/// SearXNG is a self-hosted metasearch engine with a JSON API at
/// `GET /search?q=...&format=json`. The daemon does NOT manage a SearXNG
/// subprocess -- it sends HTTP requests to a pre-configured URL.
///
/// # Configuration
///
/// The `base_url` field must point to the SearXNG instance root (e.g.,
/// `https://search.example.com`). Search requests are sent to
/// `{base_url}/search?q=...&format=json`.
///
/// No API key is needed since the instance is self-hosted.
///
/// # Example
///
/// ```ignore
/// use std::sync::Arc;
/// use metasearchd::anti_blocking::RateLimiter;
/// use metasearchd::search::provider::ProviderClientConfig;
/// use metasearchd::search::providers::searxng::SearxngProvider;
///
/// let config = ProviderClientConfig {
///     tls_shuffle_ciphers: false,
///     ip_rotation_strategy: None,
///     ipv6_subnet: None,
///     proxies: None,
///     timeout_secs: Some(15),
/// };
/// let limiter = Arc::new(RateLimiter::new());
/// let provider = SearxngProvider::new(
///     Some("https://search.example.com".to_string()),
///     &config,
///     limiter,
///     60,
/// ).unwrap();
///
/// assert!(provider.is_available());
/// assert_eq!(provider.name(), "searxng");
/// ```
pub struct SearxngProvider {
    /// Whether this provider is configured and the base_url is reachable.
    available: bool,
    /// Pre-built HTTP client with User-Agent and timeout configuration.
    client: Client,
    /// Base URL of the SearXNG instance (e.g., `https://search.example.com`).
    base_url: String,
    /// Shared rate limiter used across all providers.
    rate_limiter: Arc<RateLimiter>,
    /// Requests-per-minute cap for this provider.
    rpm: u32,
}

impl SearxngProvider {
    /// Create a new `SearxngProvider`.
    ///
    /// The provider is only available when `base_url` is `Some`. The HTTP client
    /// is built eagerly from the supplied `client_config` with a random User-Agent.
    ///
    /// # Errors
    ///
    /// Returns [`ProviderError::Internal`] if the `reqwest::Client` cannot be
    /// constructed.
    pub fn new(
        base_url: Option<String>,
        client_config: &ProviderClientConfig,
        rate_limiter: Arc<RateLimiter>,
        rpm: u32,
    ) -> Result<Self, ProviderError> {
        let available = base_url.is_some();
        // Use a placeholder when base_url is None — this URL is never called
        // because is_available() returns false, but we still need a valid struct.
        let base_url = base_url.unwrap_or_default();

        let ua = UserAgentPool::random_ua();
        let timeout_secs = client_config.timeout_secs.unwrap_or(30);

        let mut builder = Client::builder()
            .timeout(std::time::Duration::from_secs(timeout_secs))
            .connect_timeout(std::time::Duration::from_secs(10))
            .pool_max_idle_per_host(2)
            .pool_idle_timeout(std::time::Duration::from_secs(90))
            .user_agent(ua);

        if client_config.tls_shuffle_ciphers {
            builder = builder.use_rustls_tls();
        }

        let client = builder
            .build()
            .map_err(|e| ProviderError::Internal(format!("failed to build HTTP client: {}", e)))?;

        Ok(SearxngProvider {
            available,
            client,
            base_url,
            rate_limiter,
            rpm,
        })
    }

    /// Build the search URL with query parameters.
    fn build_search_url(&self, request: &SearchRequest) -> String {
        let mut url = format!("{}/search?q={}&format=json", self.base_url, urlencoding(&request.query));

        if let Some(ref lang) = request.language {
            url.push_str(&format!("&language={}", urlencoding(lang)));
        }

        if request.safe_search {
            // SearXNG: safesearch=2 (strict), 1 (moderate), 0 (off)
            url.push_str("&safesearch=2");
        }

        if let Some(ref freshness) = request.freshness {
            let time_range = match freshness {
                Freshness::Day => "day",
                Freshness::Week => "week",
                Freshness::Month => "month",
                Freshness::Year => "year",
            };
            url.push_str(&format!("&time_range={}", time_range));
        }

        url
    }

    /// Parse SearXNG JSON API response into canonical [`SearchResult`] entries.
    fn parse_results(json: &str) -> Result<Vec<SearchResult>, ProviderError> {
        let response: SearxngResponse = serde_json::from_str(json).map_err(|e| {
            ProviderError::ParseError(format!("failed to parse SearXNG JSON response: {}", e))
        })?;

        let results: Vec<SearchResult> = response
            .results
            .into_iter()
            .enumerate()
            .map(|(i, r)| SearchResult {
                title: r.title,
                url: r.url,
                snippet: r.content,
                published_date: r.published_date,
                provider_name: "searxng".to_string(),
                rank_score: r.score.unwrap_or(1.0 / ((i + 1) as f64)),
            })
            .collect();

        Ok(results)
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Percent-encode a string for a URL query parameter value.
///
/// Encodes characters that are not unreserved per RFC 3986: space, quotes,
/// angle brackets, and other special characters that could break query strings.
fn urlencoding(input: &str) -> String {
    let mut result = String::with_capacity(input.len());
    for byte in input.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                result.push(byte as char);
            }
            b' ' => result.push_str("%20"),
            _ => {
                result.push_str(&format!("%{:02X}", byte));
            }
        }
    }
    result
}

// ---------------------------------------------------------------------------
// Provider trait implementation
// ---------------------------------------------------------------------------

#[async_trait]
impl Provider for SearxngProvider {
    fn name(&self) -> &str {
        "searxng"
    }

    fn tags(&self) -> &[Tag] {
        &[Tag::Web, Tag::Privacy]
    }

    fn description(&self) -> &str {
        "SearXNG self-hosted metasearch engine (JSON API, no API key required)"
    }

    fn is_available(&self) -> bool {
        self.available
    }

    async fn search(&self, request: &SearchRequest) -> Result<SearchResponse, ProviderError> {
        let start = Instant::now();

        // Acquire rate-limit token before making the request.
        self.rate_limiter.acquire("searxng", self.rpm).await;

        let url = self.build_search_url(request);

        let response = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|e| {
                if e.is_timeout() {
                    ProviderError::Timeout {
                        elapsed_secs: start.elapsed().as_secs(),
                    }
                } else {
                    ProviderError::Internal("SearXNG HTTP request failed".to_string())
                }
            })?;

        let status = response.status();

        if status == reqwest::StatusCode::FORBIDDEN {
            return Err(ProviderError::AccessDenied);
        }
        if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
            return Err(ProviderError::RateLimited { retry_after: None });
        }
        if !status.is_success() {
            return Err(ProviderError::HttpError {
                status: status.as_u16(),
                body: format!("{}", status.canonical_reason().unwrap_or("Unknown")),
            });
        }

        let body = response.text().await.map_err(|_| {
            ProviderError::Internal("failed to read SearXNG response body".to_string())
        })?;

        if body.len() > 10 * 1024 * 1024 {
            return Err(ProviderError::ParseError("response body too large".into()));
        }

        let results = Self::parse_results(&body)?;
        let total_found = results.len();
        let elapsed_ms = start.elapsed().as_millis() as u64;

        Ok(SearchResponse {
            request_id: request.request_id.clone(),
            results,
            total_found,
            providers_used: vec!["searxng".to_string()],
            cache_hit: false,
            elapsed_ms,
        })
    }

    async fn build_client(&self, config: &ProviderClientConfig) -> Result<Client, ProviderError> {
        let timeout_secs = config.timeout_secs.unwrap_or(30);
        let ua = UserAgentPool::random_ua();

        let mut builder = Client::builder()
            .timeout(std::time::Duration::from_secs(timeout_secs))
            .connect_timeout(std::time::Duration::from_secs(10))
            .pool_max_idle_per_host(2)
            .pool_idle_timeout(std::time::Duration::from_secs(90))
            .user_agent(ua);

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

    /// Parse the SearXNG fixture and verify title, URL, snippet, and score extraction.
    #[test]
    fn parses_fixture_into_search_results() {
        let json = include_str!("../../../tests/fixtures/searxng/search.json");
        let results = SearxngProvider::parse_results(json).expect("parse fixture JSON");

        assert_eq!(results.len(), 3, "expected 3 results from fixture");

        // First result: rust-lang.org
        assert_eq!(results[0].title, "Rust Programming Language");
        assert_eq!(results[0].url, "https://www.rust-lang.org/");
        assert!(results[0].snippet.contains("reliable and efficient"));
        assert_eq!(results[0].provider_name, "searxng");

        // Second result: Wikipedia
        assert_eq!(results[1].title, "Rust (programming language) - Wikipedia");
        assert_eq!(results[1].url, "https://en.wikipedia.org/wiki/Rust_(programming_language)");
        assert!(results[1].snippet.contains("general-purpose"));

        // Third result: Rust Book
        assert_eq!(results[2].title, "The Rust Programming Language - The Rust Programming Language");
        assert_eq!(results[2].url, "https://doc.rust-lang.org/book/");
        assert!(results[2].snippet.contains("Steve Klabnik"));

        // All results should have the correct provider name.
        for result in &results {
            assert_eq!(result.provider_name, "searxng");
            assert!(result.rank_score > 0.0);
            assert!(result.rank_score <= 1.0);
        }

        // Rank scores should decrease with position (since fixture has explicit scores).
        assert!(results[0].rank_score > results[1].rank_score);
        assert!(results[1].rank_score > results[2].rank_score);
    }

    /// Verify rank_score fallback when score field is absent.
    #[test]
    fn parse_results_uses_position_based_fallback_when_score_absent() {
        let json = r#"{
            "query": "test",
            "number_of_results": 2,
            "results": [
                {"url": "https://a.com", "title": "A", "content": "snippet a"},
                {"url": "https://b.com", "title": "B", "content": "snippet b"}
            ]
        }"#;

        let results = SearxngProvider::parse_results(json).expect("parse");
        assert_eq!(results.len(), 2);
        // Position-based: 1.0, 0.5
        assert!((results[0].rank_score - 1.0).abs() < f64::EPSILON);
        assert!((results[1].rank_score - 0.5).abs() < f64::EPSILON);
    }

    /// An empty results array produces zero results (no panic).
    #[test]
    fn parse_results_empty_response_yields_empty_vec() {
        let json = r#"{"query": "empty", "number_of_results": 0, "results": []}"#;
        let results = SearxngProvider::parse_results(json).expect("parse empty JSON");
        assert!(results.is_empty());
    }

    /// Malformed JSON returns a ParseError.
    #[test]
    fn parse_results_returns_parse_error_for_malformed_json() {
        let result = SearxngProvider::parse_results("not json");
        assert!(matches!(result, Err(ProviderError::ParseError(_))));
    }

    /// `build_search_url` constructs the correct URL with query parameters.
    #[test]
    fn build_search_url_encodes_query_and_adds_format() {
        let config = ProviderClientConfig {
            tls_shuffle_ciphers: false,
            ip_rotation_strategy: None,
            ipv6_subnet: None,
            proxies: None,
            timeout_secs: Some(10),
        };
        let limiter = Arc::new(RateLimiter::new());
        let provider = SearxngProvider::new(
            Some("https://search.example.com".to_string()),
            &config,
            limiter,
            DEFAULT_RPM,
        )
        .unwrap();

        let request = SearchRequest {
            request_id: "test-id".into(),
            query: "rust async".into(),
            limit: 10,
            providers: vec![],
            language: None,
            country: None,
            safe_search: false,
            freshness: None,
            dispatch_mode: None,
        };

        let url = provider.build_search_url(&request);
        assert!(url.starts_with("https://search.example.com/search?q="));
        assert!(url.contains("rust%20async"));
        assert!(url.contains("format=json"));
        // No safesearch param when safe_search is false.
        assert!(!url.contains("safesearch"));
    }

    /// URL includes safesearch when enabled.
    #[test]
    fn build_search_url_includes_safesearch_when_enabled() {
        let config = ProviderClientConfig {
            tls_shuffle_ciphers: false,
            ip_rotation_strategy: None,
            ipv6_subnet: None,
            proxies: None,
            timeout_secs: Some(10),
        };
        let limiter = Arc::new(RateLimiter::new());
        let provider = SearxngProvider::new(
            Some("https://search.example.com".to_string()),
            &config,
            limiter,
            DEFAULT_RPM,
        )
        .unwrap();

        let request = SearchRequest {
            request_id: "test-id".into(),
            query: "test".into(),
            limit: 10,
            providers: vec![],
            language: None,
            country: None,
            safe_search: true,
            freshness: None,
            dispatch_mode: None,
        };

        let url = provider.build_search_url(&request);
        assert!(url.contains("safesearch=2"));
    }

    /// URL includes language parameter when set.
    #[test]
    fn build_search_url_includes_language_when_set() {
        let config = ProviderClientConfig {
            tls_shuffle_ciphers: false,
            ip_rotation_strategy: None,
            ipv6_subnet: None,
            proxies: None,
            timeout_secs: Some(10),
        };
        let limiter = Arc::new(RateLimiter::new());
        let provider = SearxngProvider::new(
            Some("https://search.example.com".to_string()),
            &config,
            limiter,
            DEFAULT_RPM,
        )
        .unwrap();

        let request = SearchRequest {
            request_id: "test-id".into(),
            query: "test".into(),
            limit: 10,
            providers: vec![],
            language: Some("fr".into()),
            country: None,
            safe_search: false,
            freshness: None,
            dispatch_mode: None,
        };

        let url = provider.build_search_url(&request);
        assert!(url.contains("language=fr"));
    }

    /// URL includes time_range for freshness filter.
    #[test]
    fn build_search_url_includes_time_range_for_freshness() {
        let config = ProviderClientConfig {
            tls_shuffle_ciphers: false,
            ip_rotation_strategy: None,
            ipv6_subnet: None,
            proxies: None,
            timeout_secs: Some(10),
        };
        let limiter = Arc::new(RateLimiter::new());
        let provider = SearxngProvider::new(
            Some("https://search.example.com".to_string()),
            &config,
            limiter,
            DEFAULT_RPM,
        )
        .unwrap();

        let request = SearchRequest {
            request_id: "test-id".into(),
            query: "test".into(),
            limit: 10,
            providers: vec![],
            language: None,
            country: None,
            safe_search: false,
            freshness: Some(Freshness::Month),
            dispatch_mode: None,
        };

        let url = provider.build_search_url(&request);
        assert!(url.contains("time_range=month"));
    }

    /// `name()` returns the expected provider name.
    #[test]
    fn name_returns_searxng() {
        let config = ProviderClientConfig {
            tls_shuffle_ciphers: false,
            ip_rotation_strategy: None,
            ipv6_subnet: None,
            proxies: None,
            timeout_secs: Some(10),
        };
        let limiter = Arc::new(RateLimiter::new());
        let provider = SearxngProvider::new(
            Some("https://search.example.com".to_string()),
            &config,
            limiter,
            DEFAULT_RPM,
        )
        .unwrap();

        assert_eq!(provider.name(), "searxng");
    }

    /// `is_available()` returns true only when base_url is configured.
    #[test]
    fn is_available_returns_true_when_base_url_configured() {
        let config = ProviderClientConfig {
            tls_shuffle_ciphers: false,
            ip_rotation_strategy: None,
            ipv6_subnet: None,
            proxies: None,
            timeout_secs: Some(10),
        };
        let limiter = Arc::new(RateLimiter::new());

        let configured = SearxngProvider::new(
            Some("https://search.example.com".to_string()),
            &config,
            limiter.clone(),
            DEFAULT_RPM,
        )
        .unwrap();
        assert!(configured.is_available());

        let not_configured = SearxngProvider::new(None, &config, limiter, DEFAULT_RPM).unwrap();
        assert!(!not_configured.is_available());
    }

    /// `tags()` returns Web and Privacy.
    #[test]
    fn tags_returns_web_and_privacy() {
        let config = ProviderClientConfig {
            tls_shuffle_ciphers: false,
            ip_rotation_strategy: None,
            ipv6_subnet: None,
            proxies: None,
            timeout_secs: Some(10),
        };
        let limiter = Arc::new(RateLimiter::new());
        let provider = SearxngProvider::new(
            Some("https://search.example.com".to_string()),
            &config,
            limiter,
            DEFAULT_RPM,
        )
        .unwrap();

        let tags = provider.tags();
        assert_eq!(tags.len(), 2);
        assert!(tags.contains(&Tag::Web));
        assert!(tags.contains(&Tag::Privacy));
    }

    /// `description()` returns a non-empty string.
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
        let provider = SearxngProvider::new(
            Some("https://search.example.com".to_string()),
            &config,
            limiter,
            DEFAULT_RPM,
        )
        .unwrap();

        assert!(!provider.description().is_empty());
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
        let provider = SearxngProvider::new(
            Some("https://search.example.com".to_string()),
            &config,
            limiter,
            DEFAULT_RPM,
        )
        .unwrap();

        let rt = tokio::runtime::Runtime::new().expect("create runtime");
        let client = rt
            .block_on(provider.build_client(&config))
            .expect("build_client should succeed");

        assert!(client
            .get("http://example.com")
            .build()
            .is_ok());
    }

    /// SearXNG fixture response total_found matches result count.
    #[test]
    fn fixture_response_has_correct_result_count() {
        let json = include_str!("../../../tests/fixtures/searxng/search.json");
        let response: SearxngResponse =
            serde_json::from_str(json).expect("parse fixture");
        assert_eq!(response.number_of_results, 3);
        assert_eq!(response.query, "rust programming language");
        assert_eq!(response.results.len(), 3);
    }

    /// Language parameter values containing injection characters must be percent-encoded.
    #[test]
    fn build_search_url_encodes_language_parameter() {
        let config = ProviderClientConfig {
            tls_shuffle_ciphers: false,
            ip_rotation_strategy: None,
            ipv6_subnet: None,
            proxies: None,
            timeout_secs: Some(10),
        };
        let limiter = Arc::new(RateLimiter::new());
        let provider = SearxngProvider::new(
            Some("https://search.example.com".to_string()),
            &config,
            limiter,
            DEFAULT_RPM,
        )
        .unwrap();

        // Malicious language value attempting parameter injection.
        let request = SearchRequest {
            request_id: "test-id".into(),
            query: "test".into(),
            limit: 10,
            providers: vec![],
            language: Some("fr&safesearch=0".into()),
            country: None,
            safe_search: true,
            freshness: None,
            dispatch_mode: None,
        };

        let url = provider.build_search_url(&request);
        // The ampersand in the language value must be percent-encoded, not
        // interpreted as a query parameter separator.
        assert!(!url.contains("&safesearch=0"));
        assert!(url.contains("language=fr%26safesearch%3D0"));
        // The legitimate safesearch=2 must still be present.
        assert!(url.contains("safesearch=2"));
    }
}
