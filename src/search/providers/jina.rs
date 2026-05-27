use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use reqwest::header::{HeaderMap, HeaderValue, ACCEPT, AUTHORIZATION, CONTENT_TYPE};
use reqwest::Client;
use serde::Deserialize;

use crate::anti_blocking::{RateLimiter, UserAgentPool};
use crate::search::provider::{Provider, ProviderClientConfig, ProviderError};
use crate::search::{SearchRequest, SearchResponse, SearchResult, Tag};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Default base URL for the Jina AI search API.
const JINA_DEFAULT_BASE_URL: &str = "https://s.jina.ai";

/// Default requests-per-minute rate limit for Jina AI (free tier: 100 RPM).
#[allow(dead_code)]
const DEFAULT_JINA_RPM: u32 = 100;

// ---------------------------------------------------------------------------
// JSON response types (internal, not re-exported)
// ---------------------------------------------------------------------------

/// Top-level Jina AI API response envelope.
#[derive(Debug, Deserialize)]
struct JinaResponse {
    /// HTTP-equivalent status code from Jina.
    code: u16,
    /// Response data payload — flat array of search results.
    data: Vec<JinaResult>,
}

/// A single search result from the Jina AI API.
#[derive(Debug, Deserialize)]
struct JinaResult {
    /// Title of the result.
    title: Option<String>,
    /// URL of the result.
    url: Option<String>,
    /// Text description or snippet.
    description: Option<String>,
    /// Optional publication date in ISO 8601 format.
    published_date: Option<String>,
}

// ---------------------------------------------------------------------------
// JinaProvider
// ---------------------------------------------------------------------------

/// Jina AI search provider that calls the `s.jina.ai` API.
///
/// This is an API-based Tier 1 provider. It requires an API key, configured
/// either directly (`api_key`) or via an environment variable (`api_key_env`).
/// The key is sent as a Bearer token in the `Authorization` header.
///
/// The provider sends a POST request to `{base_url}/` with a JSON body
/// containing the query, and parses the JSON response into canonical
/// [`SearchResult`] entries.
///
/// # Availability
///
/// `is_available()` returns `false` when no API key has been configured.
/// When the environment variable referenced by `api_key_env` is not set at
/// construction time, the provider logs a warning and marks itself unavailable.
///
/// # Rate limits
///
/// The free tier allows 100 requests per minute. The rate limiter ensures
/// the daemon does not exceed this cap. The RPM is configurable via the
/// provider config `rate_limit_rpm` field.
///
/// # Example
///
/// ```ignore
/// use std::sync::Arc;
/// use skipjackd::anti_blocking::RateLimiter;
/// use skipjackd::search::provider::ProviderClientConfig;
/// use skipjackd::search::providers::jina::JinaProvider;
///
/// let config = ProviderClientConfig {
///     tls_shuffle_ciphers: false,
///     ip_rotation_strategy: None,
///     ipv6_subnet: None,
///     proxies: None,
///     timeout_secs: Some(15),
/// };
/// let limiter = Arc::new(RateLimiter::new());
/// let provider = JinaProvider::new(
///     &config,
///     limiter,
///     DEFAULT_JINA_RPM,
///     Some("jina_abc123".to_string()),
///     Some("https://s.jina.ai".to_string()),
/// ).unwrap();
///
/// assert!(provider.is_available());
/// assert_eq!(provider.name(), "jina");
/// ```
pub struct JinaProvider {
    /// Whether this provider is configured and enabled (API key present).
    available: bool,
    /// Pre-built HTTP client with User-Agent, auth header, and timeout configuration.
    client: Client,
    /// Base URL for the Jina AI search endpoint.
    base_url: String,
    /// The API key sent in the Authorization header.
    api_key: String,
    /// Shared rate limiter used across all providers.
    rate_limiter: Arc<RateLimiter>,
    /// Requests-per-minute cap for this provider.
    rpm: u32,
}

impl JinaProvider {
    /// Create a new `JinaProvider`.
    ///
    /// The HTTP client is built eagerly from the supplied `client_config`.
    /// A random User-Agent is drawn from the embedded pool and the API key
    /// is set as a default `Authorization: Bearer <key>` header.
    ///
    /// If `api_key` is `None`, the provider is constructed but marked
    /// unavailable. It will still be registered in the catalog so users can
    /// see it in `list_providers` output, but it will be skipped during
    /// dispatch.
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
        base_url: Option<String>,
    ) -> Result<Self, ProviderError> {
        let ua = UserAgentPool::random_ua();
        let timeout_secs = client_config.timeout_secs.unwrap_or(30);
        let base_url = base_url.unwrap_or_else(|| JINA_DEFAULT_BASE_URL.to_string());

        let available = matches!(api_key.as_deref(), Some(key) if !key.is_empty());
        let api_key = api_key.unwrap_or_default();

        let mut default_headers = HeaderMap::new();
        if available {
            let auth_value = format!("Bearer {}", api_key);
            let auth_header = HeaderValue::from_str(&auth_value)
                .map_err(|e| ProviderError::Internal(format!("invalid API key: {}", e)))?;
            default_headers.insert(AUTHORIZATION, auth_header);
            default_headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        }

        let mut builder = Client::builder()
            .timeout(std::time::Duration::from_secs(timeout_secs))
            .connect_timeout(std::time::Duration::from_secs(10))
            .pool_max_idle_per_host(2)
            .pool_idle_timeout(std::time::Duration::from_secs(90))
            .user_agent(ua)
            .default_headers(default_headers);

        if client_config.tls_shuffle_ciphers {
            let tls_config = crate::anti_blocking::build_shuffled_tls_config()
                .map_err(|e| ProviderError::Internal(format!("TLS shuffle failed: {}", e)))?;
            builder = builder.use_preconfigured_tls(tls_config);
        }

        let client = builder
            .build()
            .map_err(|e| ProviderError::Internal(format!("failed to build HTTP client: {}", e)))?;

        Ok(JinaProvider {
            available,
            client,
            base_url,
            api_key,
            rate_limiter,
            rpm,
        })
    }

    /// Parse Jina AI JSON response into canonical [`SearchResult`] entries.
    ///
    /// Each item in `data.results` is mapped to one `SearchResult`. Results
    /// with an empty title or URL are skipped. The rank score decays with
    /// position: rank_score = 1.0 / (position + 1).
    ///
    /// # Errors
    ///
    /// Returns [`ProviderError::ParseError`] if the JSON cannot be deserialized
    /// into the expected structure.
    fn parse_results(json: &str) -> Result<Vec<SearchResult>, ProviderError> {
        let response: JinaResponse = serde_json::from_str(json).map_err(|e| {
            ProviderError::ParseError(format!("failed to parse Jina JSON response: {}", e))
        })?;

        if response.code != 200 {
            return Err(ProviderError::HttpError {
                status: response.code,
                body: format!("Jina API returned non-200 code: {}", response.code),
            });
        }

        let mut results = Vec::new();

        for (i, item) in response.data.into_iter().enumerate() {
            let title = item.title.unwrap_or_default().trim().to_string();
            let url = item.url.unwrap_or_default().trim().to_string();

            // Skip entries that failed to produce meaningful data.
            if title.is_empty() || url.is_empty() {
                continue;
            }

            let snippet = item.description.unwrap_or_default().trim().to_string();
            let published_date = item.published_date.filter(|d| !d.is_empty());

            results.push(SearchResult {
                title,
                url,
                snippet,
                published_date,
                provider_name: "jina".to_string(),
                rank_score: 1.0 / ((i + 1) as f64),
            });
        }

        Ok(results)
    }
}

// ---------------------------------------------------------------------------
// Provider trait implementation
// ---------------------------------------------------------------------------

#[async_trait]
impl Provider for JinaProvider {
    fn name(&self) -> &str {
        "jina"
    }

    fn tags(&self) -> &[Tag] {
        &[Tag::Web]
    }

    fn description(&self) -> &str {
        "Jina AI search (API-based, API key required, 100 RPM free tier)"
    }

    fn is_available(&self) -> bool {
        self.available
    }

    async fn search(&self, request: &SearchRequest) -> Result<SearchResponse, ProviderError> {
        let start = Instant::now();

        // Acquire rate-limit token before making the request.
        self.rate_limiter.acquire("jina", self.rpm).await;

        // Build JSON request body.
        let body = serde_json::json!({
            "q": request.query,
            "limit": request.limit,
        });

        let json_body = serde_json::to_vec(&body).map_err(|e| {
            ProviderError::Internal(format!("failed to serialize request body: {}", e))
        })?;

        let response = self
            .client
            .post(&self.base_url)
            .header(CONTENT_TYPE, "application/json")
            .header(ACCEPT, "application/json")
            .body(json_body)
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

        // HTTP 401/403: terminal error, no retry.
        if status == reqwest::StatusCode::UNAUTHORIZED || status == reqwest::StatusCode::FORBIDDEN {
            return Err(ProviderError::AccessDenied);
        }

        // HTTP 429: rate limited, respect Retry-After header.
        if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
            let retry_after = response
                .headers()
                .get("retry-after")
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.parse::<u64>().ok());
            return Err(ProviderError::RateLimited { retry_after });
        }

        if !status.is_success() {
            return Err(ProviderError::HttpError {
                status: status.as_u16(),
                body: status.canonical_reason().unwrap_or("Unknown").to_string(),
            });
        }

        let body = response
            .text()
            .await
            .map_err(|e| ProviderError::Internal(format!("failed to read response body: {}", e)))?;

        let results = Self::parse_results(&body)?;
        let total_found = results.len();
        let elapsed_ms = start.elapsed().as_millis() as u64;

        Ok(SearchResponse {
            request_id: request.request_id.clone(),
            results,
            total_found,
            providers_used: vec!["jina".to_string()],
            cache_hit: false,
            elapsed_ms,
        })
    }

    async fn build_client(&self, config: &ProviderClientConfig) -> Result<Client, ProviderError> {
        let timeout_secs = config.timeout_secs.unwrap_or(30);
        let ua = UserAgentPool::random_ua();

        let mut default_headers = HeaderMap::new();
        if !self.api_key.is_empty() {
            let auth_value = format!("Bearer {}", self.api_key);
            let auth_header = HeaderValue::from_str(&auth_value)
                .map_err(|e| ProviderError::Internal(format!("invalid API key: {}", e)))?;
            default_headers.insert(AUTHORIZATION, auth_header);
            default_headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        }

        let mut builder = Client::builder()
            .timeout(std::time::Duration::from_secs(timeout_secs))
            .connect_timeout(std::time::Duration::from_secs(10))
            .pool_max_idle_per_host(2)
            .pool_idle_timeout(std::time::Duration::from_secs(90))
            .user_agent(ua)
            .default_headers(default_headers);

        if config.tls_shuffle_ciphers {
            let tls_config = crate::anti_blocking::build_shuffled_tls_config()
                .map_err(|e| ProviderError::Internal(format!("TLS shuffle failed: {}", e)))?;
            builder = builder.use_preconfigured_tls(tls_config);
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

    /// Parse a realistic fixture and verify title, URL, snippet, and date
    /// extraction.
    #[test]
    fn parses_fixture_into_search_results() {
        let json = include_str!("../../../tests/fixtures/jina/search.json");
        let results = JinaProvider::parse_results(json).expect("parse fixture JSON");

        assert_eq!(results.len(), 3, "expected 3 results from fixture");

        // First result: rust-lang.org
        assert_eq!(results[0].title, "Rust Programming Language");
        assert_eq!(results[0].url, "https://www.rust-lang.org/");
        assert!(results[0].snippet.contains("reliable and efficient"));
        assert!(results[0].published_date.is_none());

        // Second result: Wikipedia (has published_date)
        assert_eq!(results[1].title, "Rust (programming language) - Wikipedia");
        assert_eq!(
            results[1].url,
            "https://en.wikipedia.org/wiki/Rust_(programming_language)"
        );
        assert!(results[1].snippet.contains("general-purpose"));
        assert_eq!(results[1].published_date.as_deref(), Some("2024-03-15"));

        // Third result: Rust Book
        assert_eq!(
            results[2].title,
            "The Rust Programming Language - The Rust Book"
        );
        assert_eq!(results[2].url, "https://doc.rust-lang.org/book/");
        assert!(results[2].snippet.contains("Steve Klabnik"));
        assert_eq!(results[2].published_date.as_deref(), Some("2023-12-01"));

        // All results should have the correct provider name.
        for result in &results {
            assert_eq!(result.provider_name, "jina");
            assert!(result.rank_score > 0.0);
            assert!(result.rank_score <= 1.0);
        }

        // Rank scores should decrease with position.
        assert!(results[0].rank_score > results[1].rank_score);
        assert!(results[1].rank_score > results[2].rank_score);
    }

    /// An empty JSON data object produces zero results (no panic).
    #[test]
    fn parse_results_empty_data_yields_empty_vec() {
        let json = r#"{"code": 200, "data": []}"#;
        let results = JinaProvider::parse_results(json).expect("parse empty results");
        assert!(results.is_empty());
    }

    /// Results with empty title should be skipped.
    #[test]
    fn parse_results_skips_entries_with_empty_title() {
        let json = r#"{
  "code": 200,
  "data": [
    {"title": "", "url": "https://example.com", "description": "Should be skipped.", "published_date": null},
    {"title": "Valid Title", "url": "https://valid.com", "description": "Valid snippet.", "published_date": null}
  ]
}"#;
        let results = JinaProvider::parse_results(json).expect("parse");
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].title, "Valid Title");
        assert_eq!(results[0].url, "https://valid.com");
    }

    /// Results with empty URL should be skipped.
    #[test]
    fn parse_results_skips_entries_with_empty_url() {
        let json = r#"{
  "code": 200,
  "data": [
    {"title": "No URL", "url": "", "description": "Should be skipped.", "published_date": null},
    {"title": "Has URL", "url": "https://hasurl.com", "description": "Valid.", "published_date": null}
  ]
}"#;
        let results = JinaProvider::parse_results(json).expect("parse");
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].title, "Has URL");
    }

    /// Non-200 response code returns an HttpError.
    #[test]
    fn parse_results_non_200_code_returns_error() {
        let json = r#"{"code": 500, "data": []}"#;
        let err = JinaProvider::parse_results(json).unwrap_err();
        match err {
            ProviderError::HttpError { status, .. } => {
                assert_eq!(status, 500);
            }
            other => panic!("expected HttpError, got {:?}", other),
        }
    }

    /// Malformed JSON returns a ParseError.
    #[test]
    fn parse_results_malformed_json_returns_parse_error() {
        let json = "not valid json";
        let err = JinaProvider::parse_results(json).unwrap_err();
        match err {
            ProviderError::ParseError(_) => {}
            other => panic!("expected ParseError, got {:?}", other),
        }
    }

    /// `name()` returns the expected provider name.
    #[test]
    fn name_returns_jina() {
        let config = ProviderClientConfig {
            tls_shuffle_ciphers: false,
            ip_rotation_strategy: None,
            ipv6_subnet: None,
            proxies: None,
            timeout_secs: Some(10),
        };
        let limiter = Arc::new(RateLimiter::new());
        let provider = JinaProvider::new(
            &config,
            limiter,
            DEFAULT_JINA_RPM,
            Some("jina_test_key".to_string()),
            None,
        )
        .expect("build provider");

        assert_eq!(provider.name(), "jina");
    }

    /// `is_available()` returns true when an API key is provided.
    #[test]
    fn is_available_true_when_api_key_provided() {
        let config = ProviderClientConfig {
            tls_shuffle_ciphers: false,
            ip_rotation_strategy: None,
            ipv6_subnet: None,
            proxies: None,
            timeout_secs: Some(10),
        };
        let limiter = Arc::new(RateLimiter::new());

        let with_key = JinaProvider::new(
            &config,
            limiter.clone(),
            DEFAULT_JINA_RPM,
            Some("my_api_key".to_string()),
            None,
        )
        .expect("build");
        assert!(with_key.is_available());
    }

    /// `is_available()` returns false when no API key is given.
    #[test]
    fn is_available_false_when_no_api_key() {
        let config = ProviderClientConfig {
            tls_shuffle_ciphers: false,
            ip_rotation_strategy: None,
            ipv6_subnet: None,
            proxies: None,
            timeout_secs: Some(10),
        };
        let limiter = Arc::new(RateLimiter::new());

        let without_key =
            JinaProvider::new(&config, limiter, DEFAULT_JINA_RPM, None, None).expect("build");
        assert!(!without_key.is_available());
    }

    /// `is_available()` returns false when an empty API key string is given.
    #[test]
    fn is_available_false_when_api_key_empty() {
        let config = ProviderClientConfig {
            tls_shuffle_ciphers: false,
            ip_rotation_strategy: None,
            ipv6_subnet: None,
            proxies: None,
            timeout_secs: Some(10),
        };
        let limiter = Arc::new(RateLimiter::new());

        let empty_key = JinaProvider::new(
            &config,
            limiter,
            DEFAULT_JINA_RPM,
            Some(String::new()),
            None,
        )
        .expect("build");
        assert!(!empty_key.is_available());
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
        let provider = JinaProvider::new(
            &config,
            limiter,
            DEFAULT_JINA_RPM,
            Some("jina_test_key".to_string()),
            None,
        )
        .expect("build");

        let tags = provider.tags();
        assert_eq!(tags.len(), 1);
        assert!(tags.contains(&Tag::Web));
    }

    /// `description()` returns a non-empty string.
    #[test]
    fn description_returns_non_empty() {
        let config = ProviderClientConfig {
            tls_shuffle_ciphers: false,
            ip_rotation_strategy: None,
            ipv6_subnet: None,
            proxies: None,
            timeout_secs: Some(10),
        };
        let limiter = Arc::new(RateLimiter::new());
        let provider = JinaProvider::new(
            &config,
            limiter,
            DEFAULT_JINA_RPM,
            Some("jina_test_key".to_string()),
            None,
        )
        .expect("build");

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
        let provider = JinaProvider::new(
            &config,
            limiter,
            DEFAULT_JINA_RPM,
            Some("jina_test_key".to_string()),
            None,
        )
        .expect("build");

        let rt = tokio::runtime::Runtime::new().expect("create runtime");
        let client = rt
            .block_on(provider.build_client(&config))
            .expect("build_client should succeed");

        // The client should be usable.
        assert!(client.get("http://example.com").build().is_ok());
    }
}
