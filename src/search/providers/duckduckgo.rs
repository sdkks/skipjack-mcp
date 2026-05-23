use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use reqwest::Client;
use scraper::{Html, Selector};

use crate::anti_blocking::{RateLimiter, UserAgentPool};
use crate::search::provider::{Provider, ProviderClientConfig, ProviderError};
use crate::search::{SearchRequest, SearchResponse, SearchResult, Tag};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const DUCKDUCKGO_HTML_BASE_URL: &str = "https://html.duckduckgo.com/html/";

/// CSS selectors for DuckDuckGo HTML result pages.
const SEL_RESULT_CONTAINER: &str = ".result";
const SEL_RESULT_TITLE: &str = ".result__title a.result__a";
const SEL_RESULT_URL: &str = ".result__url";
const SEL_RESULT_SNIPPET: &str = ".result__snippet";

/// Body markers that indicate a CAPTCHA or Cloudflare challenge page was served
/// instead of search results.
const CAPTCHA_MARKERS: &[&str] = &[
    "g-recaptcha",
    "grecaptcha",
    "cf-challenge",
    "cf_captcha",
    "challenge-form",
    "/cdn-cgi/l/chk",
    "checking your browser",
];

/// Default requests-per-minute rate limit for DuckDuckGo HTML scraping.
#[cfg_attr(not(test), allow(dead_code))]
const DEFAULT_DDG_RPM: u32 = 30;

// ---------------------------------------------------------------------------
// DuckDuckGoProvider
// ---------------------------------------------------------------------------

/// DuckDuckGo search provider that scrapes the HTML results page.
///
/// Unlike API-based providers, this requires no API key. It sends a POST
/// request to `https://html.duckduckgo.com/html/` and parses the returned HTML
/// using CSS selectors.
///
/// Rate limiting is enforced via an embedded [`RateLimiter`] reference so that
/// multiple concurrent searches do not exceed the configured RPM.
///
/// # Example
///
/// ```ignore
/// use std::sync::Arc;
/// use skipjackd::anti_blocking::RateLimiter;
/// use skipjackd::search::provider::ProviderClientConfig;
/// use skipjackd::search::providers::duckduckgo::DuckDuckGoProvider;
///
/// let config = ProviderClientConfig {
///     tls_shuffle_ciphers: false,
///     ip_rotation_strategy: None,
///     ipv6_subnet: None,
///     proxies: None,
///     timeout_secs: Some(15),
/// };
/// let limiter = Arc::new(RateLimiter::new());
/// let provider = DuckDuckGoProvider::new(&config, limiter, 30, true).unwrap();
///
/// assert!(provider.is_available());
/// assert_eq!(provider.name(), "duckduckgo");
/// ```
pub struct DuckDuckGoProvider {
    /// Whether this provider is configured and enabled.
    available: bool,
    /// Pre-built HTTP client with User-Agent and timeout configuration.
    client: Client,
    /// Base URL for the HTML search endpoint.
    base_url: String,
    /// Shared rate limiter used across all providers.
    rate_limiter: Arc<RateLimiter>,
    /// Requests-per-minute cap for this provider.
    rpm: u32,
}

impl DuckDuckGoProvider {
    /// Create a new `DuckDuckGoProvider`.
    ///
    /// The HTTP client is built eagerly from the supplied `client_config`.
    /// A random User-Agent is drawn from the embedded pool for the client.
    ///
    /// # Errors
    ///
    /// Returns [`ProviderError::Internal`] if the `reqwest::Client` cannot be
    /// constructed.
    pub fn new(
        client_config: &ProviderClientConfig,
        rate_limiter: Arc<RateLimiter>,
        rpm: u32,
        available: bool,
    ) -> Result<Self, ProviderError> {
        let ua = UserAgentPool::random_ua();
        let timeout_secs = client_config.timeout_secs.unwrap_or(30);

        let mut builder = Client::builder()
            .timeout(std::time::Duration::from_secs(timeout_secs))
            .connect_timeout(std::time::Duration::from_secs(10))
            .pool_max_idle_per_host(2)
            .pool_idle_timeout(std::time::Duration::from_secs(90))
            .user_agent(ua);

        // TODO: Integrate TLS cipher shuffling when TASK-0007 findings are
        // available (custom rustls::ClientConfig with reordered cipher suites).
        if client_config.tls_shuffle_ciphers {
            builder = builder.use_rustls_tls();
        }

        let client = builder
            .build()
            .map_err(|e| ProviderError::Internal(format!("failed to build HTTP client: {}", e)))?;

        Ok(DuckDuckGoProvider {
            available,
            client,
            base_url: DUCKDUCKGO_HTML_BASE_URL.to_string(),
            rate_limiter,
            rpm,
        })
    }

    /// Scan the response body for known CAPTCHA / challenge markers.
    ///
    /// Returns the text of the first matching marker, or `None` if the body
    /// appears to be legitimate search results.
    fn detect_captcha(body: &str) -> Option<&'static str> {
        let body_lower = body.to_lowercase();
        CAPTCHA_MARKERS
            .iter()
            .find(|&&marker| body_lower.contains(&marker.to_lowercase()))
            .copied()
    }

    /// Parse DuckDuckGo HTML into canonical [`SearchResult`] entries.
    ///
    /// Each `.result` container is mapped to one `SearchResult`. Results with
    /// an empty title or URL are skipped.
    fn parse_results(html: &str) -> Result<Vec<SearchResult>, ProviderError> {
        let document = Html::parse_document(html);

        let result_sel = Selector::parse(SEL_RESULT_CONTAINER).map_err(|e| {
            ProviderError::ParseError(format!(
                "invalid result selector '{}': {:?}",
                SEL_RESULT_CONTAINER, e
            ))
        })?;
        let title_sel = Selector::parse(SEL_RESULT_TITLE).map_err(|e| {
            ProviderError::ParseError(format!(
                "invalid title selector '{}': {:?}",
                SEL_RESULT_TITLE, e
            ))
        })?;
        let url_sel = Selector::parse(SEL_RESULT_URL).map_err(|e| {
            ProviderError::ParseError(format!(
                "invalid url selector '{}': {:?}",
                SEL_RESULT_URL, e
            ))
        })?;
        let snippet_sel = Selector::parse(SEL_RESULT_SNIPPET).map_err(|e| {
            ProviderError::ParseError(format!(
                "invalid snippet selector '{}': {:?}",
                SEL_RESULT_SNIPPET, e
            ))
        })?;

        let mut results = Vec::new();

        for (i, result_elem) in document.select(&result_sel).enumerate() {
            // Title: text content of the <a> inside .result__title
            let title = result_elem
                .select(&title_sel)
                .next()
                .map(|el| el.text().collect::<Vec<_>>().join("").trim().to_string())
                .unwrap_or_default();

            // URL: prefer the href attribute on the title link; fall back to
            // the text content of .result__url.
            let url = result_elem
                .select(&title_sel)
                .next()
                .and_then(|el| el.value().attr("href"))
                .map(|href| href.to_string())
                .or_else(|| {
                    result_elem
                        .select(&url_sel)
                        .next()
                        .map(|el| el.text().collect::<Vec<_>>().join("").trim().to_string())
                })
                .unwrap_or_default();

            // Skip entries that failed to produce meaningful data.
            if title.is_empty() || url.is_empty() {
                continue;
            }

            // Snippet: text content of .result__snippet
            let snippet = result_elem
                .select(&snippet_sel)
                .next()
                .map(|el| el.text().collect::<Vec<_>>().join("").trim().to_string())
                .unwrap_or_default();

            results.push(SearchResult {
                title,
                url,
                snippet,
                published_date: None,
                provider_name: "duckduckgo".to_string(),
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
impl Provider for DuckDuckGoProvider {
    fn name(&self) -> &str {
        "duckduckgo"
    }

    fn tags(&self) -> &[Tag] {
        &[Tag::Web, Tag::Privacy]
    }

    fn description(&self) -> &str {
        "DuckDuckGo HTML search (scraping-based, no API key required)"
    }

    fn is_available(&self) -> bool {
        self.available
    }

    async fn search(&self, request: &SearchRequest) -> Result<SearchResponse, ProviderError> {
        let start = Instant::now();

        // Acquire rate-limit token before making the request.
        self.rate_limiter.acquire("duckduckgo", self.rpm).await;

        // Build POST form body.
        let params = [("q", request.query.as_str())];

        let response = self
            .client
            .post(&self.base_url)
            .form(&params)
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

        if status == reqwest::StatusCode::FORBIDDEN {
            return Err(ProviderError::AccessDenied);
        }
        if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
            return Err(ProviderError::RateLimited { retry_after: None });
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

        // Detect CAPTCHA before attempting to parse.
        if let Some(_marker) = Self::detect_captcha(&body) {
            return Err(ProviderError::CaptchaDetected {
                provider: "duckduckgo".to_string(),
            });
        }

        let results = Self::parse_results(&body)?;
        let total_found = results.len();
        let elapsed_ms = start.elapsed().as_millis() as u64;

        Ok(SearchResponse {
            request_id: request.request_id.clone(),
            results,
            total_found,
            providers_used: vec!["duckduckgo".to_string()],
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

        // TODO: Integrate TLS cipher shuffling when TASK-0007 findings are
        // available (custom rustls::ClientConfig with reordered cipher suites).
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

    /// Parse a realistic fixture and verify title, URL, and snippet extraction.
    #[test]
    fn parses_fixture_into_search_results() {
        let html = include_str!("../../../tests/fixtures/duckduckgo/search.html");
        let results = DuckDuckGoProvider::parse_results(html).expect("parse fixture HTML");

        assert_eq!(results.len(), 3, "expected 3 results from fixture");

        // First result: rust-lang.org
        assert_eq!(results[0].title, "Rust Programming Language");
        assert_eq!(results[0].url, "https://www.rust-lang.org/");
        assert!(results[0].snippet.contains("reliable and efficient"));

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
            assert_eq!(result.provider_name, "duckduckgo");
            assert!(result.rank_score > 0.0);
            assert!(result.rank_score <= 1.0);
            assert!(result.published_date.is_none());
        }

        // Rank scores should decrease with position.
        assert!(results[0].rank_score > results[1].rank_score);
        assert!(results[1].rank_score > results[2].rank_score);
    }

    /// Verify CAPTCHA detection fires on a body containing a known marker.
    #[test]
    fn detect_captcha_returns_marker_when_present() {
        let body =
            "<html><body><div class=\"g-recaptcha\" data-sitekey=\"abc123\"></div></body></html>";
        assert!(DuckDuckGoProvider::detect_captcha(body).is_some());

        // Cloudflare challenge detection
        let cf_body = "<html>Checking your browser before accessing duckduckgo.com</html>";
        assert!(DuckDuckGoProvider::detect_captcha(cf_body).is_some());

        // cf-challenge marker
        let cf2 = "<!-- cf-challenge -->Please enable JavaScript.";
        assert!(DuckDuckGoProvider::detect_captcha(cf2).is_some());
    }

    /// CAPTCHA detection should not fire on legitimate result pages.
    #[test]
    fn detect_captcha_returns_none_for_legitimate_page() {
        let html = include_str!("../../../tests/fixtures/duckduckgo/search.html");
        assert!(DuckDuckGoProvider::detect_captcha(html).is_none());
    }

    /// An empty HTML document produces zero results (no panic).
    #[test]
    fn parse_results_empty_body_yields_empty_vec() {
        let results = DuckDuckGoProvider::parse_results("<html></html>").expect("parse empty HTML");
        assert!(results.is_empty());
    }

    /// HTML with result containers but missing title text should skip them.
    #[test]
    fn parse_results_skips_entries_with_empty_title() {
        let html = r#"
<html><body>
  <div class="result">
    <div class="result__body">
      <h2 class="result__title"><a class="result__a" href="https://example.com"></a></h2>
      <span class="result__url">example.com</span>
      <a class="result__snippet">Should be skipped.</a>
    </div>
  </div>
  <div class="result">
    <div class="result__body">
      <h2 class="result__title"><a class="result__a" href="https://valid.com">Valid Title</a></h2>
      <span class="result__url">valid.com</span>
      <a class="result__snippet">Valid snippet.</a>
    </div>
  </div>
</body></html>
"#;
        let results = DuckDuckGoProvider::parse_results(html).expect("parse");
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].title, "Valid Title");
        assert_eq!(results[0].url, "https://valid.com");
    }

    /// `name()` returns the expected provider name.
    #[test]
    fn name_returns_duckduckgo() {
        let config = ProviderClientConfig {
            tls_shuffle_ciphers: false,
            ip_rotation_strategy: None,
            ipv6_subnet: None,
            proxies: None,
            timeout_secs: Some(10),
        };
        let limiter = Arc::new(RateLimiter::new());
        let provider = DuckDuckGoProvider::new(&config, limiter, DEFAULT_DDG_RPM, true)
            .expect("build provider");

        assert_eq!(provider.name(), "duckduckgo");
    }

    /// `is_available()` reflects the availability flag passed at construction.
    #[test]
    fn is_available_reflects_constructor_flag() {
        let config = ProviderClientConfig {
            tls_shuffle_ciphers: false,
            ip_rotation_strategy: None,
            ipv6_subnet: None,
            proxies: None,
            timeout_secs: Some(10),
        };
        let limiter = Arc::new(RateLimiter::new());

        let enabled = DuckDuckGoProvider::new(&config, limiter.clone(), DEFAULT_DDG_RPM, true)
            .expect("build");
        assert!(enabled.is_available());

        let disabled =
            DuckDuckGoProvider::new(&config, limiter, DEFAULT_DDG_RPM, false).expect("build");
        assert!(!disabled.is_available());
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
        let provider =
            DuckDuckGoProvider::new(&config, limiter, DEFAULT_DDG_RPM, true).expect("build");

        let tags = provider.tags();
        assert_eq!(tags.len(), 2);
        assert!(tags.contains(&Tag::Web));
        assert!(tags.contains(&Tag::Privacy));
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
            DuckDuckGoProvider::new(&config, limiter, DEFAULT_DDG_RPM, true).expect("build");

        let rt = tokio::runtime::Runtime::new().expect("create runtime");
        let client = rt
            .block_on(provider.build_client(&config))
            .expect("build_client should succeed");

        // The client should be usable — the simplest check is that it is not
        // obviously broken (build_client didn't error).
        assert!(client.get("http://example.com").build().is_ok());
    }
}
