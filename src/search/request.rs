use serde::{Deserialize, Serialize};

/// A single search result from a provider, normalized to the canonical schema.
///
/// All provider implementations emit results in this format. Provider-specific
/// metadata beyond these six fields is dropped during normalization (see OQ-2
/// resolution in the spec).
///
/// # Example
///
/// ```
/// use skipjackd::search::SearchResult;
///
/// let result = SearchResult {
///     title: "Rust Programming Language".into(),
///     url: "https://www.rust-lang.org/".into(),
///     snippet: "A language empowering everyone to build reliable and efficient software.".into(),
///     published_date: None,
///     provider_name: "duckduckgo".into(),
///     rank_score: 0.95,
/// };
///
/// assert_eq!(result.title, "Rust Programming Language");
/// assert!(result.published_date.is_none());
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchResult {
    /// Title of the search result (from the provider or extracted from the page).
    pub title: String,
    /// Canonical URL of the result.
    pub url: String,
    /// Text snippet summarizing or excerpting the result.
    pub snippet: String,
    /// Optional publication date in ISO 8601 format (YYYY-MM-DD), if the provider supplies one.
    pub published_date: Option<String>,
    /// Name of the provider that returned this result.
    pub provider_name: String,
    /// Provider-assigned relevance score, normalized to 0.0–1.0 (higher is better).
    pub rank_score: f64,
}

/// Search parameters, as received from MCP or CLI.
///
/// # Example
///
/// ```
/// use skipjackd::search::{SearchRequest, Freshness};
///
/// let request = SearchRequest {
///     request_id: "550e8400-e29b-41d4-a716-446655440000".into(),
///     query: "rust async trait".into(),
///     limit: 10,
///     providers: vec!["duckduckgo".into(), "brave".into()],
///     language: Some("en".into()),
///     country: Some("us".into()),
///     safe_search: true,
///     freshness: Some(Freshness::Month),
///     dispatch_mode: None,
/// };
///
/// assert_eq!(request.query, "rust async trait");
/// assert!(request.safe_search);
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchRequest {
    /// Unique request identifier (UUIDv4).
    pub request_id: String,
    /// The search query string.
    pub query: String,
    /// Maximum number of results to return after deduplication and ranking.
    pub limit: usize,
    /// Explicit list of provider names to use. An empty vector means auto-tier
    /// dispatch using the configured fallback ladder.
    pub providers: Vec<String>,
    /// ISO 639-1 language code (e.g., "en", "fr").
    pub language: Option<String>,
    /// ISO 3166-1 alpha-2 country code (e.g., "us", "gb").
    pub country: Option<String>,
    /// Whether to enable safe search filtering.
    pub safe_search: bool,
    /// Optional time-based filter for result freshness.
    pub freshness: Option<Freshness>,
    /// Dispatch mode override. `None` means use the config default.
    /// Accepted values: `"concurrent"`, `"tiered"` (see DR-001).
    pub dispatch_mode: Option<String>,
}

/// Time-based freshness filter for search results.
///
/// Maps to provider-specific date-range parameters during request construction.
///
/// # Example
///
/// ```
/// use skipjackd::search::Freshness;
///
/// let f: Freshness = serde_json::from_str(r#""Week""#).unwrap();
/// assert!(matches!(f, Freshness::Week));
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Freshness {
    /// Results from the past 24 hours.
    Day,
    /// Results from the past 7 days.
    Week,
    /// Results from the past 30 days.
    Month,
    /// Results from the past 365 days.
    Year,
}

/// Response returned to the MCP/CLI after a completed search.
///
/// # Example
///
/// ```
/// use skipjackd::search::{SearchResponse, SearchResult};
///
/// let response = SearchResponse {
///     request_id: "550e8400-e29b-41d4-a716-446655440000".into(),
///     results: vec![SearchResult {
///         title: "Rust".into(),
///         url: "https://www.rust-lang.org/".into(),
///         snippet: "Empowering everyone...".into(),
///         published_date: None,
///         provider_name: "duckduckgo".into(),
///         rank_score: 0.92,
///     }],
///     total_found: 42,
///     providers_used: vec!["duckduckgo".into()],
///     cache_hit: false,
///     elapsed_ms: 312,
/// };
///
/// assert!(!response.cache_hit);
/// assert_eq!(response.total_found, 42);
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchResponse {
    /// Echoes the request_id from the original `SearchRequest`.
    pub request_id: String,
    /// Ranked, deduplicated results (at most `limit` entries).
    pub results: Vec<SearchResult>,
    /// Total number of results found across all providers before deduplication
    /// and capping to `limit`.
    pub total_found: usize,
    /// Names of providers that contributed results.
    pub providers_used: Vec<String>,
    /// Whether the response was served from the local cache.
    pub cache_hit: bool,
    /// Wall-clock elapsed time in milliseconds from receiving the request to
    /// producing the response.
    pub elapsed_ms: u64,
}
