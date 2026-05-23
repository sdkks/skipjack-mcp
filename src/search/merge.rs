//! Result merging with URL canonicalization and deduplication.
//!
//! The [`ResultMerger`] aggregates results from multiple provider responses,
//! deduplicates by canonical URL (keeping the highest-scoring result), sorts
//! by rank score descending, and truncates to a limit.

use std::collections::HashMap;

use crate::search::{SearchResponse, SearchResult};

/// Aggregates, deduplicates by canonical URL, and ranks results from
/// multiple provider responses.
///
/// # URL canonicalization
///
/// URLs are canonicalized by:
/// - Lowercasing scheme and host (not path)
/// - Stripping the fragment
/// - Removing known tracking query parameters (utm_*, fbclid, gclid, etc.)
/// - Normalizing trailing slashes
///
/// # Deduplication
///
/// When two results canonicalize to the same URL, the one with the higher
/// `rank_score` wins.
#[derive(Debug, Clone, Default)]
pub struct ResultMerger;

impl ResultMerger {
    /// Merge multiple provider responses into a single ranked, deduplicated response.
    ///
    /// 1. Collects all results from all responses.
    /// 2. Deduplicates by URL (keeping the result with the higher rank_score).
    /// 3. Sorts by rank_score descending.
    /// 4. Takes the top `limit` results.
    pub fn merge(responses: Vec<SearchResponse>, limit: usize) -> SearchResponse {
        let mut seen: HashMap<String, SearchResult> = HashMap::new();
        let mut providers_used: Vec<String> = Vec::new();
        let mut total_found: usize = 0;
        let request_id = responses
            .first()
            .map(|r| r.request_id.clone())
            .unwrap_or_default();

        for mut response in responses {
            total_found += response.total_found;

            for name in &response.providers_used {
                if !providers_used.contains(name) {
                    providers_used.push(name.clone());
                }
            }

            for result in response.results.drain(..) {
                let canonical = Self::canonicalize_url(&result.url);
                match seen.get(&canonical) {
                    Some(existing) => {
                        if result.rank_score > existing.rank_score {
                            seen.insert(canonical, result);
                        }
                    }
                    None => {
                        seen.insert(canonical, result);
                    }
                }
            }
        }

        let mut results: Vec<SearchResult> = seen.into_values().collect();
        results.sort_by(|a, b| {
            b.rank_score
                .partial_cmp(&a.rank_score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        results.truncate(limit);

        SearchResponse {
            request_id,
            results,
            total_found,
            providers_used,
            cache_hit: false,
            elapsed_ms: 0, // Set by caller
        }
    }

    /// Produce a canonical URL for deduplication.
    ///
    /// Strips tracking parameters, fragments, trailing slashes, and lowercases
    /// the scheme and host (not path).
    fn canonicalize_url(raw_url: &str) -> String {
        // Lowercase scheme and host (not path) per FR-7.4.
        let url = match url::Url::parse(raw_url) {
            Ok(mut parsed) => {
                let scheme = parsed.scheme().to_lowercase();
                let host = parsed.host_str().unwrap_or("").to_lowercase();
                // Only set_scheme + set_host when they differ.
                let _ = parsed.set_scheme(&scheme);
                let _ = parsed.set_host(Some(&host));
                parsed.as_str().to_string()
            }
            Err(_) => raw_url.to_string(),
        };

        // Strip fragment.
        let without_fragment = url.split('#').next().unwrap_or(&url);

        // Strip known tracking query parameters.
        let tracking_params = [
            "utm_source",
            "utm_medium",
            "utm_campaign",
            "utm_term",
            "utm_content",
            "fbclid",
            "gclid",
            "ref",
            "source",
            "mc_cid",
            "mc_eid",
        ];

        let mut canonical = without_fragment.to_string();

        // Find the query string start.
        if let Some(qm_pos) = canonical.find('?') {
            let base = &canonical[..qm_pos];
            let qs = &canonical[qm_pos + 1..];

            let filtered: Vec<&str> = qs
                .split('&')
                .filter(|pair| {
                    let key = pair.split('=').next().unwrap_or("");
                    !tracking_params.contains(&key)
                })
                .collect();

            if filtered.is_empty() {
                canonical = base.to_string();
            } else {
                canonical = format!("{}?{}", base, filtered.join("&"));
            }
        }

        // Remove trailing slash (but not for root path "/").
        if let Some(stripped) = canonical.strip_suffix('/') {
            if stripped.ends_with(':') || stripped.ends_with('/') {
                // path is "/" or "https:/", don't strip
            } else {
                canonical = stripped.to_string();
            }
        }

        canonical
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merger_deduplicates_by_url() {
        let responses = vec![
            SearchResponse {
                request_id: "req-1".into(),
                results: vec![SearchResult {
                    title: "First".into(),
                    url: "https://example.com/page".into(),
                    snippet: "Snippet 1".into(),
                    published_date: None,
                    provider_name: "a".into(),
                    rank_score: 0.8,
                }],
                total_found: 1,
                providers_used: vec!["a".into()],
                cache_hit: false,
                elapsed_ms: 10,
            },
            SearchResponse {
                request_id: "req-1".into(),
                results: vec![SearchResult {
                    title: "Second (dupe)".into(),
                    url: "https://example.com/page".into(),
                    snippet: "Snippet 2".into(),
                    published_date: None,
                    provider_name: "b".into(),
                    rank_score: 0.9,
                }],
                total_found: 1,
                providers_used: vec!["b".into()],
                cache_hit: false,
                elapsed_ms: 10,
            },
        ];

        let merged = ResultMerger::merge(responses, 10);
        assert_eq!(merged.results.len(), 1);
        // The higher-scoring result should win.
        assert_eq!(merged.results[0].title, "Second (dupe)");
        assert!((merged.results[0].rank_score - 0.9).abs() < f64::EPSILON);
    }

    #[test]
    fn merger_respects_limit() {
        let mut responses = Vec::new();
        for i in 0..5 {
            responses.push(SearchResponse {
                request_id: "req-1".into(),
                results: vec![SearchResult {
                    title: format!("Result {}", i),
                    url: format!("https://example.com/{}", i),
                    snippet: "Snippet".into(),
                    published_date: None,
                    provider_name: "p".into(),
                    rank_score: (5 - i) as f64 / 5.0,
                }],
                total_found: 1,
                providers_used: vec!["p".into()],
                cache_hit: false,
                elapsed_ms: 0,
            });
        }

        let merged = ResultMerger::merge(responses, 3);
        assert_eq!(merged.results.len(), 3);
        // Results should be sorted by rank_score descending.
        assert!((merged.results[0].rank_score - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn merger_tracking_params_are_stripped() {
        let r1 = SearchResponse {
            request_id: "req-1".into(),
            results: vec![SearchResult {
                title: "A".into(),
                url: "https://example.com?utm_source=twitter&page=1".into(),
                snippet: "s".into(),
                published_date: None,
                provider_name: "p".into(),
                rank_score: 0.8,
            }],
            total_found: 1,
            providers_used: vec!["p".into()],
            cache_hit: false,
            elapsed_ms: 0,
        };
        let r2 = SearchResponse {
            request_id: "req-1".into(),
            results: vec![SearchResult {
                title: "B".into(),
                url: "https://example.com?page=1&utm_campaign=ads".into(),
                snippet: "s".into(),
                published_date: None,
                provider_name: "q".into(),
                rank_score: 0.9,
            }],
            total_found: 1,
            providers_used: vec!["q".into()],
            cache_hit: false,
            elapsed_ms: 0,
        };

        let merged = ResultMerger::merge(vec![r1, r2], 10);
        // Both URLs should canonicalize to "https://example.com?page=1"
        assert_eq!(merged.results.len(), 1);
        assert_eq!(merged.results[0].title, "B"); // higher score wins
    }
}
