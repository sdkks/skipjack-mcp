pub mod sqlite;

use crate::search::Freshness;
use sha2::{Digest, Sha256};

pub use sqlite::Cache;

/// Statistics about the cache for monitoring and status reporting.
///
/// The hit rate is computed as `hit_count / (hit_count + miss_count)` and is
/// `0.0` when no requests have been served yet (total requests is zero).
#[derive(Debug, Clone)]
pub struct CacheStats {
    /// Total number of entries currently in the cache.
    pub total_entries: u64,
    /// Approximate total size of cached responses in bytes (sum of JSON lengths).
    pub total_size_bytes: u64,
    /// Number of cache hits since the daemon started.
    pub hit_count: u64,
    /// Number of cache misses since the daemon started.
    pub miss_count: u64,
    /// Hit rate as a fraction between 0.0 and 1.0.
    pub hit_rate: f64,
}

/// Compute a deterministic SHA-256 cache key from the normalized search
/// parameters. The key is built from the query string, a sorted, pipe-joined
/// provider list, and an optional freshness filter.
///
/// # Example
///
/// ```
/// use skipjackd::cache::cache_key;
/// use skipjackd::search::Freshness;
///
/// let key1 = cache_key("rust async", &["duckduckgo".into(), "brave".into()], None);
/// let key2 = cache_key("rust async", &["brave".into(), "duckduckgo".into()], None);
/// assert_eq!(key1, key2); // order-independent
/// ```
pub fn cache_key(query: &str, providers: &[String], freshness: Option<&Freshness>) -> String {
    let mut hasher = Sha256::new();
    hasher.update(query.as_bytes());

    // Sort provider names so order does not affect the key.
    let mut sorted: Vec<&String> = providers.iter().collect();
    sorted.sort();
    for p in &sorted {
        hasher.update(b"|");
        hasher.update(p.as_bytes());
    }

    if let Some(f) = freshness {
        hasher.update(b"|freshness=");
        let tag = match f {
            Freshness::Day => "day",
            Freshness::Week => "week",
            Freshness::Month => "month",
            Freshness::Year => "year",
        };
        hasher.update(tag.as_bytes());
    }

    let result = hasher.finalize();
    result.iter().map(|b| format!("{:02x}", b)).collect()
}

/// Build a stable, sorted, comma-separated provider list string for storage
/// in the `provider_list` column. An empty slice produces the string `"auto"`,
/// matching the convention for tier-based dispatch.
///
/// # Example
///
/// ```
/// use skipjackd::cache::provider_list_string;
///
/// let s = provider_list_string(&["brave".into(), "duckduckgo".into()]);
/// assert_eq!(s, "brave,duckduckgo");
///
/// let auto = provider_list_string(&[]);
/// assert_eq!(auto, "auto");
/// ```
pub fn provider_list_string(providers: &[String]) -> String {
    if providers.is_empty() {
        "auto".to_string()
    } else {
        let mut sorted = providers.to_vec();
        sorted.sort();
        sorted.join(",")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cache_key_order_independent() {
        let a = cache_key("hello", &["one".into(), "two".into()], None);
        let b = cache_key("hello", &["two".into(), "one".into()], None);
        assert_eq!(a, b);
    }

    #[test]
    fn cache_key_different_queries() {
        let a = cache_key("hello", &["one".into()], None);
        let b = cache_key("world", &["one".into()], None);
        assert_ne!(a, b);
    }

    #[test]
    fn cache_key_different_providers() {
        let a = cache_key("hello", &["one".into()], None);
        let b = cache_key("hello", &["one".into(), "two".into()], None);
        assert_ne!(a, b);
    }

    #[test]
    fn cache_key_freshness_differs() {
        let day = cache_key("hello", &[], Some(&Freshness::Day));
        let week = cache_key("hello", &[], Some(&Freshness::Week));
        assert_ne!(day, week);
    }

    #[test]
    fn cache_key_with_and_without_freshness() {
        let without = cache_key("hello", &[], None);
        let with = cache_key("hello", &[], Some(&Freshness::Day));
        assert_ne!(without, with);
    }

    #[test]
    fn provider_list_empty_is_auto() {
        assert_eq!(provider_list_string(&[]), "auto");
    }

    #[test]
    fn provider_list_sorted() {
        let s = provider_list_string(&["c".into(), "a".into(), "b".into()]);
        assert_eq!(s, "a,b,c");
    }

    #[test]
    fn provider_list_single() {
        let s = provider_list_string(&["duckduckgo".into()]);
        assert_eq!(s, "duckduckgo");
    }
}
