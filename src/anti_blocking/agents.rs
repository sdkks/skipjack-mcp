//! User-Agent pool with lazy loading and uniform random selection.
//!
//! The pool is loaded once from `ua_pool.txt` at compile time via `include_str!`.
//! At first access, blank lines are filtered out and the remaining lines are
//! stored in a static `OnceLock<Vec<&str>>`. Subsequent calls to `random_ua()`
//! pick uniformly from the cached pool.

use rand::seq::SliceRandom;
use rand::thread_rng;
use std::sync::OnceLock;

/// Returns a static reference to the parsed user-agent pool, initializing it
/// lazily on first call.
fn ua_pool() -> &'static Vec<&'static str> {
    static POOL: OnceLock<Vec<&'static str>> = OnceLock::new();
    POOL.get_or_init(|| {
        include_str!("../../ua_pool.txt")
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty())
            .collect()
    })
}

/// A pool of browser User-Agent strings.
///
/// The pool is embedded at compile time from `ua_pool.txt` and loaded lazily
/// on first use. Use [`random_ua`] to pick a uniformly random entry.
///
/// # Example
///
/// ```
/// use metasearchd::anti_blocking::UserAgentPool;
///
/// let ua = UserAgentPool::random_ua();
/// assert!(!ua.is_empty());
/// assert!(ua.contains("Mozilla"));
/// ```
#[derive(Debug, Clone, Copy, Default)]
pub struct UserAgentPool;

impl UserAgentPool {
    /// Return a uniformly random user-agent string from the embedded pool.
    ///
    /// The pool is lazy-initialized on the first call and cached for the
    /// lifetime of the process. Each call draws an independent random sample.
    pub fn random_ua() -> &'static str {
        let pool = ua_pool();
        let mut rng = thread_rng();
        pool.choose(&mut rng)
            .expect("ua_pool.txt must contain at least one user-agent string")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn pool_has_expected_minimum_size() {
        let pool = ua_pool();
        assert!(
            pool.len() >= 20,
            "expected at least 20 user-agent strings, got {}",
            pool.len()
        );
    }

    #[test]
    fn pool_entries_are_non_empty() {
        for ua in ua_pool() {
            assert!(!ua.is_empty(), "found empty user-agent string in pool");
            assert!(
                ua.contains("Mozilla"),
                "UA string does not contain 'Mozilla': {}",
                ua
            );
        }
    }

    #[test]
    fn random_ua_returns_different_values_over_multiple_calls() {
        // Collect many samples; with 20+ entries, probability of all-same is
        // vanishingly small.
        let samples: Vec<&str> = (0..100).map(|_| UserAgentPool::random_ua()).collect();
        let unique: HashSet<&str> = samples.iter().copied().collect();
        assert!(
            unique.len() > 1,
            "expected multiple distinct user agents, but all calls returned the same value"
        );
    }

    #[test]
    fn all_returned_uas_belong_to_pool() {
        let pool_set: HashSet<&str> = ua_pool().iter().copied().collect();
        for _ in 0..50 {
            let ua = UserAgentPool::random_ua();
            assert!(
                pool_set.contains(ua),
                "random_ua returned '{ua}' which is not in the pool"
            );
        }
    }
}
