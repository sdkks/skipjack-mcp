//! Sliding-window rate limiter for per-provider request throttling.
//!
//! Implements [FR-5.11] from the spec: a sliding-window algorithm that
//! prevents the daemon from exceeding configured RPM (requests per minute)
//! limits for each provider.
//!
//! Critical sections are kept short (a few `Instant` comparisons and
//! `VecDeque` operations), so `std::sync::Mutex` is used instead of
//! `tokio::sync::Mutex`.

use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;
use std::time::{Duration, Instant};
use tokio::time::sleep;

/// Sliding-window rate limiter keyed by a provider or resource name.
///
/// Each named bucket tracks timestamps of recent requests within a 60-second
/// window. When the count hits the RPM limit, `acquire()` sleeps until the
/// oldest entry exits the window.
///
/// # Example
///
/// ```ignore
/// use metasearchd::anti_blocking::RateLimiter;
/// use std::sync::Arc;
///
/// let limiter = Arc::new(RateLimiter::new());
///
/// // Before each outbound request:
/// limiter.acquire("duckduckgo", 30).await;
/// // ... make the request ...
/// ```
pub struct RateLimiter {
    /// Map from resource name to a deque of request timestamps, oldest first.
    windows: Mutex<HashMap<String, VecDeque<Instant>>>,
}

impl RateLimiter {
    /// Create a new, empty rate limiter.
    pub fn new() -> Self {
        RateLimiter {
            windows: Mutex::new(HashMap::new()),
        }
    }

    /// Wait until a request slot is available for the named resource.
    ///
    /// The limiter maintains a sliding 60-second window. On each call:
    /// 1. Expired entries (older than 60 s) are pruned.
    /// 2. If the remaining count is below `rpm`, the current timestamp is
    ///    pushed and the function returns immediately.
    /// 3. Otherwise, the function sleeps until the oldest entry exits the
    ///    window, then retries.
    ///
    /// # Panics
    ///
    /// Panics if `rpm` is 0 (division by zero).
    pub async fn acquire(&self, name: &str, rpm: u32) {
        assert!(rpm > 0, "rpm must be greater than 0");

        loop {
            let sleep_dur = {
                let mut windows = self.windows.lock().expect("rate limiter mutex poisoned");
                let deque = windows.entry(name.to_string()).or_default();
                let now = Instant::now();
                let window = Duration::from_secs(60);

                // Prune expired entries.
                while deque.front().map_or(false, |t| now.duration_since(*t) >= window) {
                    deque.pop_front();
                }

                if (deque.len() as u32) < rpm {
                    deque.push_back(now);
                    return;
                }

                // At capacity: compute sleep until the oldest entry expires.
                let oldest = deque.front().expect("deque must be non-empty at capacity");
                let age = now.duration_since(*oldest);
                let remaining = if age < window {
                    window - age
                } else {
                    Duration::ZERO
                };
                remaining
            };

            if sleep_dur > Duration::ZERO {
                sleep(sleep_dur).await;
            }
            // Loop back to re-check — another task may have taken the slot.
        }
    }
}

impl Default for RateLimiter {
    fn default() -> Self {
        RateLimiter::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::time::Instant;

    /// `acquire` with a generous RPM should return immediately.
    #[tokio::test]
    async fn acquire_returns_immediately_below_limit() {
        let limiter = RateLimiter::new();
        let start = Instant::now();

        // 100 RPM is huge — no waiting expected.
        limiter.acquire("test_provider", 100).await;

        let elapsed = start.elapsed();
        assert!(
            elapsed < Duration::from_millis(50),
            "acquire below limit should return immediately, took {:?}",
            elapsed
        );
    }

    /// Multiple sequential acquires under the limit should all pass immediately.
    #[tokio::test]
    async fn sequential_acquires_within_limit() {
        let limiter = RateLimiter::new();
        let start = Instant::now();

        for _ in 0..5 {
            limiter.acquire("seq_provider", 60).await;
        }

        let elapsed = start.elapsed();
        assert!(
            elapsed < Duration::from_millis(100),
            "5 acquires at 60 RPM should complete instantly, took {:?}",
            elapsed
        );
    }

    /// When the RPM limit is exceeded, `acquire` should block.
    /// We pre-fill the window with timestamps 59 seconds ago so the limiter
    /// blocks for ~1 second rather than ~60 seconds.
    #[tokio::test]
    async fn blocks_when_limit_reached() {
        let limiter = RateLimiter::new();

        // Pre-fill with 3 entries from 59s ago — the oldest will expire in ~1s.
        {
            let mut windows = limiter.windows.lock().unwrap();
            let deque = windows.entry("block_test".to_string()).or_default();
            let past = Instant::now()
                .checked_sub(Duration::from_secs(59))
                .expect("instant arithmetic");
            for _ in 0..3 {
                deque.push_back(past);
            }
        }

        // With RPM=3, the next acquire must block until that oldest entry expires.
        let start = Instant::now();
        limiter.acquire("block_test", 3).await;
        let elapsed = start.elapsed();

        assert!(
            elapsed >= Duration::from_millis(900),
            "acquire at RPM limit should have blocked for at least 900ms, took {:?}",
            elapsed
        );
    }

    /// Verify that entries older than 60 seconds are pruned so that new
    /// acquires don't block indefinitely.
    #[tokio::test]
    async fn expired_entries_are_pruned() {
        let limiter = RateLimiter::new();

        // Pre-fill with timestamps that are 61 seconds old.
        {
            let mut windows = limiter.windows.lock().unwrap();
            let deque = windows.entry("prune_test".to_string()).or_default();
            let old = Instant::now() - Duration::from_secs(61);
            for _ in 0..10 {
                deque.push_back(old);
            }
        }

        // RPM=5 — the 10 old entries should be pruned, so acquire succeeds immediately.
        let start = Instant::now();
        limiter.acquire("prune_test", 5).await;
        let elapsed = start.elapsed();

        assert!(
            elapsed < Duration::from_millis(50),
            "expired entries should be pruned, acquire took {:?}",
            elapsed
        );
    }

    /// Concurrent acquires from multiple tasks should be serialized by the
    /// mutex without deadlock or data corruption.
    #[tokio::test]
    async fn concurrent_acquires_are_safe() {
        let limiter = Arc::new(RateLimiter::new());
        let mut handles = Vec::new();

        for i in 0..10 {
            let limiter = Arc::clone(&limiter);
            handles.push(tokio::spawn(async move {
                limiter.acquire("concurrent_test", 100).await;
                i
            }));
        }

        let mut indices: Vec<i32> = Vec::new();
        for handle in handles {
            indices.push(handle.await.expect("task should not panic"));
        }
        indices.sort_unstable();

        assert_eq!(indices, (0..10).collect::<Vec<i32>>());
    }

    /// `acquire` on different names should not interfere with each other.
    #[tokio::test]
    async fn independent_buckets_dont_block_each_other() {
        let limiter = RateLimiter::new();

        // Saturate provider A.
        {
            let mut windows = limiter.windows.lock().unwrap();
            let deque = windows.entry("A".to_string()).or_default();
            for _ in 0..5 {
                deque.push_back(Instant::now());
            }
        }

        // Provider B should still acquire immediately.
        let start = Instant::now();
        limiter.acquire("B", 10).await;
        let elapsed = start.elapsed();

        assert!(
            elapsed < Duration::from_millis(50),
            "independent buckets should not block each other, took {:?}",
            elapsed
        );
    }

    #[test]
    #[should_panic(expected = "rpm must be greater than 0")]
    fn rpm_zero_panics() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let limiter = RateLimiter::new();
            limiter.acquire("zero_test", 0).await;
        });
    }
}
