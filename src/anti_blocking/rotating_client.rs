//! Per-N-request TLS client rotation for JA3 fingerprint diversity over time.
//!
//! The [`RotatingClient`] wraps a `reqwest::Client` behind a `RwLock` and
//! rebuilds it every `threshold` requests using a user-supplied closure.
//! This varies the TLS cipher suite ordering (and optionally the User-Agent)
//! across requests, making long-lived connections appear as different TLS
//! stacks to passive fingerprinting observers.
//!
//! # Side-effect in `client()`
//!
//! The `client()` accessor may trigger a synchronous client rebuild when the
//! counter reaches the threshold. The rebuild calls `build_shuffled_tls_config()`
//! (which loads system root certificates) and constructs a new `reqwest::Client`.
//! This adds ~20-50ms of latency once every N requests -- negligible relative
//! to the 100-2000ms of a typical search request.
//!
//! # Concurrency
//!
//! Uses `std::sync::RwLock` (not `tokio::sync::RwLock`) because the read path
//! is not async and lock hold times are short (microseconds for `Arc::clone`,
//! a write-assignment for rotation). Reads are concurrent; writes are exclusive
//! but infrequent.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::RwLock;

use reqwest::Client;

use crate::search::provider::ProviderError;

/// A self-rotating HTTP client that rebuilds itself every N requests.
///
/// The initial client is built eagerly in [`new`](RotatingClient::new) by
/// calling the supplied closure. On every subsequent call to
/// [`client`](RotatingClient::client), the internal counter is incremented.
/// When the counter reaches a multiple of `threshold`, the closure is called
/// again to produce a fresh client with new TLS parameters (and optionally a
/// new User-Agent).
///
/// # Rotation failure
///
/// If the closure returns an error during rotation, the old client is retained,
/// a warning is logged, and the counter continues counting. The next rotation
/// attempt happens after another `threshold` requests. The daemon stays
/// operational with the last-good TLS configuration.
///
/// # Disabling rotation
///
/// Pass `threshold = 0` to never rotate. The counter still increments but the
/// rotation block is skipped by the `> 0` guard.
pub struct RotatingClient {
    /// The current HTTP client, behind a read-write lock.
    client: RwLock<Client>,
    /// Monotonically increasing request counter.
    counter: AtomicUsize,
    /// Rotate every N requests. 0 means never rotate.
    threshold: usize,
    /// Closure that builds a fresh `Client`. Called at construction and
    /// periodically during rotation.
    build_client: Box<dyn Fn() -> Result<Client, ProviderError> + Send + Sync>,
}

impl RotatingClient {
    /// Create a new `RotatingClient`.
    ///
    /// The closure is called immediately to build the initial client.
    ///
    /// # Errors
    ///
    /// Returns [`ProviderError`] if the initial client build fails.
    pub fn new(
        threshold: usize,
        build_client: Box<dyn Fn() -> Result<Client, ProviderError> + Send + Sync>,
    ) -> Result<Self, ProviderError> {
        let client = build_client()?;
        Ok(Self {
            client: RwLock::new(client),
            counter: AtomicUsize::new(0),
            threshold,
            build_client,
        })
    }

    /// Return a clone of the current HTTP client.
    ///
    /// This increments the internal request counter. If the counter reaches a
    /// multiple of `threshold` (and `threshold > 0`), the client is rebuilt
    /// with a fresh TLS configuration before being returned.
    ///
    /// `reqwest::Client` is cheap to clone (it is `Arc`-backed), so this
    /// method is suitable for per-request use.
    pub fn client(&self) -> Client {
        let count = self.counter.fetch_add(1, Ordering::Relaxed) + 1;
        if self.threshold > 0 && count.is_multiple_of(self.threshold) {
            match (self.build_client)() {
                Ok(new_client) => {
                    *self.client.write().unwrap() = new_client;
                    tracing::debug!("Rotated TLS client (request {})", count);
                }
                Err(e) => {
                    tracing::warn!(
                        request_count = count,
                        error = %e,
                        "TLS client rotation failed, keeping existing client"
                    );
                }
            }
        }
        self.client.read().unwrap().clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    /// Build a minimal reqwest client for tests.
    fn make_test_client() -> Client {
        Client::builder().build().expect("build test client")
    }

    /// A `Client` returned by `new()` is usable.
    #[test]
    fn new_creates_client() {
        let build_count = Arc::new(AtomicUsize::new(0));
        let count = build_count.clone();
        let build_fn: Box<dyn Fn() -> Result<Client, ProviderError> + Send + Sync> =
            Box::new(move || {
                count.fetch_add(1, Ordering::SeqCst);
                Ok(make_test_client())
            });
        let rc = RotatingClient::new(10, build_fn).expect("create RotatingClient");
        // Initial build in new() incremented the counter.
        assert_eq!(build_count.load(Ordering::SeqCst), 1);
        // First request (count=1, threshold=10): no rotation.
        let _c = rc.client();
        assert_eq!(build_count.load(Ordering::SeqCst), 1);
    }

    /// With threshold=0, the counter increments but no rotation happens.
    #[test]
    fn no_rotation_when_threshold_zero() {
        let build_count = Arc::new(AtomicUsize::new(0));
        let count = build_count.clone();
        let build_fn: Box<dyn Fn() -> Result<Client, ProviderError> + Send + Sync> =
            Box::new(move || {
                count.fetch_add(1, Ordering::SeqCst);
                Ok(make_test_client())
            });
        let rc = RotatingClient::new(0, build_fn).expect("create RotatingClient");
        let initial_count = build_count.load(Ordering::SeqCst);
        for _ in 0..100 {
            let _c = rc.client();
        }
        // Never rebuilt — the closure was only called once (in new()).
        assert_eq!(build_count.load(Ordering::SeqCst), initial_count);
    }

    /// With threshold=5, rotation fires at request 5, 10, 15, etc.
    #[test]
    fn rotates_at_threshold() {
        let build_count = Arc::new(AtomicUsize::new(0));
        let count = build_count.clone();
        let build_fn: Box<dyn Fn() -> Result<Client, ProviderError> + Send + Sync> =
            Box::new(move || {
                count.fetch_add(1, Ordering::SeqCst);
                Ok(make_test_client())
            });
        let rc = RotatingClient::new(5, build_fn).expect("create RotatingClient");

        let initial = build_count.load(Ordering::SeqCst); // 1 (from new)

        // Requests 1–4: no rotation.
        for _ in 0..4 {
            let _c = rc.client();
        }
        assert_eq!(build_count.load(Ordering::SeqCst), initial);

        // Request 5: rotation fires.
        let _c = rc.client();
        assert_eq!(build_count.load(Ordering::SeqCst), initial + 1);

        // Requests 6–9: no rotation.
        for _ in 0..4 {
            let _c = rc.client();
        }
        assert_eq!(build_count.load(Ordering::SeqCst), initial + 1);

        // Request 10: second rotation.
        let _c = rc.client();
        assert_eq!(build_count.load(Ordering::SeqCst), initial + 2);
    }

    /// When the closure returns an error during rotation, the old client is
    /// kept and the daemon stays operational.
    #[test]
    fn keeps_old_client_on_rotation_failure() {
        let build_count = Arc::new(AtomicUsize::new(0));
        let count = build_count.clone();
        let build_fn: Box<dyn Fn() -> Result<Client, ProviderError> + Send + Sync> =
            Box::new(move || {
                let c = count.fetch_add(1, Ordering::SeqCst);
                // Fail every call after the first (the first is new()'s initial build).
                if c >= 1 {
                    return Err(ProviderError::Internal("simulated build failure".into()));
                }
                Ok(make_test_client())
            });
        let rc = RotatingClient::new(1, build_fn).expect("create RotatingClient");

        // First client() call: count=1, 1%1==0, triggers rebuild.
        // The closure was called once in new() (c=0, succeeds).
        // Now c = fetch_add returns 0, becomes 1. c >= 1? 0 >= 1 is false.
        // Hmm wait. Let me retrace:
        // new() calls build_fn: fetch_add returns 0, atomic becomes 1. c=0. 0 >= 1: false. Returns Ok.
        // client() #1: counter.fetch_add returns 0, +1 = 1. 1%1==0. Rebuild.
        //   build_fn: fetch_add returns 1, atomic becomes 2. c=1. 1 >= 1: true. Returns Err.
        // Old client is retained. read().clone() succeeds.
        let _c = rc.client();
        // Client is still accessible — no panic.
    }

    /// threshold=1 rotates on every single request.
    #[test]
    fn threshold_one_rotates_every_call() {
        let build_count = Arc::new(AtomicUsize::new(0));
        let count = build_count.clone();
        let build_fn: Box<dyn Fn() -> Result<Client, ProviderError> + Send + Sync> =
            Box::new(move || {
                count.fetch_add(1, Ordering::SeqCst);
                Ok(make_test_client())
            });
        let rc = RotatingClient::new(1, build_fn).expect("create RotatingClient");

        let initial = build_count.load(Ordering::SeqCst); // 1 (from new)

        let _c = rc.client(); // count=1, 1%1==0 -> rotation
        assert_eq!(build_count.load(Ordering::SeqCst), initial + 1);

        let _c = rc.client(); // count=2, 2%1==0 -> rotation
        assert_eq!(build_count.load(Ordering::SeqCst), initial + 2);

        let _c = rc.client(); // count=3, 3%1==0 -> rotation
        assert_eq!(build_count.load(Ordering::SeqCst), initial + 3);
    }

    /// Concurrent access from multiple threads does not deadlock.
    #[test]
    fn concurrent_access_no_deadlock() {
        let rc =
            Arc::new(RotatingClient::new(0, Box::new(|| Ok(make_test_client()))).expect("create"));
        let mut handles = vec![];
        for _ in 0..10 {
            let rc_clone = Arc::clone(&rc);
            handles.push(std::thread::spawn(move || {
                for _ in 0..100 {
                    let _c = rc_clone.client();
                }
            }));
        }
        for h in handles {
            h.join().expect("thread should not panic");
        }
    }
}
