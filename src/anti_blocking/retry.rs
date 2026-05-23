//! Exponential backoff retry with full jitter and `Retry-After` header respect.
//!
//! Implements [FR-5.9] and [FR-5.10] from the spec:
//! - Algorithm: `min(cap, base_delay * 2^attempt)` with full jitter.
//! - Never retry: `AccessDenied`, `CaptchaDetected`.
//! - Terminal: `HttpError(404)`.
//! - Retryable: `RateLimited`, `HttpError(5xx)`, `Timeout`, `Internal`.
//! - On 429 with `Retry-After`: `max(computed_backoff, retry_after_secs)`.

use crate::search::provider::ProviderError;
use rand::Rng;
use std::time::Duration;
use tokio::time::sleep;

// ---------------------------------------------------------------------------
// RetryConfig
// ---------------------------------------------------------------------------

/// Configuration for the exponential backoff retry loop.
///
/// Constructed from [`crate::config::AntiBlockingConfig`] via `From`/`Into`.
#[derive(Debug, Clone)]
pub struct RetryConfig {
    /// Base delay in seconds before the first retry.
    pub base_delay_secs: u64,
    /// Maximum number of attempts (including the initial call).
    pub max_attempts: u32,
    /// Upper cap on the computed backoff delay in seconds.
    pub cap_secs: u64,
}

impl RetryConfig {
    /// Create a new `RetryConfig`.
    pub fn new(base_delay_secs: u64, max_attempts: u32, cap_secs: u64) -> Self {
        RetryConfig {
            base_delay_secs,
            max_attempts,
            cap_secs,
        }
    }
}

impl From<&crate::config::AntiBlockingConfig> for RetryConfig {
    fn from(c: &crate::config::AntiBlockingConfig) -> Self {
        RetryConfig {
            base_delay_secs: c.retry_base_delay_secs,
            max_attempts: c.retry_max_attempts,
            cap_secs: c.retry_cap_secs,
        }
    }
}

// ---------------------------------------------------------------------------
// ClassifyRetry trait
// ---------------------------------------------------------------------------

/// Classification of an error for retry decision-making.
///
/// Implementors declare whether a given error variant is retryable and,
/// optionally, provide a server-suggested backoff duration (e.g., from
/// the `Retry-After` header on HTTP 429).
pub trait ClassifyRetry {
    /// Returns `true` if the error is transient and the operation can be retried.
    fn is_retryable(&self) -> bool;

    /// If the server provided a `Retry-After` hint (in seconds), return it.
    ///
    /// The retry loop uses `max(computed_backoff, retry_after_hint)` when
    /// this returns `Some`.
    fn retry_after_hint(&self) -> Option<u64>;
}

impl ClassifyRetry for ProviderError {
    fn is_retryable(&self) -> bool {
        match self {
            // Never retry
            ProviderError::AccessDenied => false,
            ProviderError::CaptchaDetected { .. } => false,
            // Terminal HTTP status
            ProviderError::HttpError { status: 404, .. } => false,
            // Retryable
            ProviderError::RateLimited { .. } => true,
            ProviderError::HttpError { status, .. } if *status >= 500 => true,
            ProviderError::Timeout { .. } => true,
            ProviderError::Internal(_) => true,
            // All other cases (NotConfigured, ParseError, non-5xx HttpError) are not retried
            _ => false,
        }
    }

    fn retry_after_hint(&self) -> Option<u64> {
        match self {
            ProviderError::RateLimited { retry_after } => *retry_after,
            _ => None,
        }
    }
}

// ---------------------------------------------------------------------------
// retry_with_backoff
// ---------------------------------------------------------------------------

/// Execute an async operation with exponential backoff and full jitter.
///
/// The closure `f` is called at most `config.max_attempts` times. Between
/// attempts the function sleeps for a jittered delay computed as:
///
/// ```text
/// computed = min(cap_secs, base_delay_secs * 2^attempt)
/// delay    = rand::random::<f64>() * computed
/// ```
///
/// If the error carries a `Retry-After` hint (e.g., from HTTP 429), the
/// delay is `max(delay, retry_after_hint)`. Non-retryable errors are
/// returned immediately without sleeping.
///
/// # Example
///
/// ```ignore
/// use metasearchd::anti_blocking::{retry_with_backoff, RetryConfig};
/// use metasearchd::search::ProviderError;
///
/// let config = RetryConfig::new(1, 3, 60);
/// let result = retry_with_backoff(
///     || async { /* fallible HTTP call returning Result<T, ProviderError> */ },
///     &config,
/// ).await;
/// ```
pub async fn retry_with_backoff<F, Fut, T, E>(mut f: F, config: &RetryConfig) -> Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, E>>,
    E: ClassifyRetry,
{
    if config.max_attempts == 0 {
        panic!("max_attempts must be >= 1");
    }

    let mut last_error: Option<E> = None;

    for attempt in 0..config.max_attempts {
        match f().await {
            Ok(value) => return Ok(value),
            Err(err) => {
                // If not retryable, or this was the last attempt, return immediately.
                if !err.is_retryable() {
                    return Err(err);
                }

                if attempt + 1 >= config.max_attempts {
                    return Err(err);
                }

                // Compute backoff: cap * 2^attempt, capped at cap_secs.
                let shift = 1u64.checked_shl(attempt).unwrap_or(u64::MAX);
                let computed =
                    (config.base_delay_secs as f64 * shift as f64).min(config.cap_secs as f64);

                // Full jitter: random in [0, computed].
                let jittered = rand::thread_rng().gen::<f64>() * computed;

                // Honour Retry-After if present and larger.
                let delay_secs = match err.retry_after_hint() {
                    Some(retry_after) => jittered.max(retry_after as f64),
                    None => jittered,
                };

                sleep(Duration::from_secs_f64(delay_secs)).await;
                last_error = Some(err);
            }
        }
    }

    Err(last_error.expect("retry loop must produce an error before falling through"))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// A test error type implementing `ClassifyRetry`.
    #[derive(Debug, PartialEq, Clone)]
    enum TestError {
        Transient,
        Fatal,
        WithRetryAfter(u64),
    }

    impl ClassifyRetry for TestError {
        fn is_retryable(&self) -> bool {
            matches!(self, TestError::Transient | TestError::WithRetryAfter(_))
        }

        fn retry_after_hint(&self) -> Option<u64> {
            match self {
                TestError::WithRetryAfter(secs) => Some(*secs),
                _ => None,
            }
        }
    }

    #[tokio::test]
    async fn non_retryable_error_returned_immediately() {
        let config = RetryConfig::new(1, 3, 60);
        let mut call_count = 0u32;

        let result = retry_with_backoff(
            || {
                call_count += 1;
                async { Err::<(), _>(TestError::Fatal) }
            },
            &config,
        )
        .await;

        assert_eq!(result, Err(TestError::Fatal));
        assert_eq!(call_count, 1, "fatal errors must not be retried");
    }

    #[tokio::test]
    async fn success_on_first_attempt() {
        let config = RetryConfig::new(1, 3, 60);

        let result = retry_with_backoff(|| async { Ok::<i32, TestError>(42) }, &config).await;

        assert_eq!(result, Ok(42));
    }

    #[tokio::test]
    async fn retries_up_to_max_attempts() {
        let config = RetryConfig::new(0, 3, 60); // base_delay=0 for fast test
        let mut call_count = 0u32;

        let result = retry_with_backoff(
            || {
                call_count += 1;
                async { Err::<(), _>(TestError::Transient) }
            },
            &config,
        )
        .await;

        assert_eq!(result, Err(TestError::Transient));
        assert_eq!(call_count, 3, "should have tried max_attempts times");
    }

    #[tokio::test]
    async fn succeeds_on_retry() {
        let config = RetryConfig::new(0, 3, 60);
        let mut call_count = 0u32;

        let result = retry_with_backoff(
            || {
                call_count += 1;
                async move {
                    if call_count < 3 {
                        Err(TestError::Transient)
                    } else {
                        Ok(99)
                    }
                }
            },
            &config,
        )
        .await;

        assert_eq!(result, Ok(99));
        assert_eq!(call_count, 3);
    }

    /// Verify that `ClassifyRetry` is correctly implemented for `ProviderError`
    /// according to the spec:
    /// - AccessDenied, CaptchaDetected, HttpError(404) are NOT retryable
    /// - RateLimited, HttpError(5xx), Timeout, Internal ARE retryable
    #[test]
    fn provider_error_classification() {
        // Non-retryable
        assert!(!ProviderError::AccessDenied.is_retryable());
        assert!(!ProviderError::CaptchaDetected {
            provider: "test".into()
        }
        .is_retryable());
        assert!(!ProviderError::HttpError {
            status: 404,
            body: "not found".into()
        }
        .is_retryable());
        assert!(!ProviderError::HttpError {
            status: 403,
            body: "forbidden".into()
        }
        .is_retryable());
        assert!(!ProviderError::NotConfigured("nope".into()).is_retryable());
        assert!(!ProviderError::ParseError("bad json".into()).is_retryable());

        // Retryable
        assert!(ProviderError::RateLimited { retry_after: None }.is_retryable());
        assert!(ProviderError::RateLimited {
            retry_after: Some(30)
        }
        .is_retryable());
        assert!(ProviderError::HttpError {
            status: 500,
            body: "boom".into()
        }
        .is_retryable());
        assert!(ProviderError::HttpError {
            status: 503,
            body: "unavailable".into()
        }
        .is_retryable());
        assert!(ProviderError::Timeout { elapsed_secs: 10 }.is_retryable());
        assert!(ProviderError::Internal("something broke".into()).is_retryable());
    }

    #[test]
    fn rate_limited_carries_retry_after_hint() {
        let err = ProviderError::RateLimited {
            retry_after: Some(42),
        };
        assert_eq!(err.retry_after_hint(), Some(42));

        let err = ProviderError::RateLimited { retry_after: None };
        assert_eq!(err.retry_after_hint(), None);

        let err = ProviderError::Timeout { elapsed_secs: 5 };
        assert_eq!(err.retry_after_hint(), None);
    }

    #[test]
    fn test_error_with_retry_after_is_classified_correctly() {
        let err = TestError::WithRetryAfter(30);
        assert!(err.is_retryable());
        assert_eq!(err.retry_after_hint(), Some(30));
    }

    #[tokio::test]
    #[should_panic(expected = "max_attempts")]
    async fn max_attempts_zero_panics() {
        let config = RetryConfig::new(1, 0, 60);
        let _ = retry_with_backoff(|| async { Ok::<(), TestError>(()) }, &config).await;
    }
}
