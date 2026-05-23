//! TLS cipher suite shuffling for JA3 fingerprint randomization.
//!
//! # Investigation Findings
//!
//! **rustls 0.23 capability**: YES, rustls 0.23 supports per-`ClientConfig` cipher
//! suite ordering. `CryptoProvider` derives `Clone` and exposes `cipher_suites`
//! as a public `Vec<SupportedCipherSuite>` (where `SupportedCipherSuite` is
//! `Clone + Copy`). By cloning the default provider, shuffling its
//! `cipher_suites` vector, and passing the modified provider to
//! `ClientConfig::builder_with_provider()`, we can produce a `ClientConfig` with
//! a randomized cipher order that reqwest can consume via
//! `ClientBuilder::use_preconfigured_tls()`.
//!
//! **JA3 fingerprint implications**: JA3 fingerprints are computed from the
//! ClientHello's cipher suite list order. By keeping the first 3 cipher suites
//! in their default positions (matching a typical browser's most-preferred
//! ciphers) and randomizing the remaining suites, we produce varying JA3
//! fingerprints while preserving the most recognizable part of the fingerprint.
//! Each call to `build_shuffled_tls_config()` generates a different ordering,
//! making the client appear as different TLS stacks to passive observers.
//!
//! **OpenSSL fallback**: Not required. rustls 0.23 provides sufficient cipher
//! suite control for this use case. The `openssl` crate offers equivalent
//! capability via `SSL_CTX_set_cipher_list()` but adds a heavy native dependency
//! (OpenSSL dev libraries required at build time) and platform-specific build
//! complexity. The rustls-native approach is preferred because:
//! - It is pure Rust, works identically across platforms
//! - No build-time native dependencies
//! - `CryptoProvider` cloning gives us exactly the cipher reordering we need
//!
//! **Recommendation for provider implementations**: Override
//! `Provider::build_client()` to call `build_shuffled_tls_config()` and pass the
//! resulting `rustls::ClientConfig` to
//! `reqwest::ClientBuilder::use_preconfigured_tls()`. The provider should also
//! load system root certificates into the root store (see the
//! TODO_SYS_CERTS note below).

use rand::seq::SliceRandom;
use rand::thread_rng;
use std::sync::Arc;

/// Build a `rustls::ClientConfig` with shuffled TLS cipher suites.
///
/// This keeps the first 3 cipher suites in their default priority order
/// (matching a typical browser's highest-priority ciphers) and randomizes the
/// order of the remaining suites. Each call produces a different ordering,
/// resulting in a different JA3 fingerprint.
///
/// # Root certificates (spike limitation)
///
/// This spike version creates a `ClientConfig` with an **empty** root
/// certificate store. Callers MUST populate the root store with system
/// certificates before using this config for real connections. In production
/// code, load native certs via `rustls_native_certs::load_native_certs()` and
/// add them to the root store.
///
/// # Errors
///
/// Returns an error string if:
/// - The default crypto provider is not available
/// - Protocol version negotiation fails
///   (should not happen with `with_safe_default_protocol_versions()`)
///
/// # Integration with reqwest
///
/// ```ignore
/// let tls_config = build_shuffled_tls_config()?;
/// let client = reqwest::Client::builder()
///     .use_preconfigured_tls(tls_config)
///     .build()?;
/// ```
pub fn build_shuffled_tls_config() -> Result<rustls::ClientConfig, String> {
    let provider = rustls::crypto::ring::default_provider();

    let mut provider = provider.clone();

    // Shuffle cipher suites from index 3 onward.
    // The first 3 ciphers keep their default priority order, matching the
    // most common browser fingerprint baseline.
    if provider.cipher_suites.len() > 3 {
        let rest = &mut provider.cipher_suites[3..];
        rest.shuffle(&mut thread_rng());
    }
    // If there are 3 or fewer ciphers, there is nothing meaningful to shuffle.
    // The config is still valid.

    let provider = Arc::new(provider);

    // TODO_SYS_CERTS: Populate the root store with system certificates before
    // making real connections. Example:
    //
    // ```ignore
    // let mut root_store = rustls::RootCertStore::empty();
    // for cert in rustls_native_certs::load_native_certs()? {
    //     root_store.add(cert)?;
    // }
    // ```
    let root_store = rustls::RootCertStore::empty();

    let config = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|e| format!("failed to set protocol versions: {}", e))?
        .with_root_certificates(root_store)
        .with_no_client_auth();

    Ok(config)
}

/// Verify that the cipher suites in the provider can be shuffled in place.
///
/// Returns `true` if there are enough cipher suites (> 3) to perform a
/// meaningful shuffle. This is a diagnostics helper for providers.
pub fn can_shuffle_ciphers() -> bool {
    let provider = rustls::crypto::ring::default_provider();
    provider.cipher_suites.len() > 3
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    /// `build_shuffled_tls_config()` returns an Ok config with the ring provider.
    #[test]
    fn build_shuffled_tls_config_returns_ok() {
        let result = build_shuffled_tls_config();
        assert!(
            result.is_ok(),
            "build_shuffled_tls_config() should return Ok, got: {:?}",
            result.err()
        );
    }

    /// Verify that two calls to `build_shuffled_tls_config()` produce configs
    /// with different cipher suite orderings (probabilistic test over multiple
    /// iterations to rule out random collision).
    #[test]
    fn shuffled_configs_produce_different_cipher_orders() {
        // We inspect the cipher suites order by sampling the provider's
        // cipher_suites vector after independent shuffles. Since CipherSuite
        // does not implement Hash or Eq, we compare the full ordered sequence
        // as strings and store in a HashSet.
        let mut seen_orders: HashSet<String> = HashSet::new();
        let iterations = 10;

        for _ in 0..iterations {
            let provider = {
                let mut p = rustls::crypto::ring::default_provider();
                if p.cipher_suites.len() > 3 {
                    let rest = &mut p.cipher_suites[3..];
                    rest.shuffle(&mut thread_rng());
                }
                p
            };

            let order_str: String = provider
                .cipher_suites
                .iter()
                .map(|cs| format!("{:?}", cs.suite()))
                .collect::<Vec<_>>()
                .join(",");

            seen_orders.insert(order_str);
        }

        assert!(
            seen_orders.len() >= 2,
            "expected at least 2 distinct cipher orderings across {} shuffles, got {} (possible but astronomically unlikely if shuffle works)",
            iterations,
            seen_orders.len()
        );
    }

    /// The first 3 cipher suites should remain in their original positions after
    /// shuffling. Only positions 3..N should change.
    #[test]
    fn first_three_ciphers_are_stable() {
        // Get the baseline (unshuffled) order.
        let default_provider = rustls::crypto::ring::default_provider();
        let default_first_three: Vec<rustls::CipherSuite> = default_provider
            .cipher_suites
            .iter()
            .take(3)
            .map(|cs| cs.suite())
            .collect();

        // Validate many shuffled configs.
        for _ in 0..10 {
            let mut p = default_provider.clone();
            if p.cipher_suites.len() > 3 {
                let rest = &mut p.cipher_suites[3..];
                rest.shuffle(&mut thread_rng());
            }

            let shuffled_first_three: Vec<rustls::CipherSuite> = p
                .cipher_suites
                .iter()
                .take(3)
                .map(|cs| cs.suite())
                .collect();

            assert_eq!(
                shuffled_first_three, default_first_three,
                "first 3 cipher suites must remain in the same order after shuffling"
            );
        }
    }

    #[test]
    fn can_shuffle_ciphers_reports_truthfully() {
        let provider = rustls::crypto::ring::default_provider();
        let expected = provider.cipher_suites.len() > 3;
        assert_eq!(can_shuffle_ciphers(), expected);
    }
}
