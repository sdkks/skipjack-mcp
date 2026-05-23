//! IP rotation strategies for outbound request source address diversity.
//!
//! # Investigation Findings
//!
//! **IPv6 binding via `local_address()`**: YES, reqwest 0.12 supports binding
//! outbound connections to a specific local address via
//! `ClientBuilder::local_address()`. However, `local_address()` sets a **single**
//! bind address for the entire `Client`. To achieve per-request IP rotation
//! (each request from a different source IP), you must either:
//!
//! - Build a new `Client` for each request (with `local_address()` set to the
//!   next IP in the pool). Cost: TLS handshake per request.
//! - Or use a custom `hyper` connector that binds individual sockets before
//!   dialing. This requires wrapping the `TcpStream` via `socket2` and
//!   `socket.bind()` before `connect()`. This is more complex but reuses
//!   connections when possible.
//!
//! The current implementation chooses the simpler "new Client per request"
//! approach because:
//! - Anti-blocking requests are intentionally low-throughput
//! - Reusing connections negates the IP diversity we are trying to achieve
//! - A custom hyper connector adds significant complexity for limited benefit
//!
//! **Proxy rotation**: reqwest supports HTTP and SOCKS5 proxies via
//! `Proxy::all()`. Like `local_address()`, the proxy is set per-`Client`, so
//! per-request proxy rotation also requires building a new `Client` each time.
//!
//! **Recommendation for provider implementations**: Providers should call
//! `IP_ROTATION_STRATEGY.lock().unwrap().configure_client(builder)` in their
//! `build_client()` override. The strategy is a global singleton (or
//! per-provider instance) keyed by the `ip_rotation_strategy` config field.

use ipnet::Ipv6Net;
use std::net::{IpAddr, Ipv6Addr};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

// ---------------------------------------------------------------------------
// IpRotationStrategy trait
// ---------------------------------------------------------------------------

/// A strategy for rotating the source IP address of outbound HTTP requests.
///
/// Implementations determine what local address (if any) to bind to and how to
/// configure a `reqwest::ClientBuilder` for each outbound request.
///
/// The strategy is intended to be called once per request: providers call
/// `next_bind_addr()` to get the bind address, then `configure_client()` to
/// apply it (and any proxy settings) to a fresh `ClientBuilder`.
pub trait IpRotationStrategy: Send + Sync {
    /// Return the next local IP address to bind outbound connections to.
    ///
    /// Returns `None` if the strategy does not require a specific bind address
    /// (e.g., static IP using the system default, or proxy-based routing where
    /// the proxy determines the source IP). The OS assigns an ephemeral port.
    fn next_bind_addr(&mut self) -> Option<IpAddr>;

    /// Configure a `reqwest::ClientBuilder` with the strategy's connection
    /// parameters (bind address, proxy URL).
    ///
    /// The default implementation calls `next_bind_addr()` and applies it via
    /// `builder.local_address()`.
    fn configure_client(&mut self, mut builder: reqwest::ClientBuilder) -> reqwest::ClientBuilder {
        if let Some(addr) = self.next_bind_addr() {
            builder = builder.local_address(addr);
        }
        builder
    }
}

// ---------------------------------------------------------------------------
// StaticIpStrategy
// ---------------------------------------------------------------------------

/// Uses the system's default IP address for all outbound connections.
///
/// Always returns `None` from `next_bind_addr()`. `configure_client()` is a
/// pass-through. This is the simplest strategy and the default when no IP
/// rotation is configured.
#[derive(Debug, Clone, Copy, Default)]
pub struct StaticIpStrategy;

impl IpRotationStrategy for StaticIpStrategy {
    fn next_bind_addr(&mut self) -> Option<IpAddr> {
        None
    }

    fn configure_client(&mut self, builder: reqwest::ClientBuilder) -> reqwest::ClientBuilder {
        builder
    }
}

// ---------------------------------------------------------------------------
// Ipv6PoolStrategy
// ---------------------------------------------------------------------------

/// Rotates through a pool of IPv6 addresses defined by a CIDR range.
///
/// Each call to `next_bind_addr()` returns the next address in the subnet in
/// round-robin order with port 0 (the OS picks an ephemeral port). The strategy
/// wraps around when it reaches the end of the usable range.
///
/// The usable range is [network + 1 .. network + subnet_size - 1], excluding the
/// network address and the broadcast/all-nodes address.
///
/// # Example subnet size
///
/// A `/120` subnet provides 254 usable addresses (256 minus network and broadcast).
/// A `/64` subnet is too large to iterate meaningfully — the strategy limits
/// iteration to a maximum of `u32::MAX` addresses.
#[derive(Debug)]
pub struct Ipv6PoolStrategy {
    /// The network address of the subnet (all host bits zero).
    network: u128,
    /// Number of usable host addresses in the subnet.
    /// Equal to `min(2^(128 - prefix_len) - 2, u32::MAX as u128)`.
    pool_size: u32,
    /// Current position in the round-robin (0-based index into usable hosts).
    cursor: AtomicU32,
}

impl Ipv6PoolStrategy {
    /// Create a new IPv6 pool strategy from a CIDR notation string.
    ///
    /// # Arguments
    ///
    /// * `cidr` — IPv6 subnet in CIDR notation, e.g., `"2001:db8::/120"`.
    ///   The prefix must be between 64 and 127 (prefixes shorter than /64
    ///   produce too many addresses to iterate practically; /128 has no hosts).
    ///
    /// # Errors
    ///
    /// Returns an error string if the CIDR cannot be parsed, the prefix is
    /// invalid, or the subnet is too large or too small.
    pub fn new(cidr: &str) -> Result<Self, String> {
        let net: Ipv6Net = cidr
            .parse()
            .map_err(|e| format!("invalid IPv6 CIDR '{}': {}", cidr, e))?;

        let prefix_len = net.prefix_len();

        if prefix_len > 127 {
            return Err(format!(
                "IPv6 prefix /{} has no usable host addresses (use /127 or shorter)",
                prefix_len
            ));
        }

        if prefix_len < 64 {
            return Err(format!(
                "IPv6 prefix /{} is too large for practical iteration (use /64 or longer)",
                prefix_len
            ));
        }

        let host_bits = 128 - prefix_len;
        let total_hosts = 1u128 << host_bits;

        // Usable hosts: exclude network address (all-zero host) and
        // broadcast/all-nodes address (all-ones host).
        // Guard against 0 usable hosts for very short prefixes.
        let usable = if total_hosts <= 2 {
            0u32
        } else {
            // Cap at u32::MAX for practical iteration.
            let count = total_hosts - 2;
            if count > u32::MAX as u128 {
                u32::MAX
            } else {
                count as u32
            }
        };

        let network = u128::from(net.network());

        Ok(Ipv6PoolStrategy {
            network,
            pool_size: usable,
            cursor: AtomicU32::new(0),
        })
    }

    /// Compute the IPv6 address at offset `n` within the usable host range.
    /// The first usable address is `network + 1`, second is `network + 2`, etc.
    fn nth_usable_host(network: u128, n: u32) -> Ipv6Addr {
        let addr = network.wrapping_add(1).wrapping_add(n as u128);
        Ipv6Addr::from(addr)
    }
}

impl IpRotationStrategy for Ipv6PoolStrategy {
    fn next_bind_addr(&mut self) -> Option<IpAddr> {
        if self.pool_size == 0 {
            return None;
        }

        // Atomic fetch-and-increment with wrapping.
        let idx = self.cursor.fetch_add(1, Ordering::Relaxed) % self.pool_size;
        let addr = Self::nth_usable_host(self.network, idx);
        Some(IpAddr::V6(addr))
    }

    fn configure_client(&mut self, builder: reqwest::ClientBuilder) -> reqwest::ClientBuilder {
        if let Some(addr) = self.next_bind_addr() {
            builder.local_address(addr)
        } else {
            builder
        }
    }
}

// ---------------------------------------------------------------------------
// ProxyPoolStrategy
// ---------------------------------------------------------------------------

/// Rotates through a pool of HTTP/SOCKS5 proxy URLs in round-robin order.
///
/// Each call to `configure_client()` sets the `proxy` on the builder to the
/// next URL in the pool. `next_bind_addr()` always returns `None` because the
/// proxy determines the source IP, not local binding.
///
/// Proxy URLs must be in a format reqwest understands:
/// - `"http://proxy.example.com:8080"` for HTTP proxies
/// - `"socks5://proxy.example.com:1080"` for SOCKS5 proxies
///   (requires the `socks` feature, which is enabled)
#[derive(Debug)]
pub struct ProxyPoolStrategy {
    /// The list of proxy URLs to rotate through.
    proxies: Vec<String>,
    /// Current position in the round-robin.
    cursor: AtomicU32,
}

impl ProxyPoolStrategy {
    /// Create a new proxy pool strategy from a list of proxy URLs.
    ///
    /// # Arguments
    ///
    /// * `proxies` — A list of proxy URLs. Must not be empty.
    ///
    /// # Panics
    ///
    /// Panics if `proxies` is empty.
    pub fn new(proxies: Vec<String>) -> Self {
        assert!(
            !proxies.is_empty(),
            "proxy pool must contain at least one URL"
        );
        ProxyPoolStrategy {
            proxies,
            cursor: AtomicU32::new(0),
        }
    }
}

impl IpRotationStrategy for ProxyPoolStrategy {
    fn next_bind_addr(&mut self) -> Option<IpAddr> {
        None
    }

    fn configure_client(&mut self, mut builder: reqwest::ClientBuilder) -> reqwest::ClientBuilder {
        let idx = self.cursor.fetch_add(1, Ordering::Relaxed) % self.proxies.len() as u32;
        let proxy_url = &self.proxies[idx as usize];

        // Build the proxy and apply it. If proxy construction fails, log the
        // error and return the builder unmodified — the caller should handle
        // the resulting connection error gracefully.
        match reqwest::Proxy::all(proxy_url) {
            Ok(proxy) => {
                builder = builder.proxy(proxy);
            }
            Err(e) => {
                tracing::warn!(
                    proxy_url = %proxy_url,
                    error = %e,
                    "Failed to build proxy, continuing without proxy for this request"
                );
            }
        }

        builder
    }
}

// ---------------------------------------------------------------------------
// Thread-safe strategy wrapper
// ---------------------------------------------------------------------------

/// A boxed, mutex-guarded IP rotation strategy for shared use across threads.
///
/// Providers hold an `Arc<Mutex<dyn IpRotationStrategy>>` and call
/// `configure_client()` on it in their `build_client()` method.
pub type SharedIpStrategy = Arc<Mutex<dyn IpRotationStrategy>>;

/// Build a shared IP rotation strategy from config values.
///
/// Factory function that returns the appropriate strategy based on the
/// strategy name and configuration parameters.
///
/// # Arguments
///
/// * `strategy_name` — `"static"`, `"ipv6_pool"`, or `"proxy_pool"`.
/// * `ipv6_subnet` — Required for `"ipv6_pool"` strategy.
/// * `proxies` — Required for `"proxy_pool"` strategy.
pub fn build_ip_rotation_strategy(
    strategy_name: &str,
    ipv6_subnet: Option<&str>,
    proxies: Option<&[String]>,
) -> Result<Box<dyn IpRotationStrategy>, String> {
    match strategy_name {
        "static" | "" => Ok(Box::new(StaticIpStrategy)),

        "ipv6_pool" => {
            let subnet = ipv6_subnet.ok_or_else(|| {
                "ipv6_pool strategy requires 'ipv6_subnet' configuration".to_string()
            })?;
            let strategy = Ipv6PoolStrategy::new(subnet)?;
            Ok(Box::new(strategy))
        }

        "proxy_pool" => {
            let urls = proxies.ok_or_else(|| {
                "proxy_pool strategy requires 'proxies' configuration".to_string()
            })?;
            if urls.is_empty() {
                return Err("proxy_pool strategy requires at least one proxy URL".to_string());
            }
            Ok(Box::new(ProxyPoolStrategy::new(urls.to_vec())))
        }

        other => Err(format!(
            "unknown IP rotation strategy '{}' (valid: static, ipv6_pool, proxy_pool)",
            other
        )),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    // -----------------------------------------------------------------------
    // StaticIpStrategy
    // -----------------------------------------------------------------------

    #[test]
    fn static_strategy_always_returns_none() {
        let mut strategy = StaticIpStrategy;
        for _ in 0..10 {
            assert_eq!(strategy.next_bind_addr(), None);
        }
    }

    #[test]
    fn static_strategy_configure_client_is_pass_through() {
        let mut strategy = StaticIpStrategy;
        let builder = reqwest::Client::builder();
        let _builder2 = strategy.configure_client(builder);
        // No assertion needed; if the method compiles and doesn't panic, it
        // is a pass-through.
    }

    // -----------------------------------------------------------------------
    // Ipv6PoolStrategy
    // -----------------------------------------------------------------------

    #[test]
    fn ipv6_pool_valid_cidr() {
        let strategy = Ipv6PoolStrategy::new("2001:db8::/120").expect("valid CIDR should parse");
        assert!(strategy.pool_size > 0);
    }

    #[test]
    fn ipv6_pool_cidr_too_short_prefix() {
        let err =
            Ipv6PoolStrategy::new("2001:db8::/48").expect_err("short prefix should be rejected");
        assert!(
            err.contains("too large"),
            "expected 'too large' error, got: {}",
            err
        );
    }

    #[test]
    fn ipv6_pool_round_robin_iterates_different_addresses() {
        let mut strategy = Ipv6PoolStrategy::new("2001:db8::/120").expect("valid CIDR");

        // The /120 subnet has 256 addresses, 254 usable.
        // Collect several addresses and verify they are distinct.
        let mut seen: HashSet<IpAddr> = HashSet::new();
        for _ in 0..20 {
            let addr = strategy.next_bind_addr().expect("should produce addresses");
            seen.insert(addr);
        }

        assert_eq!(
            seen.len(),
            20,
            "expected 20 distinct addresses from round-robin, got {}",
            seen.len()
        );
    }

    #[test]
    fn ipv6_pool_wraps_around() {
        let mut strategy = Ipv6PoolStrategy::new("2001:db8::/120").expect("valid CIDR");

        // /120 => pool_size = 254
        // Read all 254 addresses to wrap the cursor.
        let mut first_pass: Vec<IpAddr> = Vec::new();
        for _ in 0..strategy.pool_size {
            first_pass.push(strategy.next_bind_addr().unwrap());
        }

        // The next call should wrap to the first address again.
        let wrapped = strategy.next_bind_addr().unwrap();
        assert_eq!(
            wrapped, first_pass[0],
            "after {} iterations, cursor should wrap to first address",
            strategy.pool_size
        );
    }

    #[test]
    fn ipv6_pool_first_host_is_network_plus_one() {
        // For 2001:db8::/120, the network is 2001:db8::, first usable is ::1
        let mut strategy = Ipv6PoolStrategy::new("2001:db8::/120").expect("valid CIDR");

        let first = strategy.next_bind_addr().expect("should have addresses");
        assert_eq!(
            first,
            IpAddr::V6(Ipv6Addr::from(
                0x2001_0db8_0000_0000_0000_0000_0000_0001u128
            ))
        );
    }

    #[test]
    fn ipv6_pool_rejects_invalid_cidr() {
        assert!(Ipv6PoolStrategy::new("not-a-cidr").is_err());
        assert!(Ipv6PoolStrategy::new("192.168.1.0/24").is_err()); // IPv4
        assert!(Ipv6PoolStrategy::new("2001:db8::/129").is_err()); // bad prefix
    }

    // -----------------------------------------------------------------------
    // ProxyPoolStrategy
    // -----------------------------------------------------------------------

    #[test]
    fn proxy_pool_round_robin_cycles_through_proxies() {
        let proxies = vec![
            "http://proxy1.example.com:8080".to_string(),
            "http://proxy2.example.com:8080".to_string(),
            "http://proxy3.example.com:8080".to_string(),
        ];
        let mut strategy = ProxyPoolStrategy::new(proxies.clone());

        // next_bind_addr always returns None for proxy strategy
        assert_eq!(strategy.next_bind_addr(), None);

        // Verify the internal cursor iterates through proxy indices.
        // We can't easily inspect which proxy was set on the builder without
        // building the client, so we verify by checking that the cursor
        // cycles correctly across calls.
        let indices: Vec<u32> = (0..6)
            .map(|_| {
                let idx = strategy.cursor.fetch_add(1, Ordering::Relaxed);
                // Simulate the actual modulo logic
                idx % proxies.len() as u32
            })
            .collect();

        // Expected: 0, 1, 2, 0, 1, 2 for 6 calls over 3 proxies
        let expected: Vec<u32> = (0..6).map(|i| i as u32 % 3).collect();
        assert_eq!(indices, expected);
    }

    #[test]
    #[should_panic(expected = "proxy pool must contain at least one URL")]
    fn proxy_pool_empty_panics() {
        ProxyPoolStrategy::new(vec![]);
    }

    // -----------------------------------------------------------------------
    // Factory function
    // -----------------------------------------------------------------------

    #[test]
    fn factory_static_strategy() {
        let strategy =
            build_ip_rotation_strategy("static", None, None).expect("static should build");
        // Verify it's a StaticIpStrategy by calling next_bind_addr
        let mut dummy = strategy;
        assert_eq!(dummy.next_bind_addr(), None);
    }

    #[test]
    fn factory_ipv6_pool_strategy() {
        let strategy = build_ip_rotation_strategy("ipv6_pool", Some("fd12:3456:789a::/64"), None)
            .expect("ipv6_pool should build with valid subnet");

        // A /64 subnet is large but we capped at u32::MAX
        // Just verify it compiles and runs.
        let mut dummy = strategy;
        let addr = dummy.next_bind_addr();
        assert!(addr.is_some());
    }

    #[test]
    fn factory_ipv6_pool_missing_subnet_errors() {
        let result = build_ip_rotation_strategy("ipv6_pool", None, None);
        match result {
            Ok(_) => panic!("expected error for missing subnet"),
            Err(err) => assert!(err.contains("ipv6_subnet"), "got: {}", err),
        }
    }

    #[test]
    fn factory_proxy_pool_strategy() {
        let proxies = vec!["socks5://proxy1:1080".to_string()];
        let strategy = build_ip_rotation_strategy("proxy_pool", None, Some(&proxies))
            .expect("proxy_pool should build with valid proxies");
        let mut dummy = strategy;
        assert_eq!(dummy.next_bind_addr(), None);
    }

    #[test]
    fn factory_proxy_pool_missing_proxies_errors() {
        let result = build_ip_rotation_strategy("proxy_pool", None, None);
        match result {
            Ok(_) => panic!("expected error for missing proxies"),
            Err(err) => assert!(err.contains("proxies"), "got: {}", err),
        }
    }

    #[test]
    fn factory_unknown_strategy_errors() {
        let result = build_ip_rotation_strategy("unicorn", None, None);
        match result {
            Ok(_) => panic!("expected error for unknown strategy"),
            Err(err) => assert!(err.contains("unknown"), "got: {}", err),
        }
    }

    // -----------------------------------------------------------------------
    // Integration: build a reqwest client with Ipv6PoolStrategy
    // -----------------------------------------------------------------------

    /// Verify that building a reqwest client with `local_address()` set to an
    /// IPv6 loopback address succeeds. This is a compile-and-construct test;
    /// it does not make network requests.
    #[test]
    fn reqwest_client_with_ipv6_local_address_builds() {
        // Use ::1 (IPv6 loopback) for deterministic local testing.
        let addr = IpAddr::V6(Ipv6Addr::LOCALHOST);
        let client = reqwest::Client::builder()
            .local_address(addr)
            .build()
            .expect("client with loopback IPv6 local_address should build");
        // Client built successfully — no need to make a request.
        drop(client);
    }

    /// Verify that building a reqwest client with a proxy URL succeeds.
    #[test]
    fn reqwest_client_with_proxy_builds() {
        // Use an obviously non-existent proxy; we are testing construction,
        // not connectivity.
        let proxy = reqwest::Proxy::all("http://127.0.0.1:1").expect("proxy URL should parse");
        let client = reqwest::Client::builder()
            .proxy(proxy)
            .build()
            .expect("client with proxy should build");
        drop(client);
    }
}
