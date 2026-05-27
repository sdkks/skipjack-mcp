//! Anti-blocking countermeasures: user-agent rotation, exponential backoff
//! retry, sliding-window rate limiting, TLS cipher shuffling, and IP rotation.
//!
//! These subsystems implement [FR-5.1] through [FR-5.12] of the spec.

pub mod agents;
pub mod ip_rotation;
pub mod rate_limiter;
pub mod retry;
pub mod rotating_client;
pub mod tls;

// Re-export the key public API for convenience.
pub use agents::UserAgentPool;
pub use ip_rotation::{
    build_ip_rotation_strategy, IpRotationStrategy, Ipv6PoolStrategy, ProxyPoolStrategy,
    SharedIpStrategy, StaticIpStrategy,
};
pub use rate_limiter::RateLimiter;
pub use retry::{retry_with_backoff, ClassifyRetry, RetryConfig};
pub use rotating_client::RotatingClient;
pub use tls::{build_shuffled_tls_config, can_shuffle_ciphers};
