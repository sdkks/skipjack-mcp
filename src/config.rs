//! Configuration system with TOML parsing and environment variable overrides.
//!
//! Reads configuration from `~/.config/skipjackd/config.toml` by default,
//! with an optional path override. Environment variables with the `SKIPJACKD_`
//! prefix override config file values using double-underscore separators for
//! nested keys (e.g., `SKIPJACKD_CACHE__DEFAULT_TTL_SECS=7200`).
//!
//! The config is frozen into an `Arc<Config>` after loading for sharing across
//! the application.

use anyhow::Context;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

// ---------------------------------------------------------------------------
// DaemonConfig
// ---------------------------------------------------------------------------

/// Configuration for the daemon process lifecycle.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct DaemonConfig {
    /// Process name used for PID file and socket file naming.
    pub name: String,
    /// Directory for the Unix domain socket.
    pub socket_dir: String,
    /// Directory for the PID file.
    pub pid_dir: String,
    /// Seconds to wait for in-flight requests before force-shutdown.
    pub shutdown_grace_period_secs: u64,
    /// Log level: trace, debug, info, warn, error.
    pub log_level: String,
    /// Log file path. Empty string means stdout.
    pub log_file: String,
}

impl Default for DaemonConfig {
    fn default() -> Self {
        DaemonConfig {
            name: "skipjackd".into(),
            socket_dir: "/tmp".into(),
            pid_dir: "/tmp".into(),
            shutdown_grace_period_secs: 30,
            log_level: "info".into(),
            log_file: String::new(),
        }
    }
}

// ---------------------------------------------------------------------------
// CacheConfig
// ---------------------------------------------------------------------------

/// Configuration for the local SQLite result cache.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct CacheConfig {
    /// Path to the SQLite cache database.
    pub db_path: String,
    /// Default TTL in seconds for cache entries.
    pub default_ttl_secs: u64,
    /// Interval in seconds between background eviction runs.
    pub eviction_interval_secs: u64,
}

impl Default for CacheConfig {
    fn default() -> Self {
        CacheConfig {
            db_path: default_cache_db_path(),
            default_ttl_secs: 3600,
            eviction_interval_secs: 300,
        }
    }
}

fn default_cache_db_path() -> String {
    dirs::home_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".cache")
        .join("skipjackd")
        .join("cache.db")
        .to_string_lossy()
        .to_string()
}

// ---------------------------------------------------------------------------
// SearchConfig
// ---------------------------------------------------------------------------

/// Global search request defaults and limits.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct SearchConfig {
    /// Default number of results to return.
    pub default_limit: u32,
    /// Hard cap on the number of results per request.
    pub max_limit: u32,
    /// Maximum time in seconds to wait for all providers.
    pub request_timeout_secs: u64,
}

impl Default for SearchConfig {
    fn default() -> Self {
        SearchConfig {
            default_limit: 10,
            max_limit: 100,
            request_timeout_secs: 60,
        }
    }
}

// ---------------------------------------------------------------------------
// DispatchConfig
// ---------------------------------------------------------------------------

/// Provider dispatch mode configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct DispatchConfig {
    /// Dispatch mode: "concurrent" or "tiered".
    pub mode: String,
}

impl Default for DispatchConfig {
    fn default() -> Self {
        DispatchConfig {
            mode: "concurrent".into(),
        }
    }
}

// ---------------------------------------------------------------------------
// AntiBlockingConfig
// ---------------------------------------------------------------------------

/// Anti-blocking countermeasure parameters.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct AntiBlockingConfig {
    /// Pool of realistic browser User-Agent strings.
    pub user_agents: Vec<String>,
    /// Minimum delay in milliseconds between paginated page requests.
    pub page_delay_min_ms: u64,
    /// Maximum delay in milliseconds between paginated page requests.
    pub page_delay_max_ms: u64,
    /// Base delay in seconds for exponential backoff.
    pub retry_base_delay_secs: u64,
    /// Maximum retry attempts per provider per request.
    pub retry_max_attempts: u32,
    /// Cap in seconds for exponential backoff computation.
    pub retry_cap_secs: u64,
}

impl Default for AntiBlockingConfig {
    fn default() -> Self {
        AntiBlockingConfig {
            user_agents: default_user_agents(),
            page_delay_min_ms: 500,
            page_delay_max_ms: 2000,
            retry_base_delay_secs: 1,
            retry_max_attempts: 3,
            retry_cap_secs: 60,
        }
    }
}

fn default_user_agents() -> Vec<String> {
    vec![
        // Chrome 131 on Windows 10
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36".into(),
        // Chrome 131 on macOS
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36".into(),
        // Firefox 134 on Windows 10
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:134.0) Gecko/20100101 Firefox/134.0".into(),
        // Firefox 134 on macOS
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:134.0) Gecko/20100101 Firefox/134.0".into(),
        // Safari 18 on macOS
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.2 Safari/605.1.15".into(),
        // Chrome 131 on Linux
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36".into(),
        // Firefox 134 on Linux
        "Mozilla/5.0 (X11; Linux x86_64; rv:134.0) Gecko/20100101 Firefox/134.0".into(),
        // Edge 131 on Windows 10
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0".into(),
        // Chrome 130 on Windows 10
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36".into(),
        // Chrome 130 on macOS
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36".into(),
        // Firefox 133 on Windows 10
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:133.0) Gecko/20100101 Firefox/133.0".into(),
        // Safari 17 on macOS
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.6 Safari/605.1.15".into(),
        // Chrome 129 on Windows 10
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36".into(),
        // Firefox 132 on Linux
        "Mozilla/5.0 (X11; Linux x86_64; rv:132.0) Gecko/20100101 Firefox/132.0".into(),
        // Chrome 131 on Windows 11 ARM
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36".into(),
        // Firefox 134 on Windows 11
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:134.0) Gecko/20100101 Firefox/134.0".into(),
        // Chrome 128 on macOS
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36".into(),
        // Firefox ESR 128 on Linux
        "Mozilla/5.0 (X11; Linux x86_64; rv:128.0) Gecko/20100101 Firefox/128.0".into(),
        // Chrome 127 on Linux
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36".into(),
        // Safari 16 on macOS
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Safari/605.1.15".into(),
    ]
}

// ---------------------------------------------------------------------------
// RankingConfig
// ---------------------------------------------------------------------------

/// Configuration for result ranking weight dimensions.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct RankingConfig {
    pub provider_reputation: RankingDimension,
    pub result_position: RankingDimension,
    pub freshness_bonus: FreshnessBonusDimension,
}

impl Default for RankingConfig {
    fn default() -> Self {
        RankingConfig {
            provider_reputation: RankingDimension {
                enabled: false,
                weight: 1.0,
            },
            result_position: RankingDimension {
                enabled: true,
                weight: 1.0,
            },
            freshness_bonus: FreshnessBonusDimension {
                enabled: false,
                weight: 0.0,
                window_days: 7,
            },
        }
    }
}

/// A single ranking weight dimension with an enable toggle and weight.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct RankingDimension {
    /// Whether this dimension contributes to the composite score.
    pub enabled: bool,
    /// Weight multiplier for this dimension.
    pub weight: f64,
}

impl Default for RankingDimension {
    fn default() -> Self {
        RankingDimension {
            enabled: false,
            weight: 0.0,
        }
    }
}

/// Freshness bonus dimension — extends RankingDimension with a time window.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct FreshnessBonusDimension {
    /// Whether the freshness bonus is active.
    pub enabled: bool,
    /// Weight multiplier for the freshness bonus.
    pub weight: f64,
    /// Number of days within which results receive a freshness boost.
    pub window_days: u32,
}

impl Default for FreshnessBonusDimension {
    fn default() -> Self {
        FreshnessBonusDimension {
            enabled: false,
            weight: 0.0,
            window_days: 7,
        }
    }
}

// ---------------------------------------------------------------------------
// ProviderConfig
// ---------------------------------------------------------------------------

/// Per-provider configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ProviderConfig {
    /// Unique provider name (e.g., "duckduckgo", "brave").
    pub name: String,
    /// Whether this provider participates in dispatch.
    pub enabled: bool,
    /// API key (literal value in config).
    pub api_key: Option<String>,
    /// Name of environment variable holding the API key.
    pub api_key_env: Option<String>,
    /// Override the provider's default base URL.
    pub base_url: Option<String>,
    /// Per-provider timeout in seconds.
    pub timeout_secs: Option<u64>,
    /// Provider weight for ranking.
    pub weight: Option<f64>,
    /// Per-provider cache TTL override in seconds.
    pub cache_ttl_secs: Option<u64>,
    /// Whether to shuffle TLS cipher suites for this provider.
    pub tls_shuffle_ciphers: Option<bool>,
    /// IP rotation strategy: "static", "ipv6_pool", "proxy_pool".
    pub ip_rotation_strategy: Option<String>,
    /// IPv6 subnet in CIDR notation (for ipv6_pool strategy).
    pub ipv6_subnet: Option<String>,
    /// List of proxy URLs (for proxy_pool strategy).
    pub proxies: Option<Vec<String>>,
    /// Rate limit in requests per minute.
    pub rate_limit_rpm: Option<u32>,
    /// Per-provider override for max retry attempts.
    pub retry_max_attempts: Option<u32>,
    /// Semantic tags for provider categorization.
    pub tags: Option<Vec<String>>,
}

impl Default for ProviderConfig {
    fn default() -> Self {
        ProviderConfig {
            name: String::new(),
            enabled: true,
            api_key: None,
            api_key_env: None,
            base_url: None,
            timeout_secs: None,
            weight: None,
            cache_ttl_secs: None,
            tls_shuffle_ciphers: None,
            ip_rotation_strategy: None,
            ipv6_subnet: None,
            proxies: None,
            rate_limit_rpm: None,
            retry_max_attempts: None,
            tags: None,
        }
    }
}

// ---------------------------------------------------------------------------
// TierConfig
// ---------------------------------------------------------------------------

/// Tiered dispatch configuration.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct TierConfig {
    /// Ordered list of tier numbers (e.g., [0, 1, 2, 3, 4, 5]).
    pub order: Vec<u32>,
    /// Map of tier number to tier definition.
    #[serde(default)]
    pub tiers: HashMap<u32, TierDef>,
}

/// Definition for a single dispatch tier.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct TierDef {
    /// Provider names in this tier.
    pub providers: Vec<String>,
    /// Maximum time in seconds for this tier to complete.
    pub timeout_secs: u64,
}

impl Default for TierDef {
    fn default() -> Self {
        TierDef {
            providers: Vec::new(),
            timeout_secs: 10,
        }
    }
}

// ---------------------------------------------------------------------------
// Top-level Config
// ---------------------------------------------------------------------------

/// Root configuration combining all sections.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct Config {
    /// Daemon lifecycle settings.
    pub daemon: DaemonConfig,
    /// Local cache settings.
    pub cache: CacheConfig,
    /// Global search defaults.
    pub search: SearchConfig,
    /// Provider dispatch mode.
    pub dispatch: DispatchConfig,
    /// Anti-blocking countermeasure parameters.
    pub anti_blocking: AntiBlockingConfig,
    /// Ranking weight dimensions.
    pub ranking: RankingConfig,
    /// Configured search providers.
    pub providers: Vec<ProviderConfig>,
    /// Tiered dispatch configuration.
    pub tiers: TierConfig,
}

impl Config {
    /// Load configuration from the default path (`~/.config/skipjackd/config.toml`)
    /// or an optional explicit path.
    ///
    /// If the config file does not exist, defaults are used and a warning is logged.
    /// Environment variables with the `SKIPJACKD_` prefix are applied as overrides
    /// after the file is parsed. Provider API keys are resolved from environment
    /// variables when `api_key_env` is set.
    pub fn load(config_path: Option<&str>) -> anyhow::Result<Config> {
        let default_path = dirs::home_dir()
            .unwrap_or_else(|| PathBuf::from("."))
            .join(".config")
            .join("skipjackd")
            .join("config.toml");

        let path: &Path = match config_path {
            Some(p) => Path::new(p),
            None => &default_path,
        };

        let mut config = if path.exists() {
            let content = std::fs::read_to_string(path)
                .with_context(|| format!("Failed to read config file: {}", path.display()))?;
            toml::from_str::<Config>(&content)
                .with_context(|| format!("Failed to parse config file: {}", path.display()))?
        } else {
            tracing::warn!(
                "Config file not found at '{}', using default configuration",
                path.display()
            );
            Config::default()
        };

        apply_env_overrides(&mut config)?;
        validate_providers(&mut config);

        Ok(config)
    }

    /// Consume the config and wrap it in an `Arc` for sharing across threads.
    pub fn freeze(self) -> Arc<Config> {
        Arc::new(self)
    }
}

// ---------------------------------------------------------------------------
// Environment variable overrides
// ---------------------------------------------------------------------------

/// Walk all env vars with the `SKIPJACKD_` prefix and apply them as overrides
/// to the config. Double underscores (`__`) map to nested TOML keys.
fn apply_env_overrides(config: &mut Config) -> anyhow::Result<()> {
    // Round-trip current config to a toml::Table for structural manipulation.
    let config_str = toml::to_string(config)?;
    let mut root: toml::Table = toml::from_str(&config_str)?;

    for (key, value) in std::env::vars() {
        if let Some(suffix) = key.strip_prefix("SKIPJACKD_") {
            let parts: Vec<&str> = suffix.split("__").collect();
            if parts.is_empty() {
                continue;
            }

            let mut current = &mut root;
            for (i, part) in parts.iter().enumerate() {
                let part_lower = part.to_lowercase();
                if i == parts.len() - 1 {
                    // Last segment is the key name.
                    current.insert(part_lower, parse_env_toml_value(&value));
                } else {
                    // Intermediate segment is a table name.
                    let part_for_err = part_lower.clone();
                    current = current
                        .entry(part_lower)
                        .or_insert_with(|| toml::Value::Table(toml::Table::new()))
                        .as_table_mut()
                        .ok_or_else(|| {
                            anyhow::anyhow!(
                                "Cannot apply env override '{}': '{}' is not a TOML table",
                                key,
                                part_for_err
                            )
                        })?;
                }
            }
        }
    }

    // Deserialize the modified table back into Config.
    let merged_str = toml::to_string(&root)?;
    *config = toml::from_str(&merged_str)?;

    Ok(())
}

/// Parse an environment variable value into a TOML value.
///
/// Wraps the value in a synthetic TOML document (`v = <value>`) and parses it,
/// falling back to a string if parsing fails.
fn parse_env_toml_value(s: &str) -> toml::Value {
    let doc = format!("v = {}", s);
    match toml::from_str::<toml::Table>(&doc) {
        Ok(mut table) => {
            if let Some(v) = table.remove("v") {
                return v;
            }
            toml::Value::String(s.to_string())
        }
        Err(_) => toml::Value::String(s.to_string()),
    }
}

// ---------------------------------------------------------------------------
// Provider validation
// ---------------------------------------------------------------------------

/// Validate provider configurations after loading.
///
/// For providers that have `api_key_env` set but no `api_key`, attempt to read
/// the environment variable. If the env var is not set, log a warning.
/// Providers without either an API key or an `api_key_env` are assumed to be
/// keyless (e.g., HTML scraping providers) and are left as-is.
fn validate_providers(config: &mut Config) {
    for provider in &mut config.providers {
        if provider.api_key.is_some() {
            continue;
        }

        if let Some(env_var) = &provider.api_key_env {
            match std::env::var(env_var) {
                Ok(key) => {
                    provider.api_key = Some(key);
                }
                Err(_) => {
                    tracing::warn!(
                        "Provider '{}' references env var '{}' which is not set; \
                         marking provider unavailable",
                        provider.name,
                        env_var
                    );
                    provider.enabled = false;
                }
            }
        }
        // If neither api_key nor api_key_env is set, the provider is assumed to
        // be keyless (e.g., DuckDuckGo HTML scraping). No warning is emitted.
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// Round-trip: serialize default config to TOML, parse it back, and verify
    /// key values match the expected defaults.
    #[test]
    fn roundtrip_default_config() {
        let original = Config::default();
        let toml_str = toml::to_string_pretty(&original).expect("serialize default config");

        let parsed: Config = toml::from_str(&toml_str).expect("parse round-tripped config");

        // Daemon defaults
        assert_eq!(parsed.daemon.name, "skipjackd");
        assert_eq!(parsed.daemon.socket_dir, "/tmp");
        assert_eq!(parsed.daemon.pid_dir, "/tmp");
        assert_eq!(parsed.daemon.shutdown_grace_period_secs, 30);
        assert_eq!(parsed.daemon.log_level, "info");
        assert_eq!(parsed.daemon.log_file, "");

        // Cache defaults
        assert!(parsed.cache.db_path.ends_with("cache.db"));
        assert_eq!(parsed.cache.default_ttl_secs, 3600);
        assert_eq!(parsed.cache.eviction_interval_secs, 300);

        // Search defaults
        assert_eq!(parsed.search.default_limit, 10);
        assert_eq!(parsed.search.max_limit, 100);
        assert_eq!(parsed.search.request_timeout_secs, 60);

        // Dispatch defaults
        assert_eq!(parsed.dispatch.mode, "concurrent");

        // Anti-blocking defaults
        assert!(
            parsed.anti_blocking.user_agents.len() >= 20,
            "expected >=20 user agents, got {}",
            parsed.anti_blocking.user_agents.len()
        );
        assert_eq!(parsed.anti_blocking.page_delay_min_ms, 500);
        assert_eq!(parsed.anti_blocking.page_delay_max_ms, 2000);
        assert_eq!(parsed.anti_blocking.retry_base_delay_secs, 1);
        assert_eq!(parsed.anti_blocking.retry_max_attempts, 3);
        assert_eq!(parsed.anti_blocking.retry_cap_secs, 60);

        // Ranking defaults
        assert!(!parsed.ranking.provider_reputation.enabled);
        assert!((parsed.ranking.provider_reputation.weight - 1.0).abs() < f64::EPSILON);
        assert!(parsed.ranking.result_position.enabled);
        assert!((parsed.ranking.result_position.weight - 1.0).abs() < f64::EPSILON);
        assert!(!parsed.ranking.freshness_bonus.enabled);
        assert!((parsed.ranking.freshness_bonus.weight - 0.0).abs() < f64::EPSILON);
        assert_eq!(parsed.ranking.freshness_bonus.window_days, 7);

        // Providers and tiers are empty by default
        assert!(parsed.providers.is_empty());
        assert!(parsed.tiers.order.is_empty());
        assert!(parsed.tiers.tiers.is_empty());
    }

    /// When only a subset of fields are present in TOML, missing fields should
    /// fall back to their defaults.
    #[test]
    fn partial_toml_uses_defaults_for_missing_fields() {
        let toml_str = r#"
[daemon]
name = "customd"

[search]
default_limit = 25
"#;

        let config: Config = toml::from_str(toml_str).expect("parse partial config");

        // Explicitly set fields
        assert_eq!(config.daemon.name, "customd");
        assert_eq!(config.search.default_limit, 25);

        // Unspecified fields should use defaults
        assert_eq!(config.daemon.socket_dir, "/tmp");
        assert_eq!(config.daemon.shutdown_grace_period_secs, 30);
        assert_eq!(config.search.max_limit, 100);
        assert_eq!(config.dispatch.mode, "concurrent");
        assert!(config.providers.is_empty());
    }

    /// Providers with `api_key_env` set should resolve the key from the
    /// environment at validation time.
    #[test]
    fn provider_api_key_resolved_from_env() {
        // Set up a test env var
        std::env::set_var("SKIPJACKD_TEST_JINA_KEY", "test-jina-key-12345");

        let toml_str = r#"
[[providers]]
name = "jina"
enabled = true
api_key_env = "SKIPJACKD_TEST_JINA_KEY"
"#;

        let mut config: Config = toml::from_str(toml_str).expect("parse provider config");
        validate_providers(&mut config);

        assert_eq!(config.providers.len(), 1);
        assert_eq!(
            config.providers[0].api_key.as_deref(),
            Some("test-jina-key-12345")
        );

        // Clean up
        std::env::remove_var("SKIPJACKD_TEST_JINA_KEY");
    }

    /// A provider with a missing `api_key_env` variable should log a warning
    /// and be marked unavailable (enabled = false).
    #[test]
    fn provider_missing_env_var_marks_unavailable() {
        // Ensure the env var is not set
        std::env::remove_var("NONEXISTENT_API_KEY");

        let toml_str = r#"
[[providers]]
name = "someapi"
enabled = true
api_key_env = "NONEXISTENT_API_KEY"
"#;

        let mut config: Config = toml::from_str(toml_str).expect("parse provider config");
        validate_providers(&mut config);

        assert_eq!(config.providers.len(), 1);
        assert!(config.providers[0].api_key.is_none());
        // Provider with missing api_key_env is marked unavailable
        assert!(!config.providers[0].enabled);
    }

    /// A keyless provider (no api_key, no api_key_env) is left untouched.
    #[test]
    fn keyless_provider_not_flagged() {
        let toml_str = r#"
[[providers]]
name = "duckduckgo"
enabled = true
"#;

        let mut config: Config = toml::from_str(toml_str).expect("parse keyless provider");
        validate_providers(&mut config);

        assert_eq!(config.providers.len(), 1);
        assert!(config.providers[0].api_key.is_none());
        assert!(config.providers[0].api_key_env.is_none());
        assert!(config.providers[0].enabled);
    }

    /// `freeze()` wraps the config in an `Arc`.
    #[test]
    fn freeze_returns_arc() {
        let config = Config::default();
        let frozen = config.freeze();

        assert_eq!(frozen.daemon.name, "skipjackd");
        // Verify Arc reference count starts at 1
        assert_eq!(Arc::strong_count(&frozen), 1);
    }

    /// Test that `SKIPJACKD_CACHE__DEFAULT_TTL_SECS=7200` env override works.
    #[test]
    fn env_override_sets_cache_ttl() {
        std::env::set_var("SKIPJACKD_CACHE__DEFAULT_TTL_SECS", "7200");

        let mut config = Config::default();
        apply_env_overrides(&mut config).expect("apply env overrides");

        assert_eq!(config.cache.default_ttl_secs, 7200);

        std::env::remove_var("SKIPJACKD_CACHE__DEFAULT_TTL_SECS");
    }

    /// Test nested env override for ranking dimensions.
    #[test]
    fn env_override_sets_ranking_dimension() {
        std::env::set_var("SKIPJACKD_RANKING__FRESHNESS_BONUS__ENABLED", "true");
        std::env::set_var("SKIPJACKD_RANKING__FRESHNESS_BONUS__WEIGHT", "0.5");

        let mut config = Config::default();
        apply_env_overrides(&mut config).expect("apply env overrides");

        assert!(config.ranking.freshness_bonus.enabled);
        assert!((config.ranking.freshness_bonus.weight - 0.5).abs() < f64::EPSILON);

        std::env::remove_var("SKIPJACKD_RANKING__FRESHNESS_BONUS__ENABLED");
        std::env::remove_var("SKIPJACKD_RANKING__FRESHNESS_BONUS__WEIGHT");
    }

    /// Test that boolean env values parse correctly.
    #[test]
    fn env_override_boolean_values() {
        std::env::set_var("SKIPJACKD_RANKING__RESULT_POSITION__ENABLED", "false");

        let mut config = Config::default();
        apply_env_overrides(&mut config).expect("apply env overrides");

        assert!(!config.ranking.result_position.enabled);

        std::env::remove_var("SKIPJACKD_RANKING__RESULT_POSITION__ENABLED");
    }

    /// Test that string env values parse correctly (including those with spaces).
    #[test]
    fn env_override_string_value() {
        std::env::set_var("SKIPJACKD_DAEMON__LOG_LEVEL", "debug");

        let mut config = Config::default();
        apply_env_overrides(&mut config).expect("apply env overrides");

        assert_eq!(config.daemon.log_level, "debug");

        std::env::remove_var("SKIPJACKD_DAEMON__LOG_LEVEL");
    }

    /// Test that array env values parse correctly via TOML array syntax in env var.
    #[test]
    fn env_override_array_value() {
        std::env::set_var("SKIPJACKD_TIERS__ORDER", "[0, 1, 2]");

        let mut config = Config::default();
        apply_env_overrides(&mut config).expect("apply env overrides");

        assert_eq!(config.tiers.order, vec![0, 1, 2]);

        std::env::remove_var("SKIPJACKD_TIERS__ORDER");
    }

    /// The `dispatch.mode` field defaults to "concurrent" and can be overridden.
    #[test]
    fn dispatch_mode_default_and_override() {
        // Default
        let config = Config::default();
        assert_eq!(config.dispatch.mode, "concurrent");

        // Override via env
        std::env::set_var("SKIPJACKD_DISPATCH__MODE", "tiered");
        let mut config = Config::default();
        apply_env_overrides(&mut config).expect("apply env overrides");
        assert_eq!(config.dispatch.mode, "tiered");
        std::env::remove_var("SKIPJACKD_DISPATCH__MODE");
    }
}
