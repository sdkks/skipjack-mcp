//! Provider orchestration manager with concurrent dispatch and health tracking.
//!
//! The [`Manager`] is the central nervous system of the search daemon. It wires
//! together: cache, rate limiter, retry, provider dispatch (concurrent or tiered),
//! result merging, and per-provider health tracking.
//!
//! # Architecture
//!
//! ```text
//! search(request) -> dispatch_mode?
//!   concurrent  -> Manager::search_concurrent(request)
//!   tiered      -> Manager::search_tiered(request)
//!
//! Per-provider pipeline:
//!   cache check -> rate limiter -> retry wrapper -> provider.search() -> cache set
//! ```

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::RwLock;
use tokio::task::JoinSet;
use tracing;

use crate::anti_blocking::{retry_with_backoff, RateLimiter, RetryConfig};
use crate::cache::{cache_key, provider_list_string, Cache};
use crate::config::{Config, ProviderConfig as CfgProviderConfig, RankingConfig};
use crate::search::merge::ResultMerger;
use crate::search::provider::{Provider, ProviderClientConfig, ProviderError, Tag};
use crate::search::{SearchRequest, SearchResponse};

// ---------------------------------------------------------------------------
// Health tracking
// ---------------------------------------------------------------------------

/// A health event recorded for a provider.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HealthEvent {
    /// A successful provider response.
    Success,
    /// A failed provider response.
    Failure,
}

/// The computed health status of a provider.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum HealthStatus {
    /// Provider is operating normally.
    Healthy,
    /// Provider error rate exceeds 50% in the sliding window.
    Degraded,
    /// Provider has exceeded the consecutive failure threshold.
    Unhealthy,
}

/// Sliding-window health state for a single provider.
///
/// Tracks per-provider success/failure events over a 5-minute window. Health
/// score is the success rate over that window. Consecutive failures beyond the
/// threshold trigger [`HealthStatus::Unhealthy`]. A successful health probe
/// can recover a previously-unhealthy provider.
///
/// # Thread safety
///
/// Wrapped in `Arc<RwLock<HealthState>>` by the [`Manager`] so that concurrent
/// dispatches can record events without contention on the same provider state.
#[derive(Debug, Clone)]
pub struct HealthState {
    /// Sliding window of recent health events with timestamps.
    events: VecDeque<(Instant, HealthEvent)>,
    /// Number of consecutive failures since the last success.
    consecutive_failures: u32,
    /// Current health status.
    status: HealthStatus,
    /// Consecutive failures that trigger unhealthy state.
    consecutive_failure_threshold: u32,
    /// Sliding window duration.
    window_duration: Duration,
}

impl HealthState {
    /// Create a new `HealthState` with default values.
    ///
    /// A newly created state is [`HealthStatus::Healthy`] and has an empty
    /// event window.
    pub fn new(consecutive_failure_threshold: u32) -> Self {
        HealthState {
            events: VecDeque::new(),
            consecutive_failures: 0,
            status: HealthStatus::Healthy,
            consecutive_failure_threshold,
            window_duration: Duration::from_secs(300), // 5 minutes
        }
    }

    /// Record a successful provider response.
    ///
    /// Resets the consecutive failure counter and can recover the provider
    /// from [`HealthStatus::Unhealthy`].
    pub fn record_success(&mut self) {
        self.prune_expired();
        self.events
            .push_back((Instant::now(), HealthEvent::Success));
        self.consecutive_failures = 0;

        // Recover from degraded or unhealthy on success.
        if self.status != HealthStatus::Healthy {
            tracing::info!("provider health recovered to Healthy");
            self.status = HealthStatus::Healthy;
        }
    }

    /// Record a failed provider response.
    ///
    /// Increments the consecutive failure counter. If the counter exceeds the
    /// threshold, the status transitions to [`HealthStatus::Unhealthy`].
    pub fn record_failure(&mut self) {
        self.prune_expired();
        self.events
            .push_back((Instant::now(), HealthEvent::Failure));
        self.consecutive_failures += 1;

        if self.consecutive_failures >= self.consecutive_failure_threshold {
            self.status = HealthStatus::Unhealthy;
            tracing::warn!(
                consecutive_failures = self.consecutive_failures,
                threshold = self.consecutive_failure_threshold,
                "provider marked Unhealthy"
            );
        } else if self.events.len() >= 3 && self.health_score() < 0.5 {
            self.status = HealthStatus::Degraded;
            tracing::debug!(
                health_score = self.health_score(),
                "provider marked Degraded"
            );
        }
    }

    /// Returns `true` when the provider's status is [`HealthStatus::Healthy`].
    ///
    /// The status field is the single source of truth, updated by
    /// [`record_success`](Self::record_success) and
    /// [`record_failure`](Self::record_failure).
    pub fn is_healthy(&self) -> bool {
        self.status == HealthStatus::Healthy
    }

    /// Return the current [`HealthStatus`].
    pub fn status(&self) -> HealthStatus {
        self.status
    }

    /// Compute the health score as the success rate over the sliding window.
    ///
    /// Filters out expired events so the score reflects only the window,
    /// even when called independently of `record_success`/`record_failure`.
    /// Returns `1.0` when no events remain after pruning.
    pub fn health_score(&self) -> f64 {
        let cutoff = Instant::now()
            .checked_sub(self.window_duration)
            .unwrap_or(Instant::now());
        let in_window: Vec<_> = self.events.iter().filter(|(t, _)| *t >= cutoff).collect();
        let total = in_window.len();
        if total == 0 {
            return 1.0;
        }
        let successes = in_window
            .iter()
            .filter(|(_, e)| *e == HealthEvent::Success)
            .count();
        successes as f64 / total as f64
    }

    /// Return the number of consecutive failures since the last success.
    pub fn consecutive_failures(&self) -> u32 {
        self.consecutive_failures
    }

    /// Prune events that are older than the 5-minute window.
    fn prune_expired(&mut self) {
        let cutoff = Instant::now()
            .checked_sub(self.window_duration)
            .unwrap_or(Instant::now());
        while self.events.front().is_some_and(|(t, _)| *t < cutoff) {
            self.events.pop_front();
        }
    }
}

// ---------------------------------------------------------------------------
// Provider catalog
// ---------------------------------------------------------------------------

/// Catalog of registered search providers, keyed by provider name.
///
/// Providers are registered at startup from the configuration file. The catalog
/// supports filtering by tag, name list, and availability.
#[derive(Default)]
pub struct ProviderCatalog {
    providers: HashMap<String, Box<dyn Provider>>,
}

impl ProviderCatalog {
    /// Create an empty provider catalog.
    pub fn new() -> Self {
        ProviderCatalog {
            providers: HashMap::new(),
        }
    }

    /// Register a provider in the catalog.
    ///
    /// If a provider with the same name already exists, it is replaced.
    pub fn register(&mut self, provider: Box<dyn Provider>) {
        let name = provider.name().to_string();
        self.providers.insert(name, provider);
    }

    /// Look up a provider by name.
    ///
    /// Returns `None` if no provider is registered under the given name.
    pub fn get(&self, name: &str) -> Option<&dyn Provider> {
        self.providers.get(name).map(|p| p.as_ref())
    }

    /// Return all enabled providers, optionally filtered by tag or name list.
    ///
    /// When `filter_names` is provided, only providers with matching names are
    /// returned. When `filter_tag` is provided, only providers with that tag
    /// are returned. Both filters are AND-combined.
    ///
    /// When neither filter is provided, all enabled providers are returned.
    pub fn enabled_providers(
        &self,
        filter_names: Option<&[String]>,
        filter_tag: Option<Tag>,
    ) -> Vec<&dyn Provider> {
        self.providers
            .values()
            .filter(|p| p.is_available())
            .filter(|p| {
                if let Some(names) = filter_names {
                    names.contains(&p.name().to_string())
                } else {
                    true
                }
            })
            .filter(|p| {
                if let Some(tag) = filter_tag {
                    p.tags().contains(&tag)
                } else {
                    true
                }
            })
            .map(|p| p.as_ref())
            .collect()
    }

    /// Return all registered providers (including disabled ones).
    pub fn all_providers(&self) -> Vec<&dyn Provider> {
        self.providers.values().map(|p| p.as_ref()).collect()
    }

    /// Return the number of registered providers.
    pub fn len(&self) -> usize {
        self.providers.len()
    }

    /// Return `true` if the catalog is empty.
    pub fn is_empty(&self) -> bool {
        self.providers.is_empty()
    }
}

// ---------------------------------------------------------------------------
// Manager
// ---------------------------------------------------------------------------

/// The provider orchestration manager.
///
/// Holds the provider catalog, cache, rate limiter, and per-provider health
/// states. The [`search`](Manager::search) method is the main entry point for
/// executing a search across configured providers.
///
/// # Example
///
/// ```ignore
/// use metasearchd::config::Config;
/// use metasearchd::daemon::manager::Manager;
///
/// let config = Config::load(None)?.freeze();
/// let manager = Manager::new(&config).await?;
/// let response = manager.search(&request).await?;
/// ```
pub struct Manager {
    /// Provider catalog with all registered providers.
    /// Stored behind `Arc` so spawned tasks can look up providers by name.
    catalog: Arc<ProviderCatalog>,
    /// Shared cache instance.
    cache: Arc<Cache>,
    /// Shared rate limiter.
    rate_limiter: Arc<RateLimiter>,
    /// Retry configuration.
    retry_config: RetryConfig,
    /// Per-provider health states.
    health_states: Arc<RwLock<HashMap<String, HealthState>>>,
    /// Global request timeout in seconds.
    request_timeout_secs: u64,
    /// Default dispatch mode ("concurrent" or "tiered").
    dispatch_mode: String,
    /// Default result limit.
    default_limit: usize,
    /// Per-provider RPM overrides from config.
    provider_rpm: HashMap<String, u32>,
    /// Default cache TTL in seconds.
    cache_ttl_secs: u64,
    /// Ranking configuration.
    ranking: RankingConfig,
    /// Per-provider weight map for ranking (provider name → weight).
    provider_weights: HashMap<String, f64>,
}

impl Manager {
    /// Create a new [`Manager`] from configuration.
    ///
    /// Reads provider configurations from `config.providers`, constructs each
    /// provider, and registers it in the internal catalog. The cache is opened
    /// from the configured database path.
    ///
    /// # Errors
    ///
    /// Returns an error if the cache database cannot be opened, or if a
    /// provider cannot be constructed.
    pub async fn new(config: &Config) -> anyhow::Result<Self> {
        let cache = Cache::open(&config.cache.db_path)?;
        let rate_limiter = Arc::new(RateLimiter::new());
        let retry_config = RetryConfig::from(&config.anti_blocking);
        let consecutive_failure_threshold = 3u32;

        let mut catalog = ProviderCatalog::new();
        let mut health_states = HashMap::new();
        let mut provider_rpm = HashMap::new();
        let mut provider_weights = HashMap::new();

        // Build providers from config.
        for provider_cfg in &config.providers {
            if !provider_cfg.enabled {
                tracing::info!(
                    provider = %provider_cfg.name,
                    "provider disabled, skipping registration"
                );
                continue;
            }

            let client_config = ProviderClientConfig {
                tls_shuffle_ciphers: provider_cfg.tls_shuffle_ciphers.unwrap_or(false),
                ip_rotation_strategy: provider_cfg.ip_rotation_strategy.clone(),
                ipv6_subnet: provider_cfg.ipv6_subnet.clone(),
                proxies: provider_cfg.proxies.clone(),
                timeout_secs: provider_cfg.timeout_secs,
            };

            let rpm = provider_cfg.rate_limit_rpm.unwrap_or(30);
            provider_rpm.insert(provider_cfg.name.clone(), rpm);
            provider_weights.insert(
                provider_cfg.name.clone(),
                provider_cfg.weight.unwrap_or(1.0),
            );

            let provider =
                Self::create_provider(provider_cfg, &client_config, rate_limiter.clone(), rpm)?;

            health_states.insert(
                provider.name().to_string(),
                HealthState::new(consecutive_failure_threshold),
            );
            catalog.register(provider);
        }

        tracing::info!(
            provider_count = catalog.len(),
            "manager initialized with {} providers",
            catalog.len()
        );

        Ok(Manager {
            catalog: Arc::new(catalog),
            cache: Arc::new(cache),
            rate_limiter,
            retry_config,
            health_states: Arc::new(RwLock::new(health_states)),
            request_timeout_secs: config.search.request_timeout_secs,
            dispatch_mode: config.dispatch.mode.clone(),
            default_limit: config.search.default_limit as usize,
            provider_rpm,
            cache_ttl_secs: config.cache.default_ttl_secs,
            ranking: config.ranking.clone(),
            provider_weights,
        })
    }

    /// Factory: create a provider from its configuration.
    ///
    /// Dispatches on `cfg.name` to construct the right provider implementation.
    fn create_provider(
        cfg: &CfgProviderConfig,
        client_config: &ProviderClientConfig,
        rate_limiter: Arc<RateLimiter>,
        rpm: u32,
    ) -> anyhow::Result<Box<dyn Provider>> {
        match cfg.name.as_str() {
            "duckduckgo" => {
                let provider = crate::search::providers::duckduckgo::DuckDuckGoProvider::new(
                    client_config,
                    rate_limiter,
                    rpm,
                    true,
                )
                .map_err(|e| anyhow::anyhow!("failed to create DuckDuckGo provider: {}", e))?;
                Ok(Box::new(provider))
            }
            other => {
                anyhow::bail!("unknown provider type: '{}'", other);
            }
        }
    }

    /// Main entry point: execute a search across configured providers.
    ///
    /// Dispatches based on `request.dispatch_mode` (falling back to config default)
    /// to either [`search_concurrent`](Manager::search_concurrent) or
    /// [`search_tiered`](Manager::search_tiered).
    pub async fn search(&self, request: &SearchRequest) -> Result<SearchResponse, ProviderError> {
        let start = Instant::now();

        let mode = request
            .dispatch_mode
            .as_deref()
            .unwrap_or(&self.dispatch_mode);

        tracing::debug!(
            request_id = %request.request_id,
            mode = mode,
            query = %request.query,
            "search started"
        );

        let mut response = match mode {
            "tiered" => self.search_tiered(request).await?,
            _ => self.search_concurrent(request).await?,
        };

        response.elapsed_ms = start.elapsed().as_millis() as u64;
        response.request_id = request.request_id.clone();

        tracing::debug!(
            request_id = %request.request_id,
            total_results = response.total_found,
            providers_used = ?response.providers_used,
            elapsed_ms = response.elapsed_ms,
            "search completed"
        );

        Ok(response)
    }

    /// Execute a concurrent dispatch: fire all eligible providers at once.
    ///
    /// Per [DR-001], this is the default mode. Playwright providers are excluded.
    ///
    /// # Algorithm
    ///
    /// 1. Determine provider set: explicit list if set in request, else all
    ///    enabled healthy non-Playwright providers.
    /// 2. Check cache for the combined provider set.
    /// 3. Spawn each provider in a [`JoinSet`] with per-provider timeout.
    /// 4. Collect results as they complete; skip errors and timeouts.
    /// 5. Merge, deduplicate, rank, and return top-N.
    async fn search_concurrent(
        &self,
        request: &SearchRequest,
    ) -> Result<SearchResponse, ProviderError> {
        let limit = if request.limit > 0 {
            request.limit
        } else {
            self.default_limit
        };

        // Determine which providers to use.
        let provider_names: Vec<String> = if !request.providers.is_empty() {
            request.providers.clone()
        } else {
            self.catalog
                .enabled_providers(None, None)
                .iter()
                .filter(|p| !p.tags().contains(&Tag::Playwright))
                .map(|p| p.name().to_string())
                .collect()
        };

        // Filter to healthy non-Playwright providers that are in the list.
        let health_lock = self.health_states.read().await;
        let providers_to_dispatch: Vec<String> = self
            .catalog
            .enabled_providers(Some(&provider_names), None)
            .into_iter()
            .filter(|p| !p.tags().contains(&Tag::Playwright))
            .filter(|p| {
                health_lock
                    .get(p.name())
                    .map(|h| h.is_healthy())
                    .unwrap_or(true)
            })
            .map(|p| p.name().to_string())
            .collect();
        drop(health_lock);

        if providers_to_dispatch.is_empty() {
            return Ok(SearchResponse {
                request_id: request.request_id.clone(),
                results: Vec::new(),
                total_found: 0,
                providers_used: Vec::new(),
                cache_hit: false,
                elapsed_ms: 0,
            });
        }

        // Check combined cache.
        let combo_cache_key = cache_key(
            &request.query,
            &providers_to_dispatch,
            request.freshness.as_ref(),
        );
        if let Ok(Some(cached)) = self.cache.get(&combo_cache_key) {
            tracing::debug!(
                request_id = %request.request_id,
                "cache hit for concurrent dispatch"
            );
            return Ok(cached);
        }

        // Fire all providers concurrently.
        let global_timeout = Duration::from_secs(self.request_timeout_secs);
        let mut join_set = JoinSet::new();

        for provider_name in &providers_to_dispatch {
            let name = provider_name.clone();
            let rpm = self.provider_rpm.get(&name).copied().unwrap_or(30);
            let rate_limiter = Arc::clone(&self.rate_limiter);
            let retry_config = self.retry_config.clone();
            let cache = Arc::clone(&self.cache);
            let health_states = Arc::clone(&self.health_states);
            let request = request.clone();
            let cache_ttl = self.cache_ttl_secs;
            let catalog = Arc::clone(&self.catalog);

            join_set.spawn(async move {
                // Look up the provider from the catalog.
                let provider = match catalog.get(&name) {
                    Some(p) => p,
                    None => {
                        let err = ProviderError::Internal(format!(
                            "provider '{}' not found in catalog",
                            name
                        ));
                        // Record health failure for a missing provider.
                        // This shouldn't happen in practice but is handled
                        // defensively.
                        let mut health_lock = health_states.write().await;
                        if let Some(state) = health_lock.get_mut(&name) {
                            state.record_failure();
                        }
                        return (name, Err(err));
                    }
                };

                let result = Self::execute_provider_pipeline(
                    provider,
                    &name,
                    rpm,
                    &rate_limiter,
                    &retry_config,
                    &cache,
                    cache_ttl,
                    &request,
                )
                .await;

                // Record health event.
                {
                    let mut health_lock = health_states.write().await;
                    if let Some(state) = health_lock.get_mut(&name) {
                        match &result {
                            Ok(_) => state.record_success(),
                            Err(_) => state.record_failure(),
                        }
                    }
                }

                (name, result)
            });
        }

        // Wait for completion with global timeout.
        let mut responses: Vec<SearchResponse> = Vec::new();
        let deadline = tokio::time::Instant::now() + global_timeout;

        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                break;
            }

            match tokio::time::timeout(remaining, join_set.join_next()).await {
                Ok(Some(Ok((provider_name, result)))) => match result {
                    Ok(response) => {
                        tracing::debug!(
                            provider = %provider_name,
                            results = response.results.len(),
                            "provider returned results"
                        );
                        responses.push(response);
                    }
                    Err(err) => {
                        tracing::warn!(
                            provider = %provider_name,
                            error = %err,
                            "provider error, skipping"
                        );
                    }
                },
                Ok(Some(Err(join_err))) => {
                    tracing::warn!(error = %join_err, "provider task panicked");
                }
                Ok(None) => {
                    // All tasks completed.
                    break;
                }
                Err(_elapsed) => {
                    // Global timeout reached.
                    tracing::debug!(
                        request_id = %request.request_id,
                        "global timeout reached, collecting partial results"
                    );
                    join_set.abort_all();
                    break;
                }
            }
        }

        // Merge, rank, and cache.
        let mut merged = ResultMerger::merge(responses, limit);
        merged.results = crate::search::rank(merged.results, &self.ranking, &self.provider_weights);
        let providers_used = merged.providers_used.clone();
        let provider_list_str = provider_list_string(&providers_used);
        let cache_response_key =
            cache_key(&request.query, &providers_used, request.freshness.as_ref());
        if let Err(e) = self.cache.set(
            &cache_response_key,
            &request.query,
            &provider_list_str,
            &merged,
            self.cache_ttl_secs,
        ) {
            tracing::warn!(error = %e, "failed to write cache entry for concurrent dispatch");
        }

        Ok(merged)
    }

    /// Execute a tiered dispatch: fire providers grouped by tiers, stopping
    /// when cumulative results meet the requested limit.
    ///
    /// Providers within a tier are fired concurrently. Tiers are processed
    /// in the configured order. If cumulative results from completed tiers
    /// are >= `request.limit`, subsequent tiers are skipped.
    async fn search_tiered(
        &self,
        request: &SearchRequest,
    ) -> Result<SearchResponse, ProviderError> {
        let limit = if request.limit > 0 {
            request.limit
        } else {
            self.default_limit
        };

        // Resolve the candidate providers first so we can check the combined cache.
        let provider_names: Vec<String> = if !request.providers.is_empty() {
            request.providers.clone()
        } else {
            self.catalog
                .enabled_providers(None, None)
                .iter()
                .map(|p| p.name().to_string())
                .collect()
        };

        let health_lock = self.health_states.read().await;
        let all_providers: Vec<String> = self
            .catalog
            .enabled_providers(Some(&provider_names), None)
            .into_iter()
            .filter(|p| {
                health_lock
                    .get(p.name())
                    .map(|h| h.is_healthy())
                    .unwrap_or(true)
            })
            .map(|p| p.name().to_string())
            .collect();
        drop(health_lock);

        // Check combined cache before spawning any provider tasks.
        let combo_cache_key = cache_key(&request.query, &all_providers, request.freshness.as_ref());
        if let Ok(Some(cached)) = self.cache.get(&combo_cache_key) {
            tracing::debug!(
                request_id = %request.request_id,
                "cache hit for tiered dispatch"
            );
            return Ok(cached);
        }

        let mut all_responses: Vec<SearchResponse> = Vec::new();
        let mut cumulative: usize = 0;
        let global_timeout = Duration::from_secs(self.request_timeout_secs);
        let deadline = tokio::time::Instant::now() + global_timeout;

        // Group providers into tiers by their primary tag.
        // Tag priority defines tier order (lower = higher priority).
        // Providers without tags go into the lowest-priority tier.
        let tier_order: Vec<Vec<String>> = {
            // Priority mapping: lower number = higher-priority tier.
            fn tag_priority(tag: Tag) -> usize {
                match tag {
                    Tag::Web => 0,
                    Tag::News => 1,
                    Tag::Academic => 2,
                    Tag::Code => 3,
                    Tag::Privacy => 4,
                    Tag::Finance => 5,
                    Tag::Knowledge => 6,
                    Tag::Images => 7,
                    Tag::Video => 8,
                    Tag::Shopping => 9,
                    Tag::Playwright => 10,
                }
            }

            let mut tiered: BTreeMap<usize, Vec<String>> = BTreeMap::new();
            for name in &all_providers {
                let tier = if let Some(p) = self.catalog.get(name) {
                    p.tags()
                        .iter()
                        .map(|t| tag_priority(*t))
                        .min()
                        .unwrap_or(usize::MAX)
                } else {
                    usize::MAX
                };
                tiered.entry(tier).or_default().push(name.clone());
            }
            tiered.into_values().collect()
        };

        for chunk in &tier_order {
            if cumulative >= limit {
                break;
            }

            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                break;
            }

            let mut join_set = JoinSet::new();

            for provider_name in chunk {
                let name = provider_name.clone();
                let rpm = self.provider_rpm.get(name.as_str()).copied().unwrap_or(30);
                let rate_limiter = Arc::clone(&self.rate_limiter);
                let retry_config = self.retry_config.clone();
                let cache = Arc::clone(&self.cache);
                let health_states = Arc::clone(&self.health_states);
                let request = request.clone();
                let cache_ttl = self.cache_ttl_secs;
                let catalog = Arc::clone(&self.catalog);

                join_set.spawn(async move {
                    // Look up the provider from the catalog.
                    let provider = match catalog.get(&name) {
                        Some(p) => p,
                        None => {
                            let err = ProviderError::Internal(format!(
                                "provider '{}' not found in catalog",
                                name
                            ));
                            let mut health_lock = health_states.write().await;
                            if let Some(state) = health_lock.get_mut(&name) {
                                state.record_failure();
                            }
                            return (name, Err(err));
                        }
                    };

                    let result = Self::execute_provider_pipeline(
                        provider,
                        &name,
                        rpm,
                        &rate_limiter,
                        &retry_config,
                        &cache,
                        cache_ttl,
                        &request,
                    )
                    .await;

                    {
                        let mut health_lock = health_states.write().await;
                        if let Some(state) = health_lock.get_mut(&name) {
                            match &result {
                                Ok(_) => state.record_success(),
                                Err(_) => state.record_failure(),
                            }
                        }
                    }

                    (name, result)
                });
            }

            // Wait for tier completion.
            while let Some(task_result) = join_set.join_next().await {
                match task_result {
                    Ok((_provider_name, Ok(response))) => {
                        cumulative += response.results.len();
                        all_responses.push(response);
                    }
                    Ok((provider_name, Err(err))) => {
                        tracing::warn!(
                            provider = %provider_name,
                            error = %err,
                            "tiered provider error, skipping"
                        );
                    }
                    Err(join_err) => {
                        tracing::warn!(error = %join_err, "tiered provider task panicked");
                    }
                }
            }
        }

        let mut merged = ResultMerger::merge(all_responses, limit);
        merged.results = crate::search::rank(merged.results, &self.ranking, &self.provider_weights);
        let providers_used = merged.providers_used.clone();
        let provider_list_str = provider_list_string(&providers_used);
        let cache_response_key =
            cache_key(&request.query, &providers_used, request.freshness.as_ref());
        if let Err(e) = self.cache.set(
            &cache_response_key,
            &request.query,
            &provider_list_str,
            &merged,
            self.cache_ttl_secs,
        ) {
            tracing::warn!(error = %e, "failed to write cache entry for tiered dispatch");
        }

        Ok(merged)
    }

    /// Execute the full per-provider pipeline: cache check, rate limiter,
    /// retry, provider search, cache set.
    ///
    /// The `provider` parameter is a trait-object reference obtained from the
    /// catalog. The caller (a spawned task) looks up the provider by name from
    /// an `Arc<ProviderCatalog>` clone, ensuring the reference is valid for the
    /// task's lifetime.
    #[allow(clippy::too_many_arguments)]
    async fn execute_provider_pipeline(
        provider: &dyn Provider,
        provider_name: &str,
        rpm: u32,
        rate_limiter: &RateLimiter,
        retry_config: &RetryConfig,
        cache: &Cache,
        cache_ttl: u64,
        request: &SearchRequest,
    ) -> Result<SearchResponse, ProviderError> {
        // 1. Cache check.
        let cache_key_str = cache_key(
            &request.query,
            &[provider_name.to_string()],
            request.freshness.as_ref(),
        );
        if let Ok(Some(mut cached)) = cache.get(&cache_key_str) {
            tracing::debug!(provider = %provider_name, "per-provider cache hit");
            cached.cache_hit = true;
            return Ok(cached);
        }

        // 2. Rate limiter acquire.
        rate_limiter.acquire(provider_name, rpm).await;

        // 3. Retry wrapper around provider search.
        let query_for_cache = request.query.clone();
        let name_for_cache = provider_name.to_string();

        let response = retry_with_backoff(
            || {
                let req = request.clone();
                let cache_ref = cache;
                let key = cache_key_str.clone();
                let ttl = cache_ttl;
                let nm = name_for_cache.clone();
                let q = query_for_cache.clone();
                async move {
                    // Call the provider's actual search method.
                    let resp = provider.search(&req).await?;
                    // 4. Cache set on success.
                    let provider_list = provider_list_string(std::slice::from_ref(&nm));
                    if let Err(e) = cache_ref.set(&key, &q, &provider_list, &resp, ttl) {
                        tracing::warn!(error = %e, provider = %nm, "failed to write per-provider cache entry");
                    }
                    Ok(resp)
                }
            },
            retry_config,
        )
        .await?;

        Ok(response)
    }

    /// Return the current provider catalog reference.
    pub fn catalog(&self) -> &ProviderCatalog {
        &self.catalog
    }

    /// Return the current health states snapshot.
    pub async fn health_snapshot(&self) -> HashMap<String, HealthState> {
        self.health_states.read().await.clone()
    }

    /// Return the current cache statistics.
    pub fn cache_stats(&self) -> crate::cache::CacheStats {
        self.cache.stats()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::search::provider::Tag;
    use crate::search::SearchResult;
    use async_trait::async_trait;
    use std::sync::Mutex;
    use std::time::Duration;

    // -----------------------------------------------------------------------
    // Mock providers for testing
    // -----------------------------------------------------------------------

    /// A mock provider that sleeps for a configurable duration before returning
    /// results. Used to verify concurrent execution.
    struct SleepProvider {
        name: String,
        tags: Vec<Tag>,
        sleep_ms: u64,
        result_count: usize,
    }

    #[async_trait]
    impl Provider for SleepProvider {
        fn name(&self) -> &str {
            &self.name
        }

        fn tags(&self) -> &[Tag] {
            &self.tags
        }

        fn description(&self) -> &str {
            "Mock sleeping provider for concurrency tests"
        }

        fn is_available(&self) -> bool {
            true
        }

        async fn search(&self, request: &SearchRequest) -> Result<SearchResponse, ProviderError> {
            tokio::time::sleep(Duration::from_millis(self.sleep_ms)).await;

            let results: Vec<SearchResult> = (0..self.result_count)
                .map(|i| SearchResult {
                    title: format!("Result {} from {}", i, self.name),
                    url: format!("https://{}.example.com/{}", self.name, i),
                    snippet: format!("Snippet from {}", self.name),
                    published_date: None,
                    provider_name: self.name.clone(),
                    rank_score: 1.0 / ((i + 1) as f64),
                })
                .collect();

            Ok(SearchResponse {
                request_id: request.request_id.clone(),
                results,
                total_found: self.result_count,
                providers_used: vec![self.name.clone()],
                cache_hit: false,
                elapsed_ms: self.sleep_ms,
            })
        }
    }

    /// A mock provider that always returns an error. Used for health
    /// degradation testing.
    struct ErrorProvider {
        name: String,
        tags: Vec<Tag>,
        error: ProviderError,
    }

    #[async_trait]
    impl Provider for ErrorProvider {
        fn name(&self) -> &str {
            &self.name
        }

        fn tags(&self) -> &[Tag] {
            &self.tags
        }

        fn description(&self) -> &str {
            "Mock error provider for health degradation tests"
        }

        fn is_available(&self) -> bool {
            true
        }

        async fn search(&self, _request: &SearchRequest) -> Result<SearchResponse, ProviderError> {
            Err(self.error.clone())
        }
    }

    /// Serialize access to the temp directory so SQLite doesn't clash.
    static DB_MUTEX: Mutex<()> = Mutex::new(());

    fn test_request(query: &str) -> SearchRequest {
        SearchRequest {
            request_id: uuid::Uuid::new_v4().to_string(),
            query: query.into(),
            limit: 10,
            providers: Vec::new(),
            language: None,
            country: None,
            safe_search: true,
            freshness: None,
            dispatch_mode: None,
        }
    }

    fn test_config(temp_dir: &tempfile::TempDir) -> Config {
        let db_path = temp_dir
            .path()
            .join("test_cache.db")
            .to_string_lossy()
            .to_string();

        let mut config = Config::default();
        config.cache.db_path = db_path;
        config.cache.default_ttl_secs = 3600;
        config.search.request_timeout_secs = 10;
        config.providers = vec![];
        config
    }

    // -----------------------------------------------------------------------
    // Health tracking tests
    // -----------------------------------------------------------------------

    #[test]
    fn health_state_starts_healthy() {
        let state = HealthState::new(3);
        assert!(state.is_healthy());
        assert_eq!(state.status(), HealthStatus::Healthy);
        assert!((state.health_score() - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn health_score_computes_success_rate() {
        let mut state = HealthState::new(3);

        state.record_success();
        state.record_failure();
        state.record_success();
        state.record_failure();

        // 2 successes out of 4 = 0.5
        assert!((state.health_score() - 0.5).abs() < f64::EPSILON);
    }

    #[test]
    fn health_score_drops_to_0_5_after_50_percent_failures() {
        let mut state = HealthState::new(3);

        // 2 successes, 2 failures within the window
        for _ in 0..2 {
            state.record_success();
        }
        for _ in 0..2 {
            state.record_failure();
        }

        assert!((state.health_score() - 0.5).abs() < f64::EPSILON);
        // 50% is not below 0.5, and consecutive failures (2) < threshold (3),
        // so still healthy.
        assert!(state.is_healthy());
        assert_eq!(state.status(), HealthStatus::Healthy);
    }

    #[test]
    fn consecutive_failures_marks_unhealthy() {
        let mut state = HealthState::new(3);

        state.record_failure();
        state.record_failure();
        state.record_failure();

        assert_eq!(state.status(), HealthStatus::Unhealthy);
        assert!(!state.is_healthy());
        assert_eq!(state.consecutive_failures(), 3);
    }

    #[test]
    fn success_recovers_unhealthy_provider() {
        let mut state = HealthState::new(3);

        // Make it unhealthy.
        for _ in 0..3 {
            state.record_failure();
        }
        assert_eq!(state.status(), HealthStatus::Unhealthy);

        // A success should recover.
        state.record_success();
        assert_eq!(state.status(), HealthStatus::Healthy);
        assert!(state.is_healthy());
        assert_eq!(state.consecutive_failures(), 0);
    }

    #[test]
    fn health_score_below_0_5_marks_degraded() {
        let mut state = HealthState::new(5);

        // 1 success, 3 failures = 25% success rate
        state.record_success();
        for _ in 0..3 {
            state.record_failure();
        }

        assert!((state.health_score() - 0.25).abs() < 0.01);
        assert_eq!(state.status(), HealthStatus::Degraded);
        // Still healthy for dispatch purposes (consecutive is only 3, threshold is 5).
        // But score < 0.5, so is_healthy returns false due to score check.
        assert!(!state.is_healthy());
    }

    #[test]
    fn health_score_defaults_to_1_0_with_no_events() {
        let state = HealthState::new(3);
        assert!((state.health_score() - 1.0).abs() < f64::EPSILON);
        assert!(state.is_healthy());
    }

    // -----------------------------------------------------------------------
    // Provider catalog tests
    // -----------------------------------------------------------------------

    #[test]
    fn catalog_register_and_lookup() {
        let mut catalog = ProviderCatalog::new();
        let provider = Box::new(SleepProvider {
            name: "test-provider".into(),
            tags: vec![Tag::Web],
            sleep_ms: 0,
            result_count: 5,
        });

        catalog.register(provider);
        assert_eq!(catalog.len(), 1);

        let enabled = catalog.enabled_providers(None, None);
        assert_eq!(enabled.len(), 1);
        assert_eq!(enabled[0].name(), "test-provider");

        // Also test direct lookup.
        let found = catalog.get("test-provider");
        assert!(found.is_some());
        assert_eq!(found.unwrap().name(), "test-provider");

        // Missing provider returns None.
        assert!(catalog.get("nonexistent").is_none());
    }

    #[test]
    fn catalog_filters_by_tag() {
        let mut catalog = ProviderCatalog::new();
        catalog.register(Box::new(SleepProvider {
            name: "web-provider".into(),
            tags: vec![Tag::Web],
            sleep_ms: 0,
            result_count: 5,
        }));
        catalog.register(Box::new(SleepProvider {
            name: "news-provider".into(),
            tags: vec![Tag::News],
            sleep_ms: 0,
            result_count: 5,
        }));

        let web = catalog.enabled_providers(None, Some(Tag::Web));
        assert_eq!(web.len(), 1);
        assert_eq!(web[0].name(), "web-provider");

        let news = catalog.enabled_providers(None, Some(Tag::News));
        assert_eq!(news.len(), 1);
        assert_eq!(news[0].name(), "news-provider");
    }

    #[test]
    fn catalog_filters_by_name_list() {
        let mut catalog = ProviderCatalog::new();
        catalog.register(Box::new(SleepProvider {
            name: "a".into(),
            tags: vec![Tag::Web],
            sleep_ms: 0,
            result_count: 5,
        }));
        catalog.register(Box::new(SleepProvider {
            name: "b".into(),
            tags: vec![Tag::Web],
            sleep_ms: 0,
            result_count: 5,
        }));

        let filtered = catalog.enabled_providers(Some(&["a".to_string()]), None);
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].name(), "a");
    }

    // -----------------------------------------------------------------------
    // Manager dispatch tests
    // -----------------------------------------------------------------------

    /// Verify that concurrent dispatch fires multiple providers in parallel.
    /// We use sleep providers with 100ms each; if they run serially, total
    /// would be 200ms+. In parallel, it should be ~100ms.
    #[tokio::test]
    async fn concurrent_dispatch_runs_in_parallel() {
        let dir = tempfile::tempdir().expect("create temp dir");
        let mut config = test_config(&dir);
        config.providers = vec![
            CfgProviderConfig {
                name: "fast-a".into(),
                enabled: true,
                ..Default::default()
            },
            CfgProviderConfig {
                name: "fast-b".into(),
                enabled: true,
                ..Default::default()
            },
        ];

        let _guard = DB_MUTEX.lock().unwrap_or_else(|e| e.into_inner());
        let cache = Arc::new(Cache::open(&config.cache.db_path).expect("open cache"));
        let rate_limiter = Arc::new(RateLimiter::new());
        let retry_config = RetryConfig::from(&config.anti_blocking);

        let mut catalog = ProviderCatalog::new();
        catalog.register(Box::new(SleepProvider {
            name: "fast-a".into(),
            tags: vec![Tag::Web],
            sleep_ms: 100,
            result_count: 3,
        }));
        catalog.register(Box::new(SleepProvider {
            name: "fast-b".into(),
            tags: vec![Tag::Web],
            sleep_ms: 100,
            result_count: 3,
        }));

        let mut health_states = HashMap::new();
        health_states.insert("fast-a".into(), HealthState::new(3));
        health_states.insert("fast-b".into(), HealthState::new(3));

        let manager = Manager {
            catalog: Arc::new(catalog),
            cache,
            rate_limiter,
            retry_config,
            health_states: Arc::new(RwLock::new(health_states)),
            request_timeout_secs: 10,
            dispatch_mode: "concurrent".into(),
            default_limit: 10,
            provider_rpm: [("fast-a".into(), 100), ("fast-b".into(), 100)].into(),
            cache_ttl_secs: 3600,
            ranking: RankingConfig::default(),
            provider_weights: HashMap::new(),
        };

        let request = test_request("parallel test");
        let start = Instant::now();
        let response = manager
            .search_concurrent(&request)
            .await
            .expect("concurrent dispatch succeeds");
        let elapsed = start.elapsed();

        // Total results: 3 + 3 = 6 from both providers.
        assert_eq!(response.results.len(), 6);

        // Parallel execution: total time should be close to one provider's sleep
        // (100ms) plus overhead, not 200ms+ (which would indicate serial).
        assert!(
            elapsed < Duration::from_millis(300),
            "parallel dispatch took {:?}, expected < 300ms",
            elapsed
        );
    }

    /// Verify that error providers cause health degradation.
    #[tokio::test]
    async fn error_provider_degrades_health() {
        let dir = tempfile::tempdir().expect("create temp dir");
        let mut config = test_config(&dir);
        config.providers = vec![CfgProviderConfig {
            name: "error-prv".into(),
            enabled: true,
            ..Default::default()
        }];

        let _guard = DB_MUTEX.lock().unwrap_or_else(|e| e.into_inner());
        let cache = Arc::new(Cache::open(&config.cache.db_path).expect("open cache"));
        let rate_limiter = Arc::new(RateLimiter::new());
        let retry_config = RetryConfig {
            base_delay_secs: 0, // Fast retry for tests
            max_attempts: 1,    // Don't retry
            cap_secs: 1,
        };

        let mut catalog = ProviderCatalog::new();
        catalog.register(Box::new(ErrorProvider {
            name: "error-prv".into(),
            tags: vec![Tag::Web],
            error: ProviderError::Timeout { elapsed_secs: 5 },
        }));

        let mut health_states = HashMap::new();
        health_states.insert("error-prv".into(), HealthState::new(3));

        let health_states = Arc::new(RwLock::new(health_states));

        let manager = Manager {
            catalog: Arc::new(catalog),
            cache,
            rate_limiter,
            retry_config,
            health_states: Arc::clone(&health_states),
            request_timeout_secs: 10,
            dispatch_mode: "concurrent".into(),
            default_limit: 10,
            provider_rpm: [("error-prv".into(), 100)].into(),
            cache_ttl_secs: 3600,
            ranking: RankingConfig::default(),
            provider_weights: HashMap::new(),
        };

        let request = test_request("error test");

        // First failure — still healthy (1 < threshold 3, and only 1 event so
        // health score degradation is not evaluated yet).
        let _ = manager.search_concurrent(&request).await;
        {
            let hs = health_states.read().await;
            let state = hs.get("error-prv").expect("health state exists");
            assert_eq!(state.consecutive_failures(), 1);
            assert_eq!(state.status(), HealthStatus::Healthy);
            assert!(state.is_healthy());
        }

        // Second failure.
        let _ = manager.search_concurrent(&request).await;
        {
            let hs = health_states.read().await;
            let state = hs.get("error-prv").expect("health state exists");
            assert_eq!(state.consecutive_failures(), 2);
            assert_eq!(state.status(), HealthStatus::Healthy);
            assert!(state.is_healthy());
        }

        // Third failure — should trigger unhealthy.
        let _ = manager.search_concurrent(&request).await;
        {
            let hs = health_states.read().await;
            let state = hs.get("error-prv").expect("health state exists");
            assert_eq!(state.consecutive_failures(), 3);
            assert_eq!(state.status(), HealthStatus::Unhealthy);
            assert!(!state.is_healthy());
        }

        // Fourth call: provider should be skipped (unhealthy).
        let response = manager.search_concurrent(&request).await;
        // No providers available, so empty response.
        assert!(response.is_ok());
        assert_eq!(response.unwrap().results.len(), 0);
    }

    /// Verify that duplicate concurrent queries don't deadlock on shared state.
    #[tokio::test]
    async fn concurrent_identical_queries_no_deadlock() {
        let dir = tempfile::tempdir().expect("create temp dir");
        let mut config = test_config(&dir);
        config.providers = vec![CfgProviderConfig {
            name: "concurrent-prv".into(),
            enabled: true,
            ..Default::default()
        }];

        let _guard = DB_MUTEX.lock().unwrap_or_else(|e| e.into_inner());
        let cache = Arc::new(Cache::open(&config.cache.db_path).expect("open cache"));
        let rate_limiter = Arc::new(RateLimiter::new());
        let retry_config = RetryConfig::from(&config.anti_blocking);

        let mut catalog = ProviderCatalog::new();
        catalog.register(Box::new(SleepProvider {
            name: "concurrent-prv".into(),
            tags: vec![Tag::Web],
            sleep_ms: 20,
            result_count: 1,
        }));

        let mut health_states = HashMap::new();
        health_states.insert("concurrent-prv".into(), HealthState::new(3));

        let manager = Arc::new(Manager {
            catalog: Arc::new(catalog),
            cache,
            rate_limiter,
            retry_config,
            health_states: Arc::new(RwLock::new(health_states)),
            request_timeout_secs: 10,
            dispatch_mode: "concurrent".into(),
            default_limit: 10,
            provider_rpm: [("concurrent-prv".into(), 100)].into(),
            cache_ttl_secs: 3600,
            ranking: RankingConfig::default(),
            provider_weights: HashMap::new(),
        });

        // Fire 5 identical concurrent queries.
        let mut handles = Vec::new();
        for _ in 0..5 {
            let mgr = Arc::clone(&manager);
            let req = test_request("same query");
            handles.push(tokio::spawn(
                async move { mgr.search_concurrent(&req).await },
            ));
        }

        // All should complete without hanging.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        for handle in handles {
            match tokio::time::timeout(
                deadline.saturating_duration_since(tokio::time::Instant::now()),
                handle,
            )
            .await
            {
                Ok(Ok(Ok(response))) => {
                    assert!(!response.results.is_empty());
                }
                Ok(Ok(Err(e))) => {
                    panic!("query failed with: {}", e);
                }
                Ok(Err(join_err)) => {
                    panic!("task panicked: {}", join_err);
                }
                Err(_) => {
                    panic!("timeout: concurrent queries deadlocked");
                }
            }
        }
    }

    /// Playwright-tagged providers are excluded from concurrent dispatch.
    #[tokio::test]
    async fn playwright_excluded_from_concurrent_dispatch() {
        let dir = tempfile::tempdir().expect("create temp dir");
        let config = test_config(&dir);

        let _guard = DB_MUTEX.lock().unwrap_or_else(|e| e.into_inner());
        let cache = Arc::new(Cache::open(&config.cache.db_path).expect("open cache"));
        let rate_limiter = Arc::new(RateLimiter::new());
        let retry_config = RetryConfig::from(&config.anti_blocking);

        let mut catalog = ProviderCatalog::new();
        catalog.register(Box::new(SleepProvider {
            name: "web-prv".into(),
            tags: vec![Tag::Web],
            sleep_ms: 10,
            result_count: 1,
        }));
        catalog.register(Box::new(SleepProvider {
            name: "playwright-prv".into(),
            tags: vec![Tag::Playwright],
            sleep_ms: 10,
            result_count: 1,
        }));

        let mut health_states = HashMap::new();
        health_states.insert("web-prv".into(), HealthState::new(3));
        health_states.insert("playwright-prv".into(), HealthState::new(3));

        let manager = Manager {
            catalog: Arc::new(catalog),
            cache,
            rate_limiter,
            retry_config,
            health_states: Arc::new(RwLock::new(health_states)),
            request_timeout_secs: 10,
            dispatch_mode: "concurrent".into(),
            default_limit: 10,
            provider_rpm: [("web-prv".into(), 100), ("playwright-prv".into(), 100)].into(),
            cache_ttl_secs: 3600,
            ranking: RankingConfig::default(),
            provider_weights: HashMap::new(),
        };

        let request = test_request("playwright exclusion");
        let response = manager
            .search_concurrent(&request)
            .await
            .expect("dispatch succeeds");

        // Only web-prv's results should be present, playwright-prv should be excluded.
        let provider_names: Vec<&str> = response
            .results
            .iter()
            .map(|r| r.provider_name.as_str())
            .collect();
        assert!(provider_names.iter().all(|n| *n == "web-prv"));
        assert!(!provider_names.contains(&"playwright-prv"));
    }

    /// Cache hit should skip all provider dispatch.
    #[tokio::test]
    async fn cache_hit_skips_provider_dispatch() {
        let dir = tempfile::tempdir().expect("create temp dir");
        let mut config = test_config(&dir);
        config.providers = vec![CfgProviderConfig {
            name: "cached-prv".into(),
            enabled: true,
            ..Default::default()
        }];

        let _guard = DB_MUTEX.lock().unwrap_or_else(|e| e.into_inner());
        let cache = Arc::new(Cache::open(&config.cache.db_path).expect("open cache"));
        let rate_limiter = Arc::new(RateLimiter::new());
        let retry_config = RetryConfig::from(&config.anti_blocking);

        let mut catalog = ProviderCatalog::new();
        catalog.register(Box::new(SleepProvider {
            name: "cached-prv".into(),
            tags: vec![Tag::Web],
            sleep_ms: 10,
            result_count: 1,
        }));

        let mut health_states = HashMap::new();
        health_states.insert("cached-prv".into(), HealthState::new(3));

        let manager = Manager {
            catalog: Arc::new(catalog),
            cache: Arc::clone(&cache),
            rate_limiter,
            retry_config,
            health_states: Arc::new(RwLock::new(health_states)),
            request_timeout_secs: 10,
            dispatch_mode: "concurrent".into(),
            default_limit: 10,
            provider_rpm: [("cached-prv".into(), 100)].into(),
            cache_ttl_secs: 3600,
            ranking: RankingConfig::default(),
            provider_weights: HashMap::new(),
        };

        let request = test_request("cache hit test");

        // First call: miss, populates cache via combined key.
        let resp1 = manager
            .search_concurrent(&request)
            .await
            .expect("first search succeeds");
        assert!(!resp1.cache_hit);

        // Second call: should hit the combined cache.
        let resp2 = manager
            .search_concurrent(&request)
            .await
            .expect("second search succeeds");
        assert!(resp2.cache_hit, "second call should be a cache hit");
    }

    /// Tiered dispatch should stop when cumulative results >= limit.
    #[tokio::test]
    async fn tiered_dispatch_stops_early() {
        let dir = tempfile::tempdir().expect("create temp dir");
        let config = test_config(&dir);

        let _guard = DB_MUTEX.lock().unwrap_or_else(|e| e.into_inner());
        let cache = Arc::new(Cache::open(&config.cache.db_path).expect("open cache"));
        let rate_limiter = Arc::new(RateLimiter::new());
        let retry_config = RetryConfig::from(&config.anti_blocking);

        let mut catalog = ProviderCatalog::new();
        catalog.register(Box::new(SleepProvider {
            name: "tier-a".into(),
            tags: vec![Tag::Web],
            sleep_ms: 10,
            result_count: 3,
        }));
        catalog.register(Box::new(SleepProvider {
            name: "tier-b".into(),
            tags: vec![Tag::News],
            sleep_ms: 10,
            result_count: 3,
        }));

        let mut health_states = HashMap::new();
        health_states.insert("tier-a".into(), HealthState::new(3));
        health_states.insert("tier-b".into(), HealthState::new(3));

        let manager = Manager {
            catalog: Arc::new(catalog),
            cache,
            rate_limiter,
            retry_config,
            health_states: Arc::new(RwLock::new(health_states)),
            request_timeout_secs: 10,
            dispatch_mode: "tiered".into(),
            default_limit: 3, // Only need 3 results total
            provider_rpm: [("tier-a".into(), 100), ("tier-b".into(), 100)].into(),
            cache_ttl_secs: 3600,
            ranking: RankingConfig::default(),
            provider_weights: HashMap::new(),
        };

        let mut request = test_request("tiered test");
        request.limit = 3;
        request.dispatch_mode = Some("tiered".into());

        let response = manager
            .search_tiered(&request)
            .await
            .expect("tiered dispatch succeeds");

        // tier-a (Web, priority 0) produces 3 results, meeting the limit of 3.
        // tier-b (News, priority 1) should be skipped by early-stop.
        assert!(response.results.len() <= 3);
        assert_eq!(
            response.providers_used,
            vec!["tier-a".to_string()],
            "tier-b should not have been dispatched"
        );
    }

    /// Verify Manager::new correctly constructs DuckDuckGo providers from config.
    #[tokio::test]
    async fn manager_new_registers_duckduckgo() {
        let dir = tempfile::tempdir().expect("create temp dir");
        let _guard = DB_MUTEX.lock().unwrap_or_else(|e| e.into_inner());

        let mut config = test_config(&dir);
        config.providers = vec![CfgProviderConfig {
            name: "duckduckgo".into(),
            enabled: true,
            rate_limit_rpm: Some(30),
            ..Default::default()
        }];

        let manager = Manager::new(&config).await.expect("create manager");
        assert_eq!(manager.catalog.len(), 1);

        let providers = manager.catalog.enabled_providers(None, None);
        assert_eq!(providers.len(), 1);
        assert_eq!(providers[0].name(), "duckduckgo");
        assert!(providers[0].is_available());
    }
}
