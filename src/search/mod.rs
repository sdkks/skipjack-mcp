pub mod merge;
pub mod provider;
pub mod providers;
pub mod rank;
pub mod request;

pub use merge::ResultMerger;
pub use provider::{Provider, ProviderError, Tag};
pub use rank::{compute_composite_score, rank};
pub use request::{Freshness, SearchRequest, SearchResponse, SearchResult};

use std::collections::HashMap;

/// The provider catalog: a map from provider name to a boxed `Provider` trait object.
///
/// The catalog is populated at startup from the configuration file.
/// Each provider implementation is registered by name so the dispatch
/// engine can select providers by tag, tier, or explicit user request.
///
/// # Example
///
/// ```ignore
/// let mut catalog = ProviderCatalog::new();
/// catalog.insert("duckduckgo".to_string(), Box::new(DuckDuckGo::new(config)?));
/// ```
pub type ProviderCatalog = HashMap<String, Box<dyn Provider>>;
