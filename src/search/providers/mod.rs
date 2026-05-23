//! Search provider implementations.
//!
//! Each submodule implements the [`Provider`](crate::search::Provider) trait for
//! a specific search backend (DuckDuckGo HTML scraping, Jina AI API, Brave Search API, SearXNG JSON API, etc.).

pub mod brave;
pub mod duckduckgo;
pub mod jina;
pub mod searxng;
