//! SQLite-backed cache implementation with WAL mode, TTL-based eviction, and
//! corruption recovery.
//!
//! The [`Cache`] struct wraps a `rusqlite::Connection` behind a `std::sync::Mutex`
//! so it can be shared across async tasks. All SQLite operations are brief
//! (sub-millisecond in WAL mode), so blocking the mutex is acceptable inside
//! a tokio runtime.
//!
//! # Schema
//!
//! ```sql
//! CREATE TABLE IF NOT EXISTS cache (
//!     key TEXT PRIMARY KEY,
//!     query TEXT NOT NULL,
//!     provider_list TEXT NOT NULL,
//!     response_json TEXT NOT NULL,
//!     created_at INTEGER NOT NULL,
//!     ttl_secs INTEGER NOT NULL
//! );
//! CREATE INDEX IF NOT EXISTS idx_cache_created_at ON cache(created_at);
//! PRAGMA journal_mode=WAL;
//! ```

use super::CacheStats;
use crate::search::SearchResponse;
use anyhow::{Context, Result};
use rusqlite::params;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

/// A thread-safe, SQLite-backed search result cache.
///
/// Hit and miss counters are stored in atomic integers so `stats()` can read
/// them without locking the SQLite connection.
pub struct Cache {
    conn: Mutex<rusqlite::Connection>,
    hits: AtomicU64,
    misses: AtomicU64,
}

impl Cache {
    // ------------------------------------------------------------------
    // Construction
    // ------------------------------------------------------------------

    /// Open or create the cache database at `db_path`.
    ///
    /// Enables WAL journal mode for concurrent read performance and runs the
    /// schema migration. If the database file is corrupt (SQLITE_CORRUPT or
    /// SQLITE_NOTADB), the file is deleted and a fresh database is created
    /// automatically, logging a warning.
    ///
    /// # Errors
    ///
    /// Returns an error if the database cannot be opened after a corruption
    /// recovery attempt, or if the directory cannot be created.
    pub fn open(db_path: &str) -> Result<Cache> {
        match Self::try_open(db_path) {
            Ok(cache) => Ok(cache),
            Err(e) => {
                if is_corruption_error(&e) {
                    tracing::warn!(
                        "Cache database at '{}' appears corrupt ({}); deleting and creating fresh",
                        db_path,
                        e
                    );
                    if let Err(rm_err) = std::fs::remove_file(db_path) {
                        if rm_err.kind() != std::io::ErrorKind::NotFound {
                            tracing::warn!(
                                "Failed to remove corrupt cache database '{}': {}",
                                db_path,
                                rm_err
                            );
                        }
                    }
                    Self::try_open(db_path)
                } else {
                    Err(e)
                }
            }
        }
    }

    /// Attempt to open and initialize the database. Factored out so that
    /// `open()` can retry after deleting a corrupt file.
    fn try_open(db_path: &str) -> Result<Cache> {
        // Ensure the parent directory exists.
        if let Some(parent) = Path::new(db_path).parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("Failed to create cache directory: {}", parent.display()))?;
        }

        let conn = rusqlite::Connection::open(db_path)
            .with_context(|| format!("Failed to open cache database at '{}'", db_path))?;

        // Enable WAL mode for better concurrent read performance.
        conn.execute_batch("PRAGMA journal_mode=WAL;")
            .context("Failed to enable WAL mode")?;

        // Run schema migration.
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS cache (
                key TEXT PRIMARY KEY,
                query TEXT NOT NULL,
                provider_list TEXT NOT NULL,
                response_json TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                ttl_secs INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_cache_created_at ON cache(created_at);",
        )
        .context("Failed to run cache schema migration")?;

        tracing::info!(
            "Cache database opened at '{}' (WAL mode, {} entries)",
            db_path,
            conn.query_row("SELECT COUNT(*) FROM cache", [], |r| r.get::<_, i64>(0))
                .unwrap_or(0)
        );

        Ok(Cache {
            conn: Mutex::new(conn),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        })
    }

    // ------------------------------------------------------------------
    // Lookup
    // ------------------------------------------------------------------

    /// Retrieve a cached `SearchResponse` by its cache key.
    ///
    /// Returns `Ok(None)` on a cache miss (key not found or entry expired).
    /// Returns `Ok(Some(response))` on a cache hit. The `cache_hit` field on
    /// the returned response is forced to `true`.
    ///
    /// Hit and miss atomic counters are updated accordingly.
    pub fn get(&self, key: &str) -> Result<Option<SearchResponse>> {
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());

        let mut stmt = conn
            .prepare("SELECT response_json, created_at, ttl_secs FROM cache WHERE key = ?1")?;

        let row: Option<(String, i64, u64)> = stmt
            .query_row(params![key], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, u64>(2)?,
                ))
            })
            .optional()?;

        match row {
            None => {
                self.misses.fetch_add(1, Ordering::Relaxed);
                Ok(None)
            }
            Some((response_json, created_at, ttl_secs)) => {
                let now = now_secs();

                // Check TTL expiration.
                if created_at.saturating_add_unsigned(ttl_secs) <= now {
                    self.misses.fetch_add(1, Ordering::Relaxed);
                    return Ok(None);
                }

                let mut response: SearchResponse = serde_json::from_str(&response_json)
                    .with_context(|| format!("Failed to deserialize cached response for key '{}'", key))?;

                // Ensure the cache_hit flag is set on the way out.
                response.cache_hit = true;

                self.hits.fetch_add(1, Ordering::Relaxed);
                Ok(Some(response))
            }
        }
    }

    // ------------------------------------------------------------------
    // Insert / replace
    // ------------------------------------------------------------------

    /// Store a search response in the cache.
    ///
    /// Uses `INSERT OR REPLACE` so concurrent identical queries that both miss
    /// and both attempt to insert are idempotent — the second write simply
    /// replaces the first.
    ///
    /// The `query` and `provider_list` parameters are stored alongside the
    /// serialized response for inspection and filtered eviction.
    pub fn set(
        &self,
        key: &str,
        query: &str,
        provider_list: &str,
        response: &SearchResponse,
        ttl_secs: u64,
    ) -> Result<()> {
        let response_json =
            serde_json::to_string(response).context("Failed to serialize response for cache")?;

        let now = now_secs();

        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        conn.execute(
            "INSERT OR REPLACE INTO cache (key, query, provider_list, response_json, created_at, ttl_secs)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![key, query, provider_list, response_json, now, ttl_secs],
        )
        .context("Failed to insert cache entry")?;

        Ok(())
    }

    // ------------------------------------------------------------------
    // Eviction
    // ------------------------------------------------------------------

    /// Delete all expired entries from the cache.
    ///
    /// Returns the number of rows removed.
    ///
    /// An entry is expired when `created_at + ttl_secs <= current_time`.
    pub fn evict_expired(&self) -> Result<usize> {
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let now = now_secs();

        let removed = conn
            .execute(
                "DELETE FROM cache WHERE created_at + ttl_secs <= ?1",
                params![now],
            )
            .context("Failed to evict expired cache entries")?;

        if removed > 0 {
            tracing::debug!("Evicted {} expired cache entries", removed);
        }

        Ok(removed)
    }

    // ------------------------------------------------------------------
    // Clear
    // ------------------------------------------------------------------

    /// Clear cache entries, optionally filtered by provider name.
    ///
    /// When `provider_filter` is `None`, all entries are deleted. When
    /// `Some(provider)`, only entries whose `provider_list` contains the
    /// given provider name are deleted.
    ///
    /// Returns the number of rows removed.
    pub fn clear(&self, provider_filter: Option<&str>) -> Result<usize> {
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());

        let removed = match provider_filter {
            None => conn
                .execute("DELETE FROM cache", [])
                .context("Failed to clear all cache entries")?,
            Some(provider) => {
                // Match the provider name exactly, or as part of a comma-separated
                // list. The four LIKE patterns cover: sole entry, first, last, middle.
                let pattern_sole = provider.to_string();
                let pattern_first = format!("{},%", provider);
                let pattern_last = format!("%,{}", provider);
                let pattern_middle = format!("%,{},%", provider);
                conn.execute(
                    "DELETE FROM cache WHERE provider_list = ?1 OR provider_list LIKE ?2 OR provider_list LIKE ?3 OR provider_list LIKE ?4",
                    params![pattern_sole, pattern_first, pattern_last, pattern_middle],
                )
                .context("Failed to clear cache entries for provider")?
            }
        };

        if removed > 0 {
            tracing::info!(
                "Cleared {} cache entries{}",
                removed,
                provider_filter
                    .map(|p| format!(" for provider '{}'", p))
                    .unwrap_or_default()
            );
        }

        Ok(removed)
    }

    // ------------------------------------------------------------------
    // Statistics
    // ------------------------------------------------------------------

    /// Return current cache statistics.
    ///
    /// Hit and miss counters are read from atomic variables without locking
    /// the SQLite connection. Entry count and total size require a brief
    /// query against the database.
    pub fn stats(&self) -> CacheStats {
        let hits = self.hits.load(Ordering::Relaxed);
        let misses = self.misses.load(Ordering::Relaxed);
        let total = hits + misses;

        let (total_entries, total_size_bytes) = {
            let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
            let entries = conn
                .query_row("SELECT COUNT(*) FROM cache", [], |r| r.get::<_, i64>(0))
                .unwrap_or(0) as u64;
            let size = conn
                .query_row(
                    "SELECT COALESCE(SUM(LENGTH(response_json)), 0) FROM cache",
                    [],
                    |r| r.get::<_, i64>(0),
                )
                .unwrap_or(0) as u64;
            (entries, size)
        };

        let hit_rate = if total > 0 {
            hits as f64 / total as f64
        } else {
            0.0
        };

        CacheStats {
            total_entries,
            total_size_bytes,
            hit_count: hits,
            miss_count: misses,
            hit_rate,
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Current Unix epoch time in seconds.
fn now_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

/// Check whether a rusqlite error indicates database corruption.
fn is_corruption_error(err: &anyhow::Error) -> bool {
    if let Some(rusqlite_err) = err.downcast_ref::<rusqlite::Error>() {
        match rusqlite_err.sqlite_error_code() {
            Some(rusqlite::ErrorCode::DatabaseCorrupt)
            | Some(rusqlite::ErrorCode::NotADatabase) => true,
            _ => false,
        }
    } else {
        // Also check the chain for rusqlite errors.
        for cause in err.chain() {
            if let Some(rusqlite_err) = cause.downcast_ref::<rusqlite::Error>() {
                match rusqlite_err.sqlite_error_code() {
                    Some(rusqlite::ErrorCode::DatabaseCorrupt)
                    | Some(rusqlite::ErrorCode::NotADatabase) => return true,
                    _ => {}
                }
            }
        }
        false
    }
}

// ---------------------------------------------------------------------------
// Extension trait: optional rows from rusqlite
// ---------------------------------------------------------------------------

trait OptionalExt<T> {
    fn optional(self) -> rusqlite::Result<Option<T>>;
}

impl<T> OptionalExt<T> for rusqlite::Result<T> {
    fn optional(self) -> rusqlite::Result<Option<T>> {
        match self {
            Ok(v) => Ok(Some(v)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e),
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::search::{SearchResponse, SearchResult};
    use std::sync::Arc;
    use tempfile::TempDir;

    /// Build a minimal `SearchResponse` for test purposes.
    fn test_response(request_id: &str, provider: &str) -> SearchResponse {
        SearchResponse {
            request_id: request_id.into(),
            results: vec![SearchResult {
                title: "Test result".into(),
                url: "https://example.com".into(),
                snippet: "A test snippet".into(),
                published_date: None,
                provider_name: provider.into(),
                rank_score: 0.9,
            }],
            total_found: 1,
            providers_used: vec![provider.into()],
            cache_hit: false,
            elapsed_ms: 100,
        }
    }

    fn open_temp_cache() -> (TempDir, Cache) {
        let dir = tempfile::tempdir().expect("create temp dir");
        let db_path = dir.path().join("cache.db");
        let cache = Cache::open(&db_path.to_string_lossy()).expect("open cache");
        (dir, cache)
    }

    // ------------------------------------------------------------------
    // Basic get / set
    // ------------------------------------------------------------------

    #[test]
    fn get_miss_returns_none() {
        let (_dir, cache) = open_temp_cache();
        let result = cache.get("nonexistent-key").expect("get succeeds");
        assert!(result.is_none());
    }

    #[test]
    fn set_and_get_roundtrip() {
        let (_dir, cache) = open_temp_cache();
        let response = test_response("req-1", "duckduckgo");

        cache
            .set("key-1", "rust async", "duckduckgo", &response, 3600)
            .expect("set succeeds");

        let cached = cache.get("key-1").expect("get succeeds").expect("should hit");
        assert_eq!(cached.request_id, "req-1");
        assert_eq!(cached.providers_used, vec!["duckduckgo"]);
        assert_eq!(cached.results.len(), 1);
        assert_eq!(cached.results[0].title, "Test result");
        assert!(cached.cache_hit, "cache_hit flag should be true");
    }

    #[test]
    fn set_with_different_key_does_not_clobber() {
        let (_dir, cache) = open_temp_cache();
        let resp1 = test_response("req-1", "duckduckgo");
        let resp2 = test_response("req-2", "brave");

        cache
            .set("key-1", "rust", "duckduckgo", &resp1, 3600)
            .unwrap();
        cache
            .set("key-2", "rust", "brave", &resp2, 3600)
            .unwrap();

        let cached1 = cache.get("key-1").unwrap().unwrap();
        let cached2 = cache.get("key-2").unwrap().unwrap();
        assert_eq!(cached1.request_id, "req-1");
        assert_eq!(cached2.request_id, "req-2");
    }

    #[test]
    fn insert_or_replace_is_idempotent() {
        let (_dir, cache) = open_temp_cache();
        let resp1 = test_response("req-1", "ddg");
        let resp2 = test_response("req-2", "ddg");

        // Both "concurrent" writers use the same key.
        cache.set("dup", "test", "ddg", &resp1, 3600).unwrap();
        cache.set("dup", "test", "ddg", &resp2, 3600).unwrap();

        let cached = cache.get("dup").unwrap().unwrap();
        // Second write wins (INSERT OR REPLACE).
        assert_eq!(cached.request_id, "req-2");
    }

    // ------------------------------------------------------------------
    // TTL-based expiration
    // ------------------------------------------------------------------

    #[test]
    fn expired_entry_not_returned_by_get() {
        let (_dir, cache) = open_temp_cache();
        let response = test_response("req-1", "ddg");

        // Set with 0-second TTL — expires immediately.
        cache
            .set("exp-key", "query", "ddg", &response, 0)
            .unwrap();

        let result = cache.get("exp-key").unwrap();
        assert!(result.is_none(), "expired entry should be a miss");
    }

    #[test]
    fn evict_expired_removes_expired_rows() {
        let (_dir, cache) = open_temp_cache();
        let response = test_response("req-1", "ddg");

        cache
            .set("exp-1", "q1", "ddg", &response, 0)
            .unwrap();
        cache
            .set("exp-2", "q2", "ddg", &response, 3600)
            .unwrap();

        let removed = cache.evict_expired().expect("evict succeeds");
        assert_eq!(removed, 1);

        // exp-1 should be gone.
        assert!(cache.get("exp-1").unwrap().is_none());
        // exp-2 should still be present.
        assert!(cache.get("exp-2").unwrap().is_some());
    }

    // ------------------------------------------------------------------
    // Stats
    // ------------------------------------------------------------------

    #[test]
    fn stats_tracks_hits_and_misses() {
        let (_dir, cache) = open_temp_cache();
        let response = test_response("req-1", "ddg");

        cache.get("nope").unwrap(); // miss
        cache.get("nope").unwrap(); // miss
        cache
            .set("hit-key", "q", "ddg", &response, 3600)
            .unwrap();
        cache.get("hit-key").unwrap(); // hit
        cache.get("nope").unwrap(); // miss

        let stats = cache.stats();
        assert_eq!(stats.hit_count, 1);
        assert_eq!(stats.miss_count, 3);
        assert!(stats.total_entries >= 1);
        assert!(stats.total_size_bytes > 0);
        assert!((stats.hit_rate - 0.25).abs() < f64::EPSILON);
    }

    #[test]
    fn stats_hit_rate_zero_when_no_requests() {
        let (_dir, cache) = open_temp_cache();
        let stats = cache.stats();
        assert_eq!(stats.hit_count, 0);
        assert_eq!(stats.miss_count, 0);
        assert!((stats.hit_rate - 0.0).abs() < f64::EPSILON);
    }

    // ------------------------------------------------------------------
    // Clear
    // ------------------------------------------------------------------

    #[test]
    fn clear_all_removes_everything() {
        let (_dir, cache) = open_temp_cache();
        let resp = test_response("req-1", "ddg");

        cache.set("k1", "q1", "ddg", &resp, 3600).unwrap();
        cache.set("k2", "q2", "brave", &resp, 3600).unwrap();

        let removed = cache.clear(None).unwrap();
        assert_eq!(removed, 2);
        assert!(cache.get("k1").unwrap().is_none());
        assert!(cache.get("k2").unwrap().is_none());
    }

    #[test]
    fn clear_by_provider() {
        let (_dir, cache) = open_temp_cache();
        let resp = test_response("req-1", "ddg");

        cache.set("k1", "q1", "duckduckgo", &resp, 3600).unwrap();
        cache.set("k2", "q2", "brave", &resp, 3600).unwrap();

        let removed = cache.clear(Some("duckduckgo")).unwrap();
        assert_eq!(removed, 1);

        assert!(cache.get("k1").unwrap().is_none());
        assert!(cache.get("k2").unwrap().is_some());
    }

    #[test]
    fn clear_no_match_returns_zero() {
        let (_dir, cache) = open_temp_cache();
        let resp = test_response("req-1", "ddg");
        cache.set("k1", "q1", "duckduckgo", &resp, 3600).unwrap();

        let removed = cache.clear(Some("nonexistent")).unwrap();
        assert_eq!(removed, 0);
        assert!(cache.get("k1").unwrap().is_some());
    }

    // ------------------------------------------------------------------
    // Corruption recovery
    // ------------------------------------------------------------------

    #[test]
    fn corrupt_database_is_recreated() {
        let dir = tempfile::tempdir().expect("create temp dir");
        let db_path = dir.path().join("cache.db");

        // Write garbage into the file to simulate corruption.
        std::fs::write(&db_path, b"this is not a valid sqlite database").unwrap();

        // open() should detect corruption, delete the file, and create a fresh DB.
        let cache = Cache::open(&db_path.to_string_lossy()).expect("open should recover");
        let stats = cache.stats();
        assert_eq!(stats.total_entries, 0);

        // Verify the cache is functional after recovery.
        let response = test_response("req-1", "ddg");
        cache
            .set("post-recovery", "query", "ddg", &response, 3600)
            .unwrap();
        let cached = cache.get("post-recovery").unwrap().unwrap();
        assert_eq!(cached.request_id, "req-1");
    }

    #[test]
    fn open_with_nonexistent_directory() {
        let dir = tempfile::tempdir().expect("create temp dir");
        let db_path = dir
            .path()
            .join("deeply")
            .join("nested")
            .join("dir")
            .join("cache.db");

        let cache = Cache::open(&db_path.to_string_lossy()).expect("open should create dirs");
        let stats = cache.stats();
        assert_eq!(stats.total_entries, 0);
    }

    // ------------------------------------------------------------------
    // Thread safety
    // ------------------------------------------------------------------

    #[test]
    fn shared_cache_across_threads() {
        let (_dir, cache) = open_temp_cache();
        let cache = Arc::new(cache);
        let resp = test_response("req-1", "ddg");

        // Pre-populate one entry.
        cache
            .set("shared", "query", "ddg", &resp, 3600)
            .unwrap();

        let mut handles = vec![];
        for i in 0..4 {
            let c = Arc::clone(&cache);
            let r = resp.clone();
            handles.push(std::thread::spawn(move || {
                c.set(
                    &format!("thread-{}", i),
                    "query",
                    "ddg",
                    &r,
                    3600,
                )
                .unwrap();
                c.get("shared").unwrap().unwrap()
            }));
        }

        for h in handles {
            let cached = h.join().unwrap();
            assert_eq!(cached.request_id, "req-1");
            assert!(cached.cache_hit);
        }
    }
}
