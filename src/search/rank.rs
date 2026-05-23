//! Composite ranking algorithm for search results.
//!
//! Computes a composite score per result by summing enabled dimension
//! contributions from [`RankingConfig`]. The three implemented dimensions are:
//!
//! - **provider_reputation** — weighted by the provider's configured weight.
//! - **result_position** — higher positions (closer to top) score higher.
//! - **freshness_bonus** — recently published results get a boost.
//!
//! [`rank`] is the main entry point: it computes composite scores, assigns them
//! to each result's `rank_score`, and returns results sorted descending with
//! stable ordering for equal scores.

use std::collections::HashMap;

use crate::config::RankingConfig;
use crate::search::SearchResult;

/// Compute a composite ranking score for a single result.
///
/// Each enabled dimension in `config` contributes its `weight * factor` to the
/// total. Disabled dimensions add zero. The result is the sum of all
/// contributions.
///
/// # Arguments
///
/// * `result` — the search result to score.
/// * `position` — 0-indexed position within the result's provider (lower is better).
/// * `config` — the ranking configuration controlling which dimensions are active.
/// * `provider_weights` — per-provider weight map (defaults to 1.0 for missing providers).
pub fn compute_composite_score(
    result: &SearchResult,
    position: usize,
    config: &RankingConfig,
    provider_weights: &HashMap<String, f64>,
) -> f64 {
    let mut score = 0.0;

    // provider_reputation
    if config.provider_reputation.enabled {
        let provider_weight = provider_weights
            .get(&result.provider_name)
            .copied()
            .unwrap_or(1.0);
        score += config.provider_reputation.weight * provider_weight;
    }

    // result_position
    if config.result_position.enabled {
        score += config.result_position.weight * (1.0 / (1.0 + position as f64));
    }

    // freshness_bonus
    if config.freshness_bonus.enabled {
        let freshness_factor = match &result.published_date {
            Some(date_str) => {
                let parsed = chrono::NaiveDate::parse_from_str(date_str, "%Y-%m-%d").ok();
                match parsed {
                    Some(date) => {
                        let today = chrono::Utc::now().date_naive();
                        let age_days = (today - date).num_days();
                        if age_days >= 0 && (age_days as u32) <= config.freshness_bonus.window_days
                        {
                            1.0
                        } else {
                            0.0
                        }
                    }
                    None => 0.5, // unparseable date treated as missing
                }
            }
            None => 0.5, // missing date gets neutral factor
        };
        score += config.freshness_bonus.weight * freshness_factor;
    }

    score
}

/// Rank a vector of search results by computing composite scores and sorting
/// descending by `rank_score`.
///
/// Positions are tracked per-provider: the first result from a provider gets
/// position 0, the next gets position 1, etc. Results from the same provider
/// are assumed to arrive in provider order.
///
/// When two results have equal composite scores, their original input order is
/// preserved (stable sort).
///
/// # Returns
///
/// A new `Vec<SearchResult>` sorted by `rank_score` descending.
pub fn rank(
    mut results: Vec<SearchResult>,
    config: &RankingConfig,
    provider_weights: &HashMap<String, f64>,
) -> Vec<SearchResult> {
    let mut provider_positions: HashMap<String, usize> = HashMap::new();

    // Compute composite scores, tracking per-provider position.
    for result in &mut results {
        let pos = provider_positions
            .entry(result.provider_name.clone())
            .or_insert(0);
        result.rank_score = compute_composite_score(result, *pos, config, provider_weights);
        *pos += 1;
    }

    // Stable sort by rank_score descending; equal scores preserve input order.
    let mut indexed: Vec<(usize, SearchResult)> = results.into_iter().enumerate().collect();
    indexed.sort_by(|(ai, a), (bi, b)| {
        b.rank_score
            .partial_cmp(&a.rank_score)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| ai.cmp(bi))
    });

    indexed.into_iter().map(|(_, r)| r).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{FreshnessBonusDimension, RankingDimension};

    fn make_result(
        title: &str,
        provider: &str,
        published_date: Option<&str>,
        rank_score: f64,
    ) -> SearchResult {
        SearchResult {
            title: title.into(),
            url: format!("https://example.com/{}", title.to_lowercase()),
            snippet: "snippet".into(),
            published_date: published_date.map(|s| s.to_string()),
            provider_name: provider.into(),
            rank_score,
        }
    }

    /// Returns a config with all dimensions disabled — each test enables only what it needs.
    fn all_disabled_config() -> RankingConfig {
        RankingConfig {
            provider_reputation: RankingDimension {
                enabled: false,
                weight: 0.0,
            },
            result_position: RankingDimension {
                enabled: false,
                weight: 0.0,
            },
            freshness_bonus: FreshnessBonusDimension {
                enabled: false,
                weight: 0.0,
                window_days: 7,
            },
        }
    }

    fn empty_weights() -> HashMap<String, f64> {
        HashMap::new()
    }

    // ------------------------------------------------------------------
    // result_position scoring
    // ------------------------------------------------------------------

    #[test]
    fn position_scoring_pos0_equals_1() {
        let config = RankingConfig {
            result_position: RankingDimension {
                enabled: true,
                weight: 1.0,
            },
            ..all_disabled_config()
        };
        let result = make_result("test", "p", None, 0.0);
        let score = compute_composite_score(&result, 0, &config, &empty_weights());
        assert!((score - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn position_scoring_pos1_equals_0_5() {
        let config = RankingConfig {
            result_position: RankingDimension {
                enabled: true,
                weight: 1.0,
            },
            ..all_disabled_config()
        };
        let result = make_result("test", "p", None, 0.0);
        let score = compute_composite_score(&result, 1, &config, &empty_weights());
        assert!((score - 0.5).abs() < f64::EPSILON);
    }

    #[test]
    fn position_scoring_pos2_approx_0_333() {
        let config = RankingConfig {
            result_position: RankingDimension {
                enabled: true,
                weight: 1.0,
            },
            ..all_disabled_config()
        };
        let result = make_result("test", "p", None, 0.0);
        let score = compute_composite_score(&result, 2, &config, &empty_weights());
        let expected = 1.0 / 3.0;
        assert!((score - expected).abs() < 1e-10);
    }

    // ------------------------------------------------------------------
    // provider_reputation with weight
    // ------------------------------------------------------------------

    #[test]
    fn provider_reputation_with_weight() {
        let config = RankingConfig {
            provider_reputation: RankingDimension {
                enabled: true,
                weight: 2.0,
            },
            ..all_disabled_config()
        };
        let result = make_result("test", "brave", None, 0.0);
        let mut weights = HashMap::new();
        weights.insert("brave".to_string(), 1.5);
        let score = compute_composite_score(&result, 0, &config, &weights);
        assert!((score - 2.0 * 1.5).abs() < f64::EPSILON);
    }

    #[test]
    fn provider_reputation_defaults_to_1_when_missing() {
        let config = RankingConfig {
            provider_reputation: RankingDimension {
                enabled: true,
                weight: 1.0,
            },
            ..all_disabled_config()
        };
        let result = make_result("test", "unknown", None, 0.0);
        let score = compute_composite_score(&result, 0, &config, &empty_weights());
        assert!((score - 1.0).abs() < f64::EPSILON);
    }

    // ------------------------------------------------------------------
    // freshness_bonus
    // ------------------------------------------------------------------

    #[test]
    fn freshness_within_window_gets_1() {
        let today = chrono::Utc::now().date_naive();
        let config = RankingConfig {
            freshness_bonus: FreshnessBonusDimension {
                enabled: true,
                weight: 1.0,
                window_days: 7,
            },
            ..all_disabled_config()
        };
        let result = make_result(
            "test",
            "p",
            Some(&today.format("%Y-%m-%d").to_string()),
            0.0,
        );
        let score = compute_composite_score(&result, 0, &config, &empty_weights());
        assert!((score - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn freshness_outside_window_gets_0() {
        let old_date = chrono::Utc::now().date_naive() - chrono::Duration::days(30);
        let config = RankingConfig {
            freshness_bonus: FreshnessBonusDimension {
                enabled: true,
                weight: 1.0,
                window_days: 7,
            },
            ..all_disabled_config()
        };
        let result = make_result(
            "test",
            "p",
            Some(&old_date.format("%Y-%m-%d").to_string()),
            0.0,
        );
        let score = compute_composite_score(&result, 0, &config, &empty_weights());
        assert!((score - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn freshness_missing_date_gets_0_5() {
        let config = RankingConfig {
            freshness_bonus: FreshnessBonusDimension {
                enabled: true,
                weight: 1.0,
                window_days: 7,
            },
            ..all_disabled_config()
        };
        let result = make_result("test", "p", None, 0.0);
        let score = compute_composite_score(&result, 0, &config, &empty_weights());
        assert!((score - 0.5).abs() < f64::EPSILON);
    }

    // ------------------------------------------------------------------
    // disabled dimension contributes 0
    // ------------------------------------------------------------------

    #[test]
    fn disabled_dimension_contributes_zero() {
        let config = RankingConfig {
            result_position: RankingDimension {
                enabled: false,
                weight: 5.0,
            },
            ..all_disabled_config()
        };
        let result = make_result("test", "p", None, 0.0);
        let score = compute_composite_score(&result, 0, &config, &empty_weights());
        assert!((score - 0.0).abs() < f64::EPSILON);
    }

    // ------------------------------------------------------------------
    // stable sort for equal scores
    // ------------------------------------------------------------------

    #[test]
    fn stable_sort_for_equal_scores() {
        let config = all_disabled_config(); // all disabled → all scores = 0.0
        let results = vec![
            make_result("A", "p1", None, 0.0),
            make_result("B", "p2", None, 0.0),
            make_result("C", "p3", None, 0.0),
        ];
        let ranked = rank(results, &config, &empty_weights());
        // With all equal scores, original order must be preserved.
        assert_eq!(ranked[0].title, "A");
        assert_eq!(ranked[1].title, "B");
        assert_eq!(ranked[2].title, "C");
    }

    // ------------------------------------------------------------------
    // rank produces correct descending order
    // ------------------------------------------------------------------

    #[test]
    fn rank_produces_correct_descending_order() {
        let config = RankingConfig {
            result_position: RankingDimension {
                enabled: true,
                weight: 1.0,
            },
            ..all_disabled_config()
        };
        // Two results from same provider: pos 0 = 1.0, pos 1 = 0.5
        // Two results from another provider: pos 0 = 1.0
        let results = vec![
            make_result("A-pos0", "p1", None, 0.0),
            make_result("B-pos1", "p1", None, 0.0),
            make_result("C-pos0", "p2", None, 0.0),
        ];
        let ranked = rank(results, &config, &empty_weights());
        // A-pos0 and C-pos0 both score 1.0, A comes first (stable)
        // B-pos1 scores 0.5
        assert_eq!(ranked[0].title, "A-pos0");
        assert!((ranked[0].rank_score - 1.0).abs() < f64::EPSILON);
        assert_eq!(ranked[1].title, "C-pos0");
        assert!((ranked[1].rank_score - 1.0).abs() < f64::EPSILON);
        assert_eq!(ranked[2].title, "B-pos1");
        assert!((ranked[2].rank_score - 0.5).abs() < f64::EPSILON);
    }

    // ------------------------------------------------------------------
    // provider positions are tracked per-provider across gaps
    // ------------------------------------------------------------------

    #[test]
    fn per_provider_positions_are_independent() {
        let config = RankingConfig {
            result_position: RankingDimension {
                enabled: true,
                weight: 1.0,
            },
            ..all_disabled_config()
        };
        // Interleaved providers: p1, p2, p1
        let results = vec![
            make_result("p1-0", "p1", None, 0.0),
            make_result("p2-0", "p2", None, 0.0),
            make_result("p1-1", "p1", None, 0.0),
        ];
        let ranked = rank(results, &config, &empty_weights());
        // p1-0: pos 0 → 1.0
        // p2-0: pos 0 → 1.0
        // p1-1: pos 1 → 0.5
        // Sort: p1-0 (1.0), p2-0 (1.0) [stable order], p1-1 (0.5)
        assert_eq!(ranked[0].title, "p1-0");
        assert!((ranked[0].rank_score - 1.0).abs() < f64::EPSILON);
        assert_eq!(ranked[1].title, "p2-0");
        assert!((ranked[1].rank_score - 1.0).abs() < f64::EPSILON);
        assert_eq!(ranked[2].title, "p1-1");
        assert!((ranked[2].rank_score - 0.5).abs() < f64::EPSILON);
    }
}
