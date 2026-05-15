// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Specific options for the metric engine to create or open a region.

use std::collections::HashMap;

use store_api::metric_engine_consts::{
    MEMTABLE_PARTITION_TREE_PRIMARY_KEY_ENCODING,
    METRIC_ENGINE_INDEX_SKIPPING_INDEX_FALSE_POSITIVE_RATE_OPTION,
    METRIC_ENGINE_INDEX_SKIPPING_INDEX_FALSE_POSITIVE_RATE_OPTION_DEFAULT,
    METRIC_ENGINE_INDEX_SKIPPING_INDEX_GRANULARITY_OPTION,
    METRIC_ENGINE_INDEX_SKIPPING_INDEX_GRANULARITY_OPTION_DEFAULT, METRIC_ENGINE_INDEX_TYPE_OPTION,
    PRIMARY_KEY_ENCODING,
};
use store_api::mito_engine_options::{COMPACTION_TYPE, COMPACTION_TYPE_TWCS, TWCS_TIME_WINDOW};

/// Prefix for legacy `memtable.partition_tree.*` option keys. These keys are
/// silently dropped by the metric engine; the partition tree memtable is gone.
const LEGACY_PARTITION_TREE_OPTION_PREFIX: &str = "memtable.partition_tree.";

use crate::error::{Error, ParseRegionOptionsSnafu, Result};

/// The empirical value for the seg row count of the metric data region.
/// Compared to the mito engine, the pattern of the metric engine constructs smaller indices.
/// Therefore, compared to the default seg row count of 1024, by adjusting it to a smaller
/// value and appropriately increasing the size of the index, it results in an improved indexing effect.
const SEG_ROW_COUNT_FOR_DATA_REGION: u32 = 256;

/// The default compaction time window for metric engine data regions.
const DEFAULT_DATA_REGION_COMPACTION_TIME_WINDOW: &str = "1d";

/// Physical region options.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PhysicalRegionOptions {
    pub index: IndexOptions,
}

/// Index options for auto created columns
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub enum IndexOptions {
    #[default]
    None,
    Inverted,
    Skipping {
        granularity: u32,
        false_positive_rate: f64,
    },
}

/// Sets data region specific options.
pub fn set_data_region_options(
    options: &mut HashMap<String, String>,
    sparse_primary_key_encoding_if_absent: bool,
) {
    options.remove(METRIC_ENGINE_INDEX_TYPE_OPTION);
    options.remove(METRIC_ENGINE_INDEX_SKIPPING_INDEX_GRANULARITY_OPTION);
    options.remove(METRIC_ENGINE_INDEX_SKIPPING_INDEX_FALSE_POSITIVE_RATE_OPTION);
    options.insert(
        "index.inverted_index.segment_row_count".to_string(),
        SEG_ROW_COUNT_FOR_DATA_REGION.to_string(),
    );

    // Extract primary key encoding from the legacy nested key before dropping
    // all `memtable.partition_tree.*` keys.
    let legacy_encoding = options.remove(MEMTABLE_PARTITION_TREE_PRIMARY_KEY_ENCODING);
    options.retain(|k, _| !k.starts_with(LEGACY_PARTITION_TREE_OPTION_PREFIX));

    // Set memtable options for the data region. Bulk memtable produces
    // flat-encoded ranges, so the SST format must be flat to match.
    options.insert("memtable.type".to_string(), "bulk".to_string());
    options.insert("sst_format".to_string(), "flat".to_string());

    // Decide the top-level primary key encoding: caller-supplied top-level key wins,
    // then extracted legacy value, then the `sparse` default if requested.
    if !options.contains_key(PRIMARY_KEY_ENCODING) {
        if let Some(encoding) = legacy_encoding {
            options.insert(PRIMARY_KEY_ENCODING.to_string(), encoding);
        } else if sparse_primary_key_encoding_if_absent {
            options.insert(PRIMARY_KEY_ENCODING.to_string(), "sparse".to_string());
        }
    }

    if !options.contains_key(TWCS_TIME_WINDOW) {
        options.insert(
            COMPACTION_TYPE.to_string(),
            COMPACTION_TYPE_TWCS.to_string(),
        );
        options.insert(
            TWCS_TIME_WINDOW.to_string(),
            DEFAULT_DATA_REGION_COMPACTION_TIME_WINDOW.to_string(),
        );
    }
}

impl TryFrom<&HashMap<String, String>> for PhysicalRegionOptions {
    type Error = Error;

    fn try_from(value: &HashMap<String, String>) -> Result<Self> {
        let index = match value
            .get(METRIC_ENGINE_INDEX_TYPE_OPTION)
            .map(|s| s.to_lowercase())
        {
            Some(ref index_type) if index_type == "inverted" => Ok(IndexOptions::Inverted),
            Some(ref index_type) if index_type == "skipping" => {
                let granularity = value
                    .get(METRIC_ENGINE_INDEX_SKIPPING_INDEX_GRANULARITY_OPTION)
                    .map_or(
                        Ok(METRIC_ENGINE_INDEX_SKIPPING_INDEX_GRANULARITY_OPTION_DEFAULT),
                        |g| {
                            g.parse().map_err(|_| {
                                ParseRegionOptionsSnafu {
                                    reason: format!("Invalid granularity: {}", g),
                                }
                                .build()
                            })
                        },
                    )?;
                let false_positive_rate = value
                    .get(METRIC_ENGINE_INDEX_SKIPPING_INDEX_FALSE_POSITIVE_RATE_OPTION)
                    .map_or(
                        Ok(METRIC_ENGINE_INDEX_SKIPPING_INDEX_FALSE_POSITIVE_RATE_OPTION_DEFAULT),
                        |f| {
                            f.parse().ok().filter(|f| *f > 0.0 && *f <= 1.0).ok_or(
                                ParseRegionOptionsSnafu {
                                    reason: format!("Invalid false positive rate: {}", f),
                                }
                                .build(),
                            )
                        },
                    )?;
                Ok(IndexOptions::Skipping {
                    granularity,
                    false_positive_rate,
                })
            }
            Some(index_type) => ParseRegionOptionsSnafu {
                reason: format!("Invalid index type: {}", index_type),
            }
            .fail(),
            None => Ok(IndexOptions::default()),
        }?;

        Ok(PhysicalRegionOptions { index })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_set_data_region_options_should_remove_metric_engine_options() {
        let mut options = HashMap::new();
        options.insert(
            METRIC_ENGINE_INDEX_TYPE_OPTION.to_string(),
            "inverted".to_string(),
        );
        options.insert(
            METRIC_ENGINE_INDEX_SKIPPING_INDEX_GRANULARITY_OPTION.to_string(),
            "102400".to_string(),
        );
        options.insert(
            METRIC_ENGINE_INDEX_SKIPPING_INDEX_FALSE_POSITIVE_RATE_OPTION.to_string(),
            "0.01".to_string(),
        );
        set_data_region_options(&mut options, false);

        for key in [
            METRIC_ENGINE_INDEX_TYPE_OPTION,
            METRIC_ENGINE_INDEX_SKIPPING_INDEX_GRANULARITY_OPTION,
            METRIC_ENGINE_INDEX_SKIPPING_INDEX_FALSE_POSITIVE_RATE_OPTION,
        ] {
            assert_eq!(options.get(key), None);
        }
    }

    #[test]
    fn test_deserialize_physical_region_options_from_hashmap() {
        let mut options = HashMap::new();
        options.insert(
            METRIC_ENGINE_INDEX_TYPE_OPTION.to_string(),
            "inverted".to_string(),
        );
        options.insert(
            METRIC_ENGINE_INDEX_SKIPPING_INDEX_GRANULARITY_OPTION.to_string(),
            "102400".to_string(),
        );
        let physical_region_options = PhysicalRegionOptions::try_from(&options).unwrap();
        assert_eq!(physical_region_options.index, IndexOptions::Inverted);

        let mut options = HashMap::new();
        options.insert(
            METRIC_ENGINE_INDEX_TYPE_OPTION.to_string(),
            "skipping".to_string(),
        );
        options.insert(
            METRIC_ENGINE_INDEX_SKIPPING_INDEX_GRANULARITY_OPTION.to_string(),
            "102400".to_string(),
        );
        options.insert(
            METRIC_ENGINE_INDEX_SKIPPING_INDEX_FALSE_POSITIVE_RATE_OPTION.to_string(),
            "0.01".to_string(),
        );
        let physical_region_options = PhysicalRegionOptions::try_from(&options).unwrap();
        assert_eq!(
            physical_region_options.index,
            IndexOptions::Skipping {
                granularity: 102400,
                false_positive_rate: 0.01,
            }
        );
    }

    #[test]
    fn test_set_data_region_options_default_compaction_time_window() {
        // Test that default time window is set when not specified
        let mut options = HashMap::new();
        set_data_region_options(&mut options, false);

        assert_eq!(options.get("memtable.type"), Some(&"bulk".to_string()));
        assert_eq!(options.get("sst_format"), Some(&"flat".to_string()));
        assert_eq!(
            options.get(COMPACTION_TYPE),
            Some(&COMPACTION_TYPE_TWCS.to_string())
        );
        assert_eq!(options.get(TWCS_TIME_WINDOW), Some(&"1d".to_string()));
    }

    #[test]
    fn test_set_data_region_options_sparse_primary_key_encoding() {
        let mut options = HashMap::new();
        set_data_region_options(&mut options, true);

        assert_eq!(options.get("memtable.type"), Some(&"bulk".to_string()));
        assert_eq!(options.get("sst_format"), Some(&"flat".to_string()));
        assert_eq!(
            options.get(PRIMARY_KEY_ENCODING),
            Some(&"sparse".to_string())
        );
        assert!(!options.contains_key(MEMTABLE_PARTITION_TREE_PRIMARY_KEY_ENCODING));
    }

    #[test]
    fn test_set_data_region_options_migrates_legacy_partition_tree_options() {
        let mut options = HashMap::new();
        options.insert("memtable.type".to_string(), "partition_tree".to_string());
        options.insert(
            MEMTABLE_PARTITION_TREE_PRIMARY_KEY_ENCODING.to_string(),
            "sparse".to_string(),
        );
        options.insert(
            "memtable.partition_tree.index_max_keys_per_shard".to_string(),
            "2048".to_string(),
        );
        set_data_region_options(&mut options, false);

        assert_eq!(options.get("memtable.type"), Some(&"bulk".to_string()));
        assert_eq!(options.get("sst_format"), Some(&"flat".to_string()));
        assert_eq!(
            options.get(PRIMARY_KEY_ENCODING),
            Some(&"sparse".to_string())
        );
        // All legacy partition-tree-specific keys should be stripped.
        assert!(!options.contains_key(MEMTABLE_PARTITION_TREE_PRIMARY_KEY_ENCODING));
        assert!(!options.contains_key("memtable.partition_tree.index_max_keys_per_shard"));
    }

    #[test]
    fn test_set_data_region_options_preserves_existing_top_level_encoding() {
        let mut options = HashMap::new();
        options.insert(PRIMARY_KEY_ENCODING.to_string(), "dense".to_string());
        // Sparse flag is on but caller already specified dense.
        set_data_region_options(&mut options, true);

        assert_eq!(
            options.get(PRIMARY_KEY_ENCODING),
            Some(&"dense".to_string())
        );
    }

    #[test]
    fn test_set_data_region_options_respects_user_compaction_time_window() {
        // Test that user-specified time window is preserved
        let mut options = HashMap::new();
        options.insert(TWCS_TIME_WINDOW.to_string(), "2h".to_string());
        options.insert(COMPACTION_TYPE.to_string(), "twcs".to_string());
        set_data_region_options(&mut options, false);

        // User's time window should be preserved
        assert_eq!(options.get(TWCS_TIME_WINDOW), Some(&"2h".to_string()));
    }
}
