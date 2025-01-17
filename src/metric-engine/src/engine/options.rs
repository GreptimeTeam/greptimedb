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

use serde::{Deserialize, Serialize};
use store_api::metric_engine_consts::{
    METRIC_ENGINE_INDEX_SKIPPING_INDEX_GRANULARITY_OPTION,
    METRIC_ENGINE_INDEX_SKIPPING_INDEX_GRANULARITY_OPTION_DEFAULT, METRIC_ENGINE_INDEX_TYPE_OPTION,
};

use crate::error::{Error, ParseRegionOptionsSnafu, Result};

/// The empirical value for the seg row count of the metric data region.
/// Compared to the mito engine, the pattern of the metric engine constructs smaller indices.
/// Therefore, compared to the default seg row count of 1024, by adjusting it to a smaller
/// value and appropriately increasing the size of the index, it results in an improved indexing effect.
const SEG_ROW_COUNT_FOR_DATA_REGION: u32 = 256;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum IndexType {
    Inverted,
    Skipping,
}

/// Physical region options.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct PhysicalRegionOptions {
    pub index: IndexOptions,
}

/// Index options for auto created columns
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum IndexOptions {
    #[default]
    Inverted,
    Skipping {
        granularity: u32,
    },
}

/// Sets data region specific options.
pub fn set_data_region_options(options: &mut HashMap<String, String>) {
    options.remove(METRIC_ENGINE_INDEX_TYPE_OPTION);
    options.remove(METRIC_ENGINE_INDEX_SKIPPING_INDEX_GRANULARITY_OPTION);
    options.insert(
        "index.inverted_index.segment_row_count".to_string(),
        SEG_ROW_COUNT_FOR_DATA_REGION.to_string(),
    );
    // Set memtable options for the data region.
    options.insert("memtable.type".to_string(), "partition_tree".to_string());
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
                Ok(IndexOptions::Skipping { granularity })
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
        set_data_region_options(&mut options);

        for key in [
            METRIC_ENGINE_INDEX_TYPE_OPTION,
            METRIC_ENGINE_INDEX_SKIPPING_INDEX_GRANULARITY_OPTION,
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
        let physical_region_options = PhysicalRegionOptions::try_from(&options).unwrap();
        assert_eq!(
            physical_region_options.index,
            IndexOptions::Skipping {
                granularity: 102400
            }
        );
    }
}
