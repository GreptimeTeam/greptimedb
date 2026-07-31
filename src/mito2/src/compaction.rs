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

mod buckets;
pub mod compactor;
pub mod memory_manager;
pub mod picker;
mod reader;
pub mod run;
mod scheduler;
mod task;
#[cfg(test)]
mod test_util;
mod twcs;
mod window;

use std::collections::HashMap;

use common_meta::key::SchemaMetadataManagerRef;
use common_telemetry::{debug, error};
use common_time::TimeToLive;
use common_time::range::TimestampRange;
pub(crate) use reader::{
    Json2Targets, json2_targets_from_schemas, rewrite_json2_batch, rewrite_json2_schema,
};
pub use scheduler::CompactionRequest;
pub(crate) use scheduler::{
    CompactionExecution, CompactionPickFinished, CompactionScheduler, CompactionTransition,
};
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use store_api::storage::RegionId;

use crate::error::{GetSchemaMetadataSnafu, Result, TimeoutSnafu};
use crate::sst::file::{FileHandle, FileMeta, Level};

/// Finds compaction options and TTL together with a single metadata fetch to reduce RTT.
async fn find_dynamic_options(
    region_id: RegionId,
    region_options: &crate::region::options::RegionOptions,
    schema_metadata_manager: &SchemaMetadataManagerRef,
) -> Result<(crate::region::options::CompactionOptions, TimeToLive)> {
    let table_id = region_id.table_id();
    if let (true, Some(ttl)) = (region_options.compaction_override, region_options.ttl) {
        debug!(
            "Use region options directly for table {}: compaction={:?}, ttl={:?}",
            table_id, region_options.compaction, region_options.ttl
        );
        return Ok((region_options.compaction.clone(), ttl));
    }

    let db_options = tokio::time::timeout(
        crate::config::FETCH_OPTION_TIMEOUT,
        schema_metadata_manager.get_schema_options_by_table_id(table_id),
    )
    .await
    .context(TimeoutSnafu)?
    .context(GetSchemaMetadataSnafu)?;

    let ttl = if let Some(ttl) = region_options.ttl {
        debug!(
            "Use region TTL directly for table {}: ttl={:?}",
            table_id, region_options.ttl
        );
        ttl
    } else {
        db_options
            .as_ref()
            .and_then(|options| options.ttl)
            .unwrap_or_default()
            .into()
    };

    let compaction = if !region_options.compaction_override {
        if let Some(schema_opts) = db_options {
            let map: HashMap<String, String> = schema_opts
                .extra_options
                .iter()
                .filter_map(|(k, v)| {
                    if k.starts_with("compaction.") {
                        Some((k.clone(), v.clone()))
                    } else {
                        None
                    }
                })
                .collect();
            if map.is_empty() {
                region_options.compaction.clone()
            } else {
                crate::region::options::RegionOptions::try_from_options(region_id, &map)
                    .map(|o| o.compaction)
                    .unwrap_or_else(|e| {
                        error!(e; "Failed to create RegionOptions from map");
                        region_options.compaction.clone()
                    })
            }
        } else {
            debug!(
                "DB options is None for table {}, use region compaction: compaction={:?}",
                table_id, region_options.compaction
            );
            region_options.compaction.clone()
        }
    } else {
        debug!(
            "No schema options for table {}, use region compaction: compaction={:?}",
            table_id, region_options.compaction
        );
        region_options.compaction.clone()
    };

    debug!(
        "Resolved dynamic options for table {}: compaction={:?}, ttl={:?}",
        table_id, compaction, ttl
    );
    Ok((compaction, ttl))
}

#[derive(Debug, Clone)]
pub struct CompactionOutput {
    /// Compaction output file level.
    pub output_level: Level,
    /// Compaction input files.
    pub inputs: Vec<FileHandle>,
    /// Whether to remove deletion markers.
    pub filter_deleted: bool,
    /// Compaction output time range. Only windowed compaction specifies output time range.
    pub output_time_range: Option<TimestampRange>,
}

/// SerializedCompactionOutput is a serialized version of [CompactionOutput] by replacing [FileHandle] with [FileMeta].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SerializedCompactionOutput {
    output_level: Level,
    inputs: Vec<FileMeta>,
    filter_deleted: bool,
    output_time_range: Option<TimestampRange>,
}
