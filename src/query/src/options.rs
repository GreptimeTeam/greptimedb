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

use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use common_base::memory_limit::MemoryLimit;
use common_base::readable_size::ReadableSize;
use datafusion::config::{ConfigEntry, ConfigExtension, ExtensionOptions};
use serde::{Deserialize, Serialize};
use session::context::QueryContextRef;
use store_api::storage::{CursorShard, RegionId, SequenceNumber};
use table::metadata::TableId;

use crate::error::{Error, InvalidQueryContextExtensionSnafu, Result};

pub const FLOW_INCREMENTAL_AFTER_SEQS: &str = "flow.incremental_after_seqs";
pub const FLOW_INCREMENTAL_MODE: &str = "flow.incremental_mode";
pub const FLOW_RETURN_REGION_SEQ: &str = "flow.return_region_seq";
/// Marks a Flow repair read. Such a query only discovers or overwrites time windows,
/// so it must never be refused for holding an old cursor: it is the query
/// that repairs the state a stale cursor can no longer explain. Ordinary incremental
/// deltas must not set it.
///
/// The value selects the mode, because the two repair reads need different scans:
/// - [`FLOW_REPAIR_MODE_DISCOVERY`]: dirty-window discovery. It may prune at file
///   granularity and force every row's sequence to the file barrier, because it only
///   needs the timestamps of the rows a stale cursor may have missed.
/// - [`FLOW_REPAIR_MODE_FENCED`]: the fenced repair DML. It must read the selected
///   windows completely, so it gets no file pruning and no forced override; it only
///   skips the `C < B` rejection and must still prove `B <= H`.
pub const FLOW_REPAIR_MODE: &str = "flow.repair_mode";
/// `flow.repair_mode` value: recovery dirty-window discovery.
pub const FLOW_REPAIR_MODE_DISCOVERY: &str = "discovery";
/// `flow.repair_mode` value: fenced repair DML.
pub const FLOW_REPAIR_MODE_FENCED: &str = "fenced";
/// Reads the physical `__sequence` column of a file this region adopted from another region,
/// instead of virtualizing every row at the handle's admission barrier. The scan then hides the
/// rows whose number is at or below the file's own substituted cut
/// (`FileMeta::substituted_up_to`) and reports their time ranges in `substituted_row_ranges`.
///
/// Only Flow's `sequence_range` incremental delta sets it.
pub const FLOW_READ_ROW_SEQUENCE: &str = "flow.read_row_sequence";
pub const FLOW_SINK_TABLE_ID: &str = "flow.sink_table_id";
/// Flow scheduler binding for the logical time of one scheduled attempt.
/// Query planning, SQL/TQL parsing, range-select rewrite and DataFusion
/// execution read this extension so `now()` is stable for the whole attempt.
pub const FLOW_SCHEDULED_TIME_MILLIS: &str = "flow.scheduled_time_millis";
/// Enable by default, set to false to explicitly disable.
pub const QUERY_ENABLE_REMOTE_DYNAMIC_FILTER_PUSHDOWN: &str =
    "query.enable_remote_dynamic_filter_pushdown";

/// The kind of Flow repair read requested through [FLOW_REPAIR_MODE].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlowRepairMode {
    /// Dirty-window discovery: prunes at file granularity and forces the file
    /// barrier as every row's sequence.
    Discovery,
    /// Fenced repair DML: reads the selected windows completely.
    Fenced,
}

impl FlowRepairMode {
    fn parse(value: &str) -> Result<Self> {
        match value {
            v if v.eq_ignore_ascii_case(FLOW_REPAIR_MODE_DISCOVERY) => Ok(Self::Discovery),
            v if v.eq_ignore_ascii_case(FLOW_REPAIR_MODE_FENCED) => Ok(Self::Fenced),
            _ => Err(invalid_query_context_extension(format!(
                "Invalid value for {}: {}, expected {} or {}",
                FLOW_REPAIR_MODE, value, FLOW_REPAIR_MODE_DISCOVERY, FLOW_REPAIR_MODE_FENCED
            ))),
        }
    }
}

pub const FLOW_INCREMENTAL_MODE_MEMTABLE_ONLY: &str = "memtable_only";
pub const FLOW_INCREMENTAL_MODE_SEQUENCE_RANGE: &str = "sequence_range";

/// Query spill mode controlling disk manager behavior.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum QuerySpillMode {
    /// Preserve DataFusion default disk manager behavior (OS temp directory).
    Default,
    /// Explicitly configure spill path, quota, and compression.
    Custom,
    /// Explicitly disable disk spilling; temporary file creation will error.
    Disabled,
}

/// Compression for spilled data files.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum QuerySpillCompression {
    /// No compression (default, matches DataFusion default).
    Uncompressed,
    /// LZ4 frame compression.
    Lz4Frame,
    /// Zstandard compression.
    Zstd,
}

/// Memory pool allocation policy.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum QueryMemoryPoolPolicy {
    /// Greedy first-come-first-served (default).
    Greedy,
    /// Fair divides memory available after unspillable reservations evenly among
    /// spillable reservations and may trigger earlier spills.
    Fair,
}

#[derive(Debug, Clone, Default)]
pub struct FactorizedJoinAggOptions {
    pub enabled: bool,
}

impl ConfigExtension for FactorizedJoinAggOptions {
    const PREFIX: &'static str = "factorized_join_agg";
}

impl ExtensionOptions for FactorizedJoinAggOptions {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
    fn cloned(&self) -> Box<dyn ExtensionOptions> {
        Box::new(self.clone())
    }
    fn set(&mut self, key: &str, value: &str) -> datafusion::error::Result<()> {
        Err(datafusion::common::DataFusionError::NotImplemented(
            format!("FactorizedJoinAggOptions does not support set key: {key} with value: {value}"),
        ))
    }
    fn entries(&self) -> Vec<ConfigEntry> {
        vec![ConfigEntry {
            key: "enabled".to_string(),
            value: Some(self.enabled.to_string()),
            description: "Enable experimental factorized join aggregation",
        }]
    }
}

/// Query engine configuration options.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct QueryOptions {
    /// Parallelism of query engine. Default to 0, which implies the number of logical CPUs.
    pub parallelism: usize,
    /// Whether to allow query fallback when push down fails.
    pub allow_query_fallback: bool,
    /// Experimental: enable shape-based factorized AVG/SUM join aggregation. Default false.
    pub experimental_enable_factorized_join_agg: bool,
    /// Memory pool size for query execution. Setting it to 0 disables the limit (unbounded).
    /// Supports absolute size (e.g., "2GB") or percentage (e.g., "50%").
    /// When this limit is reached, queries will fail with ResourceExhausted error.
    pub memory_pool_size: MemoryLimit,
    /// Whether to expose per-region query load metrics.
    #[serde(skip)]
    pub enable_per_region_metrics: bool,
    /// Experimental: spill-to-disk mode.
    /// - `default`: preserve DataFusion built-in OS temp directory behavior.
    /// - `custom`: explicitly configure spill path, max directory size, and compression.
    /// - `disabled`: explicitly disable disk spilling.
    pub experimental_spill_mode: QuerySpillMode,
    /// Experimental: spill directory path. Ignored unless `experimental_spill_mode` is
    /// `"custom"`. When set, spill files are written into this directory.
    pub experimental_spill_path: Option<PathBuf>,
    /// Experimental: maximum total size of the spill directory (data written to spill files).
    /// Ignored unless `experimental_spill_mode` is `"custom"`. Default: `1GiB`.
    pub experimental_spill_max_temp_directory_size: ReadableSize,
    /// Experimental: compression algorithm applied to spilled data.
    /// Ignored unless `experimental_spill_mode` is `"custom"`. Default: `uncompressed`.
    pub experimental_spill_compression: QuerySpillCompression,
    /// Experimental: memory pool allocation policy.
    /// - `greedy`: Greedy first-come-first-served (default).
    /// - `fair`: Fair divides memory available after unspillable reservations
    ///   evenly among spillable reservations and may trigger earlier spills.
    pub experimental_memory_pool_policy: QueryMemoryPoolPolicy,
    /// Enables the experimental native PromQL execution path for compatible queries. This
    /// path currently applies only to standalone queries using in-process region transport.
    ///
    /// This is the engine-wide default. A session `SET query.experimental_native_promql = true`
    /// or a per-request `query.experimental_native_promql` option overrides it, in that order, so
    /// a `true` default routes every query natively and a `false` one still lets one query or one
    /// session ask for it. Default: `false`.
    pub experimental_native_promql: bool,
}

#[allow(clippy::derivable_impls)]
impl Default for QueryOptions {
    fn default() -> Self {
        Self {
            parallelism: 0,
            allow_query_fallback: false,
            experimental_enable_factorized_join_agg: false,
            memory_pool_size: MemoryLimit::default(),
            enable_per_region_metrics: false,
            experimental_spill_mode: QuerySpillMode::Default,
            experimental_spill_path: None,
            experimental_spill_max_temp_directory_size: ReadableSize::gb(1),
            experimental_spill_compression: QuerySpillCompression::Uncompressed,
            experimental_memory_pool_policy: QueryMemoryPoolPolicy::Greedy,
            experimental_native_promql: false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlowIncrementalMode {
    MemtableOnly,
    SequenceRange,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct FlowQueryExtensions {
    /// Maps a region id to the incremental cursor of each of its partition expressions.
    ///
    /// The region is the **creator** of the sequences, and the cursor of a slice is the
    /// last sequence consumed in the value domain its partition expression selects (see
    /// [`CursorShard`]). A holder region reads its own memtables and files after the
    /// least advanced of its own slices, while a file it adopted from another region is
    /// filtered by the cursor of the creator's slice its own partition expression
    /// belongs to.
    pub incremental_after_seqs: Option<HashMap<u64, Vec<CursorShard>>>,
    /// Incremental read mode requested by the caller.
    pub incremental_mode: Option<FlowIncrementalMode>,
    /// Whether the caller expects per-region watermark metadata in terminal metrics.
    pub return_region_seq: bool,
    /// Which kind of Flow repair read this is, if any (see [FLOW_REPAIR_MODE]).
    pub repair_mode: Option<FlowRepairMode>,
    /// Optional sink table id used to distinguish source scans from sink reads.
    pub sink_table_id: Option<TableId>,
    /// Whether an incremental scan reads physical row sequences and hides the rows a
    /// substitution produced (see [FLOW_READ_ROW_SEQUENCE]).
    pub read_row_sequence: bool,
}

impl FlowQueryExtensions {
    /// Parses flow-specific query extensions when any flow key is present.
    ///
    /// Returns `Ok(None)` for ordinary queries with no flow-related extensions,
    /// `Ok(Some(_))` when flow context is present and valid, and `Err(_)` when a
    /// flow-related extension is present but malformed or incomplete.
    pub fn parse_flow_extensions(extensions: &HashMap<String, String>) -> Result<Option<Self>> {
        let has_flow_context = extensions.contains_key(FLOW_INCREMENTAL_AFTER_SEQS)
            || extensions.contains_key(FLOW_INCREMENTAL_MODE)
            || extensions.contains_key(FLOW_RETURN_REGION_SEQ)
            || extensions.contains_key(FLOW_REPAIR_MODE)
            || extensions.contains_key(FLOW_READ_ROW_SEQUENCE)
            || extensions.contains_key(FLOW_SINK_TABLE_ID);

        if !has_flow_context {
            return Ok(None);
        }

        let incremental_mode = extensions
            .get(FLOW_INCREMENTAL_MODE)
            .map(|value| match value.as_str() {
                v if v.eq_ignore_ascii_case(FLOW_INCREMENTAL_MODE_MEMTABLE_ONLY) => {
                    Ok(FlowIncrementalMode::MemtableOnly)
                }
                v if v.eq_ignore_ascii_case(FLOW_INCREMENTAL_MODE_SEQUENCE_RANGE) => {
                    Ok(FlowIncrementalMode::SequenceRange)
                }
                _ => Err(invalid_query_context_extension(format!(
                    "Invalid value for {}: {}",
                    FLOW_INCREMENTAL_MODE, value
                ))),
            })
            .transpose()?;

        let incremental_after_seqs = extensions
            .get(FLOW_INCREMENTAL_AFTER_SEQS)
            .map(|value| parse_incremental_after_seqs(value.as_str()))
            .transpose()?;

        let return_region_seq = extensions
            .get(FLOW_RETURN_REGION_SEQ)
            .map(|value| parse_bool(FLOW_RETURN_REGION_SEQ, value.as_str()))
            .transpose()?
            .unwrap_or(false);

        let repair_mode = extensions
            .get(FLOW_REPAIR_MODE)
            .map(|value| FlowRepairMode::parse(value.as_str()))
            .transpose()?;

        let read_row_sequence = extensions
            .get(FLOW_READ_ROW_SEQUENCE)
            .map(|value| parse_bool(FLOW_READ_ROW_SEQUENCE, value.as_str()))
            .transpose()?
            .unwrap_or(false);

        let sink_table_id = extensions
            .get(FLOW_SINK_TABLE_ID)
            .map(|value| {
                value.parse::<TableId>().map_err(|_| {
                    invalid_query_context_extension(format!(
                        "Invalid value for {}: {}",
                        FLOW_SINK_TABLE_ID, value
                    ))
                })
            })
            .transpose()?;

        if matches!(
            incremental_mode,
            Some(FlowIncrementalMode::MemtableOnly | FlowIncrementalMode::SequenceRange)
        ) {
            let mode = if incremental_mode == Some(FlowIncrementalMode::MemtableOnly) {
                FLOW_INCREMENTAL_MODE_MEMTABLE_ONLY
            } else {
                FLOW_INCREMENTAL_MODE_SEQUENCE_RANGE
            };
            let after_seqs = incremental_after_seqs.as_ref().ok_or_else(|| {
                invalid_query_context_extension(format!(
                    "{} is required when {}={}.",
                    FLOW_INCREMENTAL_AFTER_SEQS, FLOW_INCREMENTAL_MODE, mode
                ))
            })?;
            if after_seqs.is_empty() {
                return Err(invalid_query_context_extension(format!(
                    "{} must not be empty when {}={}.",
                    FLOW_INCREMENTAL_AFTER_SEQS, FLOW_INCREMENTAL_MODE, mode
                )));
            }
        }

        Ok(Some(Self {
            incremental_after_seqs,
            incremental_mode,
            return_region_seq,
            repair_mode,
            sink_table_id,
            read_row_sequence,
        }))
    }

    pub fn validate_for_scan(&self, source_region_id: RegionId) -> Result<bool> {
        if self.sink_table_id.is_some() && self.sink_table_id == Some(source_region_id.table_id()) {
            return Ok(false);
        }

        if matches!(
            self.incremental_mode,
            Some(FlowIncrementalMode::MemtableOnly | FlowIncrementalMode::SequenceRange)
        ) {
            let mode = if self.incremental_mode == Some(FlowIncrementalMode::MemtableOnly) {
                FLOW_INCREMENTAL_MODE_MEMTABLE_ONLY
            } else {
                FLOW_INCREMENTAL_MODE_SEQUENCE_RANGE
            };
            let after_seqs = self.incremental_after_seqs.as_ref().ok_or_else(|| {
                invalid_query_context_extension(format!(
                    "{} is required when {}={}.",
                    FLOW_INCREMENTAL_AFTER_SEQS, FLOW_INCREMENTAL_MODE, mode
                ))
            })?;

            if !after_seqs.contains_key(&source_region_id.as_u64()) {
                return Err(invalid_query_context_extension(format!(
                    "Missing region {} in {} when {}={}.",
                    source_region_id, FLOW_INCREMENTAL_AFTER_SEQS, FLOW_INCREMENTAL_MODE, mode
                )));
            }
        }

        Ok(self.incremental_after_seqs.is_some())
    }

    pub fn should_collect_region_watermark(&self) -> bool {
        should_collect_region_watermark(
            self.return_region_seq,
            self.incremental_after_seqs.is_some(),
        )
    }

    /// The cursor of the least advanced expression slice of `region_id`.
    ///
    /// A read of the region's **own** memtables and files cannot tell which slice a row
    /// belongs to, so it has to start after the least advanced one: the highest slice
    /// bound would drop the rows of a slice that has not been consumed yet. The value is a
    /// lower bound for the whole read, not a per-row decision.
    pub fn region_cursor(&self, region_id: u64) -> Option<SequenceNumber> {
        self.incremental_after_seqs
            .as_ref()?
            .get(&region_id)?
            .iter()
            .map(|shard| shard.seq)
            .min()
    }

    /// The slice cursors of every creator region, as a scan request carries them.
    ///
    /// Only a physical-sequence read of an adopted file needs them: it is the only read
    /// that compares a row's number against the domain the number was allocated in.
    pub fn creator_cursor_shards(&self) -> Option<Arc<BTreeMap<u64, Vec<CursorShard>>>> {
        let cursors = self.incremental_after_seqs.as_ref()?;
        Some(Arc::new(
            cursors
                .iter()
                .map(|(region_id, shards)| (*region_id, shards.clone()))
                .collect(),
        ))
    }
}

/// Returns whether query-level remote dynamic filter propagation is enabled.
///
/// The option defaults to enabled to preserve existing behavior. Callers may set
/// `query.enable_remote_dynamic_filter_pushdown=false` in query context
/// extensions to disable FE->DN remote dynamic filter propagation for a single
/// query.
pub fn remote_dyn_filter_pushdown_enabled_from_extensions(
    extensions: &HashMap<String, String>,
) -> Result<bool> {
    extensions
        .get(QUERY_ENABLE_REMOTE_DYNAMIC_FILTER_PUSHDOWN)
        .map(|value| parse_bool(QUERY_ENABLE_REMOTE_DYNAMIC_FILTER_PUSHDOWN, value.as_str()))
        .transpose()
        .map(|value| value.unwrap_or(true))
}

/// Returns whether raw Flow query extensions request terminal region watermark collection.
///
/// This is only an intent/presence check for transport/scan plumbing; callers that need
/// validated Flow options must still use [`FlowQueryExtensions::parse_flow_extensions`].
pub fn should_collect_region_watermark_from_extensions(
    extensions: &HashMap<String, String>,
) -> bool {
    let return_region_seq = extensions
        .get(FLOW_RETURN_REGION_SEQ)
        .is_some_and(|value| value.eq_ignore_ascii_case("true"));
    let has_incremental_after_seqs = extensions.contains_key(FLOW_INCREMENTAL_AFTER_SEQS);

    should_collect_region_watermark(return_region_seq, has_incremental_after_seqs)
}

fn should_collect_region_watermark(
    return_region_seq: bool,
    has_incremental_after_seqs: bool,
) -> bool {
    return_region_seq || has_incremental_after_seqs
}

/// Version marker of the expression-keyed cursor form. A reader refuses a version it does not
/// know.
const CURSOR_SHARD_VERSION_KEY: &str = "version";
/// The only version of the expression-keyed cursor form. A slice states whether it carries an
/// upper bound through [`CURSOR_SHARD_UNTIL_KEY`], so the version distinguishes nothing and
/// carries no information.
const CURSOR_SHARD_VERSION: u64 = 2;
const CURSOR_SHARD_SHARDS_KEY: &str = "shards";
const CURSOR_SHARD_EXPR_KEY: &str = "expr";
const CURSOR_SHARD_SEQ_KEY: &str = "seq";
/// Inclusive upper bound of a slice's delta; absent means the slice is unbounded above.
const CURSOR_SHARD_UNTIL_KEY: &str = "until";

/// Parses [`FLOW_INCREMENTAL_AFTER_SEQS`].
///
/// * expression-keyed (version [`CURSOR_SHARD_VERSION`]):
///   `{"<region_id>": {"version": 2, "shards": [{"expr": "<partition expr json>", "seq":
///   seq, "until": seq?}, {"expr": null, "seq": seq}, ...]}}` — one cursor per partition
///   expression of that region, `null` selecting the region's complete value domain. `expr`
///   is the JSON form `PartitionExpr::as_json_str()` writes and is validated here, so an
///   expression identity that does not decode never reaches a scan. `until` is optional: a
///   slice that states it covers `seq < number <= until`, one that omits it is unbounded
///   above. [`encode_incremental_after_seqs`] writes exactly this shape;
/// * region-keyed: `{"<region_id>": seq}` — one cursor for the region's complete value
///   domain, i.e. the single whole-domain slice. Flow still writes it whenever it knows only
///   the region projection and no per-slice cursor
///   (`batching_mode::task::inc::BatchingTask::build_flow_query_extensions`), so it is a live
///   shape, not history.
///
/// Any other shape is refused as an unknown format: it is never read as a cursor this
/// build can act on.
fn parse_incremental_after_seqs(value: &str) -> Result<HashMap<u64, Vec<CursorShard>>> {
    let raw = serde_json::from_str::<HashMap<String, serde_json::Value>>(value).map_err(|e| {
        invalid_query_context_extension(format!(
            "Invalid JSON for {}: {} ({})",
            FLOW_INCREMENTAL_AFTER_SEQS, value, e
        ))
    })?;

    raw.into_iter()
        .map(|(region_id, raw_cursor)| {
            let region_id = region_id.parse::<u64>().map_err(|_| {
                invalid_query_context_extension(format!(
                    "Invalid region id in {}: {}",
                    FLOW_INCREMENTAL_AFTER_SEQS, region_id
                ))
            })?;

            let shards = match raw_cursor {
                // Region-keyed shape: the region's complete value domain.
                serde_json::Value::Number(_) | serde_json::Value::String(_) => {
                    vec![CursorShard::whole_domain(parse_cursor_seq(
                        region_id,
                        &raw_cursor,
                    )?)]
                }
                // Expression-keyed shape.
                serde_json::Value::Object(fields) => {
                    parse_expression_keyed_shards(region_id, &fields)?
                }
                // Any other shape is an unknown format, never a cursor to guess at.
                _ => {
                    return Err(invalid_query_context_extension(format!(
                        "Invalid cursor value type in {} for region {}",
                        FLOW_INCREMENTAL_AFTER_SEQS, region_id
                    )));
                }
            };

            Ok((region_id, shards))
        })
        .collect()
}

/// Parses the expression-keyed value of one region.
fn parse_expression_keyed_shards(
    region_id: u64,
    fields: &serde_json::Map<String, serde_json::Value>,
) -> Result<Vec<CursorShard>> {
    match fields.get(CURSOR_SHARD_VERSION_KEY) {
        Some(version) if version.as_u64() == Some(CURSOR_SHARD_VERSION) => {}
        Some(version) => {
            return Err(invalid_query_context_extension(format!(
                "Unsupported cursor version {:?} in {} for region {}: this build reads version {}",
                version, FLOW_INCREMENTAL_AFTER_SEQS, region_id, CURSOR_SHARD_VERSION
            )));
        }
        None => {
            return Err(invalid_query_context_extension(format!(
                "Missing cursor version in {} for region {}: an object value must state version {}",
                FLOW_INCREMENTAL_AFTER_SEQS, region_id, CURSOR_SHARD_VERSION
            )));
        }
    }

    let Some(serde_json::Value::Array(shards)) = fields.get(CURSOR_SHARD_SHARDS_KEY) else {
        return Err(invalid_query_context_extension(format!(
            "Missing cursor shards in {} for region {}: expected an array under \"{}\"",
            FLOW_INCREMENTAL_AFTER_SEQS, region_id, CURSOR_SHARD_SHARDS_KEY
        )));
    };
    if shards.is_empty() {
        return Err(invalid_query_context_extension(format!(
            "Region {} states no cursor slice in {}",
            region_id, FLOW_INCREMENTAL_AFTER_SEQS
        )));
    }

    shards
        .iter()
        .map(|shard| parse_cursor_shard(region_id, shard))
        .collect()
}

/// Parses one `{"expr": <partition expr json> | null, "seq": N, "until": N?}` slice.
///
/// `until` is optional and read whenever it is stated: a bound is a bound. A bound below
/// the slice's own `seq` states an empty delta (a slice covers `seq < number <= until`) and
/// is refused instead of being clamped or swapped.
fn parse_cursor_shard(region_id: u64, shard: &serde_json::Value) -> Result<CursorShard> {
    let serde_json::Value::Object(fields) = shard else {
        return Err(invalid_query_context_extension(format!(
            "Invalid cursor slice in {} for region {}: expected {{\"expr\": ..., \"seq\": ...}}, found {}",
            FLOW_INCREMENTAL_AFTER_SEQS, region_id, shard
        )));
    };

    let partition_expr = match fields.get(CURSOR_SHARD_EXPR_KEY) {
        // A missing key would have to be guessed: an absent expression is not the same
        // statement as an explicit whole-domain `null`.
        None => {
            return Err(invalid_query_context_extension(format!(
                "Invalid cursor slice in {} for region {}: expected the partition expression under \"{}\" (null for the whole domain)",
                FLOW_INCREMENTAL_AFTER_SEQS, region_id, CURSOR_SHARD_EXPR_KEY
            )));
        }
        Some(serde_json::Value::Null) => None,
        Some(serde_json::Value::String(expr)) => Some(validate_partition_expr(region_id, expr)?),
        Some(other) => {
            return Err(invalid_query_context_extension(format!(
                "Invalid partition expression in {} for region {}: expected a JSON string or null, found {}",
                FLOW_INCREMENTAL_AFTER_SEQS, region_id, other
            )));
        }
    };

    let Some(seq) = fields.get(CURSOR_SHARD_SEQ_KEY) else {
        return Err(invalid_query_context_extension(format!(
            "Invalid cursor slice in {} for region {}: expected the consumed sequence under \"{}\"",
            FLOW_INCREMENTAL_AFTER_SEQS, region_id, CURSOR_SHARD_SEQ_KEY
        )));
    };
    let seq = parse_cursor_seq(region_id, seq)?;

    // A slice that states no bound is unbounded above; an explicit `null` states the same.
    let until = match fields.get(CURSOR_SHARD_UNTIL_KEY) {
        None | Some(serde_json::Value::Null) => None,
        Some(raw) => Some(parse_cursor_seq(region_id, raw)?),
    };

    // A slice covers `seq < number <= until`, so an `until` below its own `seq` states an
    // empty window. Refusing it is the only reading that keeps the producer's statement
    // intact: clamping or swapping would silently deliver a delta it never asked for. A
    // lower bound is also worse than a no-op downstream — a read bounds a file with
    // `GtLtEq { min, max }` and `max < min` selects no row at all, while a file several
    // slices match can still admit rows through another slice's smaller minimum, so the
    // read would answer with a plain wrong result instead of failing.
    if let Some(until) = until
        && until < seq
    {
        return Err(invalid_query_context_extension(format!(
            "Invalid cursor slice in {} for region {}: the upper bound {} is below the slice's \
             own consumed sequence {} (a slice covers seq < number <= until, so its until must \
             be at least its seq)",
            FLOW_INCREMENTAL_AFTER_SEQS, region_id, until, seq
        )));
    }

    Ok(match until {
        Some(until) => CursorShard::new(partition_expr, seq).with_until(until),
        None => CursorShard::new(partition_expr, seq),
    })
}

/// Checks that `expr` is a partition expression this build can read back, and returns the
/// identity to store. `store-api` cannot depend on `partition`, so this is the last place a
/// malformed expression is rejected.
fn validate_partition_expr(region_id: u64, expr: &str) -> Result<String> {
    match partition::expr::PartitionExpr::from_json_str(expr) {
        Ok(Some(parsed)) => Ok(parsed.as_json_str().map_err(|e| {
            invalid_query_context_extension(format!(
                "Cannot re-serialize the partition expression of region {} in {}: {}",
                region_id, FLOW_INCREMENTAL_AFTER_SEQS, e
            ))
        })?),
        Ok(None) => Err(invalid_query_context_extension(format!(
            "Invalid partition expression of region {} in {}: {} is not a partition expression",
            region_id, FLOW_INCREMENTAL_AFTER_SEQS, expr
        ))),
        Err(e) => Err(invalid_query_context_extension(format!(
            "Invalid partition expression of region {} in {}: {} ({})",
            region_id, FLOW_INCREMENTAL_AFTER_SEQS, expr, e
        ))),
    }
}

/// Parses one sequence number of a cursor, accepting the numeric and the string form.
fn parse_cursor_seq(region_id: u64, raw: &serde_json::Value) -> Result<u64> {
    match raw {
        serde_json::Value::Number(num) => num.as_u64().ok_or_else(|| {
            invalid_query_context_extension(format!(
                "Invalid sequence value in {} for region {}: {}",
                FLOW_INCREMENTAL_AFTER_SEQS, region_id, num
            ))
        }),
        serde_json::Value::String(s) => s.parse::<u64>().map_err(|_| {
            invalid_query_context_extension(format!(
                "Invalid sequence string in {} for region {}: {}",
                FLOW_INCREMENTAL_AFTER_SEQS, region_id, s
            ))
        }),
        other => Err(invalid_query_context_extension(format!(
            "Invalid sequence value type in {} for region {}: {}",
            FLOW_INCREMENTAL_AFTER_SEQS, region_id, other
        ))),
    }
}

/// Encodes slice cursors as the expression-keyed form of [`FLOW_INCREMENTAL_AFTER_SEQS`],
/// which [`parse_incremental_after_seqs`] reads back.
///
/// A whole-domain slice is written as an explicit `"expr": null`, and an upper bound
/// ([`CursorShard::until`]) only when the slice states one. Every region is written under
/// [`CURSOR_SHARD_VERSION`]: the bound travels per slice, so nothing about a region has to be
/// stated at its version marker.
pub fn encode_incremental_after_seqs(cursors: &BTreeMap<u64, Vec<CursorShard>>) -> Result<String> {
    let encoded = cursors
        .iter()
        .map(|(region_id, shards)| {
            let shards = shards
                .iter()
                .map(|shard| {
                    let mut encoded = serde_json::json!({
                        CURSOR_SHARD_EXPR_KEY: shard.partition_expr,
                        CURSOR_SHARD_SEQ_KEY: shard.seq,
                    });
                    // A slice with no upper bound writes no `until`: the field states a
                    // bound, so its absence is the unbounded slice.
                    if let Some(until) = shard.until {
                        encoded[CURSOR_SHARD_UNTIL_KEY] = serde_json::json!(until);
                    }
                    encoded
                })
                .collect::<Vec<_>>();
            let value = serde_json::json!({
                CURSOR_SHARD_VERSION_KEY: CURSOR_SHARD_VERSION,
                CURSOR_SHARD_SHARDS_KEY: shards,
            });
            (region_id.to_string(), value)
        })
        .collect::<BTreeMap<_, _>>();
    serde_json::to_string(&encoded).map_err(|e| {
        invalid_query_context_extension(format!(
            "Failed to serialize {}: {}",
            FLOW_INCREMENTAL_AFTER_SEQS, e
        ))
    })
}

fn parse_bool(option_name: &str, value: &str) -> Result<bool> {
    match value {
        v if v.eq_ignore_ascii_case("true") => Ok(true),
        v if v.eq_ignore_ascii_case("false") => Ok(false),
        _ => Err(invalid_query_context_extension(format!(
            "Invalid value for {}: {}",
            option_name, value
        ))),
    }
}

/// Parse the scheduled time (in milliseconds since Unix epoch) from extensions.
///
/// Returns `Ok(None)` if the extension key is absent, `Ok(Some(millis))` on success,
/// or `Err` if the value is malformed.
pub fn parse_scheduled_time_millis(extensions: &HashMap<String, String>) -> Result<Option<i64>> {
    match extensions.get(FLOW_SCHEDULED_TIME_MILLIS) {
        Some(val) => val.parse::<i64>().map(Some).map_err(|_| {
            invalid_query_context_extension(format!(
                "Invalid value for {}: {}",
                FLOW_SCHEDULED_TIME_MILLIS, val
            ))
        }),
        None => Ok(None),
    }
}

/// Parse the scheduled time from extensions into a [`DateTime<Utc>`].
///
/// Returns `Ok(None)` if the extension key is absent, `Ok(Some(datetime))` on success,
/// or `Err` if the value is malformed.  Millis that produce an out-of-range timestamp are
/// also rejected.
pub fn parse_scheduled_time_datetime(
    extensions: &HashMap<String, String>,
) -> Result<Option<DateTime<Utc>>> {
    match parse_scheduled_time_millis(extensions)? {
        Some(millis) => DateTime::from_timestamp_millis(millis)
            .map(Some)
            .ok_or_else(|| {
                invalid_query_context_extension(format!(
                    "Out-of-range timestamp for {}: {} ms",
                    FLOW_SCHEDULED_TIME_MILLIS, millis
                ))
            }),
        None => Ok(None),
    }
}

/// Best-effort helper: extract scheduled time from a [`QueryContextRef`] as a [`DateTime<Utc>`].
///
/// Errors are silently swallowed, returning `None`; use [`parse_scheduled_time_datetime`]
/// at call sites that need to reject malformed scheduled time.
pub fn scheduled_time_from_ctx(query_ctx: &QueryContextRef) -> Option<DateTime<Utc>> {
    let extensions = query_ctx.extensions();
    parse_scheduled_time_datetime(&extensions).ok().flatten()
}

/// Carries the scheduled logical "now" through [`ConfigOptions::extensions`] so
/// that the distributed plan analyzer can apply it during expression
/// simplification (preventing wall-clock constant-folding of `now()`).
#[derive(Debug, Clone)]
pub(crate) struct ScheduledTimeExtension {
    pub(crate) scheduled_time: Option<DateTime<Utc>>,
}

impl ConfigExtension for ScheduledTimeExtension {
    const PREFIX: &'static str = "flow_scheduled_time";
}

impl ExtensionOptions for ScheduledTimeExtension {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }

    fn cloned(&self) -> Box<dyn ExtensionOptions> {
        Box::new(self.clone())
    }

    fn set(&mut self, key: &str, value: &str) -> datafusion::error::Result<()> {
        Err(datafusion_common::DataFusionError::NotImplemented(format!(
            "ScheduledTimeExtension does not support set key: {key} with value: {value}"
        )))
    }

    fn entries(&self) -> Vec<ConfigEntry> {
        vec![]
    }
}

/// Carries the native PromQL route switch a query asked for through
/// [`ConfigOptions::extensions`].
///
/// The value is resolved once per query, where the query options snapshot is applied to the
/// planning state: the request extension
/// [`NATIVE_PROMQL_EXTENSION_KEY`](crate::promql::planner::NATIVE_PROMQL_EXTENSION_KEY) over a
/// session `SET` of the same option over the engine-wide
/// [`QueryOptions::experimental_native_promql`] default. Planning and `SHOW` both read it back from
/// there, so a query that states nothing routes as the engine default says.
///
/// What it records is the *request*, not the plan the query got: planning still applies the
/// conditions that decide whether the route can be taken at all (a statement that allows query
/// fallback or one planned inside a CTE keeps the existing plan whatever this says), so `SHOW
/// query.experimental_native_promql` answers what the query asked for.
#[derive(Debug, Clone, Copy)]
pub(crate) struct NativePromqlExtension {
    pub(crate) requested: bool,
}

impl ConfigExtension for NativePromqlExtension {
    const PREFIX: &'static str = "native_promql";
}

impl ExtensionOptions for NativePromqlExtension {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }

    fn cloned(&self) -> Box<dyn ExtensionOptions> {
        Box::new(*self)
    }

    fn set(&mut self, key: &str, value: &str) -> datafusion::error::Result<()> {
        Err(datafusion_common::DataFusionError::NotImplemented(format!(
            "NativePromqlExtension does not support set key: {key} with value: {value}"
        )))
    }

    fn entries(&self) -> Vec<ConfigEntry> {
        vec![ConfigEntry {
            key: "requested".to_string(),
            value: Some(self.requested.to_string()),
            description: "Whether the query requests the native fused-series PromQL route",
        }]
    }
}

fn invalid_query_context_extension(reason: String) -> Error {
    InvalidQueryContextExtensionSnafu { reason }.build()
}

#[cfg(test)]
mod flow_extension_tests {
    use datatypes::value::Value;
    use partition::expr::{Operand, col as partition_col};

    use super::*;

    /// The identity of one half-open expression slice `k0 >= lo AND k0 < hi`, in the form
    /// the wire carries it.
    fn slice_expr(lo: u64, hi: u64) -> String {
        partition_col("k0")
            .gt_eq(Operand::Value(Value::UInt64(lo)))
            .and(partition_col("k0").lt(Operand::Value(Value::UInt64(hi))))
            .as_json_str()
            .unwrap()
    }

    #[test]
    fn test_parse_flow_extensions_returns_none_for_non_flow_query() {
        let exts = HashMap::new();
        let parsed = FlowQueryExtensions::parse_flow_extensions(&exts).unwrap();

        assert_eq!(parsed, None);
    }

    #[test]
    fn test_remote_dyn_filter_pushdown_enabled_from_extensions_defaults_true() {
        assert!(remote_dyn_filter_pushdown_enabled_from_extensions(&HashMap::new()).unwrap());
    }

    #[test]
    fn test_remote_dyn_filter_pushdown_enabled_from_extensions_parses_bool() {
        let exts = HashMap::from([(
            QUERY_ENABLE_REMOTE_DYNAMIC_FILTER_PUSHDOWN.to_string(),
            "false".to_string(),
        )]);
        assert!(!remote_dyn_filter_pushdown_enabled_from_extensions(&exts).unwrap());

        let exts = HashMap::from([(
            QUERY_ENABLE_REMOTE_DYNAMIC_FILTER_PUSHDOWN.to_string(),
            "true".to_string(),
        )]);
        assert!(remote_dyn_filter_pushdown_enabled_from_extensions(&exts).unwrap());
    }

    #[test]
    fn test_remote_dyn_filter_pushdown_enabled_from_extensions_rejects_invalid_bool() {
        let exts = HashMap::from([(
            QUERY_ENABLE_REMOTE_DYNAMIC_FILTER_PUSHDOWN.to_string(),
            "invalid".to_string(),
        )]);

        let err = remote_dyn_filter_pushdown_enabled_from_extensions(&exts).unwrap_err();
        assert!(format!("{err}").contains(QUERY_ENABLE_REMOTE_DYNAMIC_FILTER_PUSHDOWN));
    }

    #[test]
    fn test_parse_flow_extensions_memtable_only_success() {
        let exts = HashMap::from([
            (
                FLOW_INCREMENTAL_MODE.to_string(),
                FLOW_INCREMENTAL_MODE_MEMTABLE_ONLY.to_string(),
            ),
            (
                FLOW_INCREMENTAL_AFTER_SEQS.to_string(),
                r#"{"1":10,"2":20}"#.to_string(),
            ),
            (FLOW_RETURN_REGION_SEQ.to_string(), "true".to_string()),
            (FLOW_SINK_TABLE_ID.to_string(), "1024".to_string()),
        ]);

        let parsed = FlowQueryExtensions::parse_flow_extensions(&exts)
            .unwrap()
            .unwrap();
        assert_eq!(
            parsed.incremental_mode,
            Some(FlowIncrementalMode::MemtableOnly)
        );
        assert_eq!(
            parsed.incremental_after_seqs.unwrap(),
            HashMap::from([
                (1, vec![CursorShard::whole_domain(10)]),
                (2, vec![CursorShard::whole_domain(20)]),
            ])
        );
        assert!(parsed.return_region_seq);
        assert_eq!(parsed.sink_table_id, Some(1024));
    }

    #[test]
    fn test_parse_flow_extensions_sequence_range_success() {
        let exts = HashMap::from([
            (
                FLOW_INCREMENTAL_MODE.to_string(),
                FLOW_INCREMENTAL_MODE_SEQUENCE_RANGE.to_string(),
            ),
            (
                FLOW_INCREMENTAL_AFTER_SEQS.to_string(),
                r#"{"1":10}"#.to_string(),
            ),
        ]);

        let parsed = FlowQueryExtensions::parse_flow_extensions(&exts)
            .unwrap()
            .unwrap();
        assert_eq!(
            parsed.incremental_mode,
            Some(FlowIncrementalMode::SequenceRange)
        );
        assert_eq!(
            parsed.incremental_after_seqs,
            Some(HashMap::from([(1, vec![CursorShard::whole_domain(10)])]))
        );
    }

    #[test]
    fn test_parse_flow_extensions_sequence_range_rejects_empty_after_seqs() {
        let exts = HashMap::from([
            (
                FLOW_INCREMENTAL_MODE.to_string(),
                FLOW_INCREMENTAL_MODE_SEQUENCE_RANGE.to_string(),
            ),
            (FLOW_INCREMENTAL_AFTER_SEQS.to_string(), "{}".to_string()),
        ]);

        let err = FlowQueryExtensions::parse_flow_extensions(&exts).unwrap_err();
        assert!(format!("{err}").contains(FLOW_INCREMENTAL_AFTER_SEQS));
    }

    #[test]
    fn test_parse_flow_extensions_sequence_range_requires_after_seqs() {
        let exts = HashMap::from([(
            FLOW_INCREMENTAL_MODE.to_string(),
            FLOW_INCREMENTAL_MODE_SEQUENCE_RANGE.to_string(),
        )]);

        let err = FlowQueryExtensions::parse_flow_extensions(&exts).unwrap_err();
        assert!(format!("{err}").contains(FLOW_INCREMENTAL_AFTER_SEQS));
    }

    #[test]
    fn test_parse_flow_extensions_mode_requires_after_seqs() {
        let exts = HashMap::from([(
            FLOW_INCREMENTAL_MODE.to_string(),
            FLOW_INCREMENTAL_MODE_MEMTABLE_ONLY.to_string(),
        )]);

        let err = FlowQueryExtensions::parse_flow_extensions(&exts).unwrap_err();
        assert!(format!("{err}").contains(FLOW_INCREMENTAL_AFTER_SEQS));
    }

    #[test]
    fn test_parse_flow_extensions_invalid_mode() {
        let exts = HashMap::from([(FLOW_INCREMENTAL_MODE.to_string(), "foo".to_string())]);

        let err = FlowQueryExtensions::parse_flow_extensions(&exts).unwrap_err();
        assert!(format!("{err}").contains(FLOW_INCREMENTAL_MODE));
    }

    #[test]
    fn test_parse_flow_extensions_invalid_after_seqs_json() {
        let exts = HashMap::from([
            (
                FLOW_INCREMENTAL_MODE.to_string(),
                FLOW_INCREMENTAL_MODE_MEMTABLE_ONLY.to_string(),
            ),
            (
                FLOW_INCREMENTAL_AFTER_SEQS.to_string(),
                "not-json".to_string(),
            ),
        ]);

        let err = FlowQueryExtensions::parse_flow_extensions(&exts).unwrap_err();
        assert!(format!("{err}").contains(FLOW_INCREMENTAL_AFTER_SEQS));
    }

    #[test]
    fn test_parse_flow_extensions_after_seqs_string_values() {
        let exts = HashMap::from([(
            FLOW_INCREMENTAL_AFTER_SEQS.to_string(),
            r#"{"1":"10","2":"20"}"#.to_string(),
        )]);

        let parsed = FlowQueryExtensions::parse_flow_extensions(&exts)
            .unwrap()
            .unwrap();
        assert_eq!(
            parsed.incremental_after_seqs.unwrap(),
            HashMap::from([
                (1, vec![CursorShard::whole_domain(10)]),
                (2, vec![CursorShard::whole_domain(20)]),
            ])
        );
    }

    #[test]
    fn test_parse_flow_extensions_after_seqs_invalid_value_type() {
        let exts = HashMap::from([(
            FLOW_INCREMENTAL_AFTER_SEQS.to_string(),
            r#"{"1":true}"#.to_string(),
        )]);

        let err = FlowQueryExtensions::parse_flow_extensions(&exts).unwrap_err();
        assert!(format!("{err}").contains(FLOW_INCREMENTAL_AFTER_SEQS));
    }

    #[test]
    fn test_parse_flow_extensions_invalid_sink_table_id() {
        let exts = HashMap::from([(FLOW_SINK_TABLE_ID.to_string(), "x".to_string())]);

        let err = FlowQueryExtensions::parse_flow_extensions(&exts).unwrap_err();
        assert!(format!("{err}").contains(FLOW_SINK_TABLE_ID));
    }

    #[test]
    fn test_validate_for_scan_missing_source_region() {
        let source_region_id = RegionId::new(100, 2);
        let existing_region_id = RegionId::new(100, 1);
        let exts = HashMap::from([
            (
                FLOW_INCREMENTAL_MODE.to_string(),
                FLOW_INCREMENTAL_MODE_MEMTABLE_ONLY.to_string(),
            ),
            (
                FLOW_INCREMENTAL_AFTER_SEQS.to_string(),
                format!(r#"{{"{}":10}}"#, existing_region_id.as_u64()),
            ),
        ]);

        let parsed = FlowQueryExtensions::parse_flow_extensions(&exts)
            .unwrap()
            .unwrap();
        let err = parsed.validate_for_scan(source_region_id).unwrap_err();
        assert!(format!("{err}").contains("Missing region"));
    }

    #[test]
    fn test_validate_for_scan_sink_table_excluded() {
        let source_region_id = RegionId::new(1024, 1);
        let exts = HashMap::from([
            (
                FLOW_INCREMENTAL_MODE.to_string(),
                FLOW_INCREMENTAL_MODE_MEMTABLE_ONLY.to_string(),
            ),
            (
                FLOW_INCREMENTAL_AFTER_SEQS.to_string(),
                format!(r#"{{"{}":10}}"#, source_region_id.as_u64()),
            ),
            (FLOW_SINK_TABLE_ID.to_string(), "1024".to_string()),
        ]);

        let parsed = FlowQueryExtensions::parse_flow_extensions(&exts)
            .unwrap()
            .unwrap();
        let apply_incremental = parsed.validate_for_scan(source_region_id).unwrap();
        assert!(!apply_incremental);
    }

    #[test]
    fn test_should_collect_region_watermark_defaults_false() {
        let parsed = FlowQueryExtensions::default();
        assert!(!parsed.should_collect_region_watermark());
    }

    #[test]
    fn test_should_collect_region_watermark_true_for_return_region_seq() {
        let parsed = FlowQueryExtensions {
            return_region_seq: true,
            ..Default::default()
        };
        assert!(parsed.should_collect_region_watermark());
    }

    #[test]
    fn test_should_collect_region_watermark_true_for_incremental_query() {
        let parsed = FlowQueryExtensions {
            incremental_after_seqs: Some(HashMap::from([(1, vec![CursorShard::whole_domain(10)])])),
            ..Default::default()
        };
        assert!(parsed.should_collect_region_watermark());
    }

    #[test]
    fn test_should_collect_region_watermark_from_extensions() {
        let exts = HashMap::from([(FLOW_RETURN_REGION_SEQ.to_string(), "true".to_string())]);
        assert!(should_collect_region_watermark_from_extensions(&exts));

        let exts = HashMap::from([(
            FLOW_INCREMENTAL_AFTER_SEQS.to_string(),
            r#"{"1":10}"#.to_string(),
        )]);
        assert!(should_collect_region_watermark_from_extensions(&exts));

        let exts = HashMap::from([(FLOW_RETURN_REGION_SEQ.to_string(), "false".to_string())]);
        assert!(!should_collect_region_watermark_from_extensions(&exts));
        assert!(!should_collect_region_watermark_from_extensions(
            &HashMap::new()
        ));
    }

    #[test]
    fn test_parse_flow_extensions_return_region_seq_only_returns_some() {
        let exts = HashMap::from([(FLOW_RETURN_REGION_SEQ.to_string(), "true".to_string())]);

        let parsed = FlowQueryExtensions::parse_flow_extensions(&exts)
            .unwrap()
            .unwrap();

        assert!(parsed.return_region_seq);
    }

    #[test]
    fn test_parse_flow_extensions_sink_table_only_returns_some() {
        let exts = HashMap::from([(FLOW_SINK_TABLE_ID.to_string(), "1024".to_string())]);

        let parsed = FlowQueryExtensions::parse_flow_extensions(&exts)
            .unwrap()
            .unwrap();

        assert_eq!(parsed.sink_table_id, Some(1024));
    }

    #[test]
    fn test_parse_flow_extensions_incremental_after_seqs_only_returns_some() {
        let exts = HashMap::from([(
            FLOW_INCREMENTAL_AFTER_SEQS.to_string(),
            r#"{"1":10}"#.to_string(),
        )]);

        let parsed = FlowQueryExtensions::parse_flow_extensions(&exts)
            .unwrap()
            .unwrap();

        assert_eq!(
            parsed.incremental_after_seqs,
            Some(HashMap::from([(1, vec![CursorShard::whole_domain(10)])]))
        );
    }

    #[test]
    fn test_parse_flow_extensions_after_seqs_slice_form() {
        // The expression-keyed shape: the partition expressions of one region, each with
        // its own cursor.
        let low = slice_expr(0, 50);
        let high = slice_expr(50, 100);
        let value = format!(
            r#"{{"7":{{"version":2,"shards":[{{"expr":{},"seq":10}},{{"expr":{},"seq":3}},{{"expr":null,"seq":4}}]}}}}"#,
            serde_json::to_string(&low).unwrap(),
            serde_json::to_string(&high).unwrap()
        );
        let exts = HashMap::from([(FLOW_INCREMENTAL_AFTER_SEQS.to_string(), value)]);

        let parsed = FlowQueryExtensions::parse_flow_extensions(&exts)
            .unwrap()
            .unwrap();

        assert_eq!(
            parsed.incremental_after_seqs,
            Some(HashMap::from([(
                7,
                vec![
                    CursorShard::new(Some(low), 10),
                    CursorShard::new(Some(high), 3),
                    CursorShard::whole_domain(4),
                ]
            )]))
        );
        // A region-level read has to start after the least advanced slice.
        assert_eq!(Some(3), parsed.region_cursor(7));
        assert_eq!(None, parsed.region_cursor(8));
    }

    /// A slice that states no upper bound decodes as the unbounded slice it is, under the one
    /// version the format has.
    #[test]
    fn test_parse_flow_extensions_after_seqs_unbounded_slice_reads() {
        let exts = HashMap::from([(
            FLOW_INCREMENTAL_AFTER_SEQS.to_string(),
            r#"{"7":{"version":2,"shards":[{"expr":null,"seq":10},{"expr":null,"seq":4}]}}"#
                .to_string(),
        )]);
        let parsed = FlowQueryExtensions::parse_flow_extensions(&exts)
            .unwrap()
            .unwrap();
        assert_eq!(
            parsed.incremental_after_seqs,
            Some(HashMap::from([(
                7,
                vec![CursorShard::whole_domain(10), CursorShard::whole_domain(4)]
            )])),
            "a slice that states no upper bound is the unbounded slice"
        );
        assert_eq!(
            None,
            parsed.incremental_after_seqs.as_ref().unwrap()[&7][0].until
        );
    }

    /// A slice that states an upper bound carries it in the same version: `seq < number <=
    /// until` decodes as the bounded slice it states, and an invalid bound is refused at the
    /// boundary like `seq`.
    #[test]
    fn test_parse_flow_extensions_after_seqs_bounded_slice_reads() {
        let exts = HashMap::from([(
            FLOW_INCREMENTAL_AFTER_SEQS.to_string(),
            r#"{"7":{"version":2,"shards":[{"expr":null,"seq":10,"until":20}]}}"#.to_string(),
        )]);
        let parsed = FlowQueryExtensions::parse_flow_extensions(&exts)
            .unwrap()
            .unwrap();
        assert_eq!(
            parsed.incremental_after_seqs,
            Some(HashMap::from([(
                7,
                vec![CursorShard::whole_domain(10).with_until(20)]
            )]))
        );

        // An upper bound that is not a sequence is refused at the boundary, like `seq`.
        for value in [
            r#"{"7":{"version":2,"shards":[{"expr":null,"seq":10,"until":"nope"}]}}"#,
            r#"{"7":{"version":2,"shards":[{"expr":null,"seq":10,"until":-1}]}}"#,
        ] {
            let exts =
                HashMap::from([(FLOW_INCREMENTAL_AFTER_SEQS.to_string(), value.to_string())]);
            let err = FlowQueryExtensions::parse_flow_extensions(&exts).unwrap_err();
            assert!(
                format!("{err}").contains(FLOW_INCREMENTAL_AFTER_SEQS),
                "{value}: {err}"
            );
        }
    }

    /// A slice covers `seq < number <= until`, so an `until` below its own `seq` states an
    /// empty delta and is refused at the boundary instead of being clamped or swapped: a read
    /// bounds a file with `GtLtEq { min, max }` and `max < min` selects no row at all, while a
    /// file several slices match can still admit rows through another slice's smaller minimum,
    /// so the wrong slice would look like a plausible but wrong answer instead of a failure.
    #[test]
    fn test_parse_flow_extensions_after_seqs_rejects_upper_bound_below_cursor() {
        let exts = HashMap::from([(
            FLOW_INCREMENTAL_AFTER_SEQS.to_string(),
            r#"{"7":{"version":2,"shards":[{"expr":null,"seq":10,"until":9}]}}"#.to_string(),
        )]);
        let err = FlowQueryExtensions::parse_flow_extensions(&exts).unwrap_err();
        let message = format!("{err}");
        assert!(
            message.contains("region 7"),
            "the failure names the region: {message}"
        );
        assert!(
            message.contains("upper bound 9"),
            "the failure names the stated bound: {message}"
        );
        assert!(
            message.contains("consumed sequence 10"),
            "the failure names the slice's own cursor: {message}"
        );
        assert!(
            message.contains("at least its seq"),
            "the failure names the ordering rule: {message}"
        );

        // The bound is refused whatever the distance is.
        for (value, region) in [
            (
                r#"{"8":{"version":2,"shards":[{"expr":null,"seq":10,"until":9}]}}"#,
                "region 8",
            ),
            (
                r#"{"9":{"version":2,"shards":[{"expr":null,"seq":5,"until":0}]}}"#,
                "region 9",
            ),
        ] {
            let exts =
                HashMap::from([(FLOW_INCREMENTAL_AFTER_SEQS.to_string(), value.to_string())]);
            let err = FlowQueryExtensions::parse_flow_extensions(&exts).unwrap_err();
            let message = format!("{err}");
            assert!(
                message.contains(region) && message.contains("at least its seq"),
                "{value}: {message}"
            );
        }

        // `until == seq` is the boundary the rule allows: the slice states the bound it
        // covers, and no row of it is above the cursor.
        let exts = HashMap::from([(
            FLOW_INCREMENTAL_AFTER_SEQS.to_string(),
            r#"{"7":{"version":2,"shards":[{"expr":null,"seq":10,"until":10}]}}"#.to_string(),
        )]);
        let parsed = FlowQueryExtensions::parse_flow_extensions(&exts)
            .unwrap()
            .unwrap();
        assert_eq!(
            parsed.incremental_after_seqs,
            Some(HashMap::from([(
                7,
                vec![CursorShard::whole_domain(10).with_until(10)]
            )])),
            "a bound equal to the cursor states an empty window and is not a contradiction"
        );
    }

    /// An expression this build cannot read back is rejected at the boundary rather than
    /// silently widened to the whole domain by the scan that receives it.
    #[test]
    fn test_parse_flow_extensions_after_seqs_rejects_invalid_expression() {
        for value in [
            // Not a partition expression at all.
            r#"{"7":{"version":2,"shards":[{"expr":"not-json","seq":1}]}}"#.to_string(),
            // A bound shape this build does not treat as an expression.
            r#"{"7":{"version":2,"shards":[{"expr":"","seq":1}]}}"#.to_string(),
            // An expression identity must be stated, an absent key is not a whole domain.
            r#"{"7":{"version":2,"shards":[{"seq":1}]}}"#.to_string(),
            // The sequence must be stated.
            r#"{"7":{"version":2,"shards":[{"expr":null}]}}"#.to_string(),
            // A non-string, non-null expression identity.
            r#"{"7":{"version":2,"shards":[{"expr":7,"seq":1}]}}"#.to_string(),
        ] {
            let exts = HashMap::from([(FLOW_INCREMENTAL_AFTER_SEQS.to_string(), value.clone())]);
            let err = FlowQueryExtensions::parse_flow_extensions(&exts).unwrap_err();
            assert!(
                format!("{err}").contains(FLOW_INCREMENTAL_AFTER_SEQS),
                "{value}: {err}"
            );
        }
    }

    /// A version this build does not know is refused, and the version marker is mandatory for
    /// an object value: a cursor is never guessed at from a shape that states its own format.
    #[test]
    fn test_parse_flow_extensions_after_seqs_rejects_unknown_version() {
        for value in [
            // A version this build does not know.
            r#"{"7":{"version":4,"shards":[{"expr":null,"seq":1}]}}"#.to_string(),
            // No version marker at all.
            r#"{"7":{"shards":[{"expr":null,"seq":1}]}}"#.to_string(),
            // The version marker is not a number.
            r#"{"7":{"version":"2","shards":[{"expr":null,"seq":1}]}}"#.to_string(),
            // Shards are missing.
            r#"{"7":{"version":2}}"#.to_string(),
        ] {
            let exts = HashMap::from([(FLOW_INCREMENTAL_AFTER_SEQS.to_string(), value.clone())]);
            let err = FlowQueryExtensions::parse_flow_extensions(&exts).unwrap_err();
            assert!(
                format!("{err}").contains(FLOW_INCREMENTAL_AFTER_SEQS),
                "{value}: {err}"
            );
        }
    }

    #[test]
    fn test_parse_flow_extensions_after_seqs_region_form_is_the_full_slice() {
        // The region-keyed shape is exactly the region's complete value domain, so a
        // reader that only knows it keeps working.
        let exts = HashMap::from([(
            FLOW_INCREMENTAL_AFTER_SEQS.to_string(),
            r#"{"7":10}"#.to_string(),
        )]);

        let parsed = FlowQueryExtensions::parse_flow_extensions(&exts)
            .unwrap()
            .unwrap();

        assert_eq!(
            parsed.incremental_after_seqs,
            Some(HashMap::from([(7, vec![CursorShard::whole_domain(10)])]))
        );
        assert_eq!(Some(10), parsed.region_cursor(7));
    }

    /// A shape this build does not know is refused, never guessed at: the numeric
    /// `[[lo, hi, seq], ...]` projection cannot name an expression, so it is not a cursor
    /// this build can act on.
    #[test]
    fn test_parse_flow_extensions_after_seqs_rejects_unknown_shapes() {
        for value in [
            // The numeric slice projection: no expression identity to act on.
            r#"{"7":[[0,50,10],[50,100,3]]}"#,
            r#"{"7":[10]}"#,
            r#"{"7":[]}"#,
            // Neither a region-keyed cursor, an expression-keyed one, nor a stateable
            // cursor at all.
            r#"{"7":true}"#,
            r#"{"7":null}"#,
        ] {
            let exts =
                HashMap::from([(FLOW_INCREMENTAL_AFTER_SEQS.to_string(), value.to_string())]);
            let err = FlowQueryExtensions::parse_flow_extensions(&exts).unwrap_err();
            assert!(
                format!("{err}").contains(FLOW_INCREMENTAL_AFTER_SEQS),
                "{value}: {err}"
            );
        }
    }

    /// The writer and the reader agree on the one surviving format: a region with two slices,
    /// one bounded and one not, encodes to the expression-keyed shape and decodes back to the
    /// very cursors it was written from.
    #[test]
    fn test_encode_incremental_after_seqs_round_trips() {
        let low = slice_expr(0, 50);
        let high = slice_expr(50, 100);
        let low_json = serde_json::to_string(&low).unwrap();
        let high_json = serde_json::to_string(&high).unwrap();
        let cursors = BTreeMap::from([
            (1, vec![CursorShard::whole_domain(10)]),
            (
                2,
                vec![
                    CursorShard::new(Some(low.clone()), 10).with_until(15),
                    CursorShard::new(Some(high.clone()), 3),
                ],
            ),
        ]);

        let json = encode_incremental_after_seqs(&cursors).unwrap();
        // Compared as JSON: the order of the object keys is serde_json's business, the
        // identity, the sequence and the optional bound are what the wire states. Every region
        // states the same version, and only a slice that carries a bound states `until`.
        assert_eq!(
            serde_json::json!({
                "1": {"version": 2, "shards": [{"expr": null, "seq": 10}]},
                "2": {
                    "version": 2,
                    "shards": [
                        {"expr": low, "seq": 10, "until": 15},
                        {"expr": high, "seq": 3},
                    ]
                },
            }),
            serde_json::from_str::<serde_json::Value>(&json).unwrap()
        );

        // The bytes themselves: `serde_json` keeps object keys sorted, so "shards" precedes
        // "version" and "expr" precedes "seq".
        assert_eq!(
            format!(
                r#"{{"1":{{"shards":[{{"expr":null,"seq":10}}],"version":2}},"2":{{"shards":[{{"expr":{},"seq":10,"until":15}},{{"expr":{},"seq":3}}],"version":2}}}}"#,
                low_json, high_json,
            ),
            json
        );

        // The reader of this build reads it back as the cursors it was written from.
        let exts = HashMap::from([(FLOW_INCREMENTAL_AFTER_SEQS.to_string(), json)]);
        let parsed = FlowQueryExtensions::parse_flow_extensions(&exts)
            .unwrap()
            .unwrap();
        assert_eq!(
            parsed.incremental_after_seqs,
            Some(
                cursors
                    .into_iter()
                    .collect::<HashMap<u64, Vec<CursorShard>>>()
            )
        );
    }

    // --- scheduled time helper tests ---

    #[test]
    fn test_parse_scheduled_time_millis_absent() {
        let exts = HashMap::new();
        assert_eq!(parse_scheduled_time_millis(&exts).unwrap(), None);
    }

    #[test]
    fn test_parse_scheduled_time_millis_valid() {
        let exts = HashMap::from([(
            FLOW_SCHEDULED_TIME_MILLIS.to_string(),
            "1700000000000".to_string(),
        )]);
        assert_eq!(
            parse_scheduled_time_millis(&exts).unwrap(),
            Some(1700000000000)
        );
    }

    #[test]
    fn test_parse_scheduled_time_millis_malformed() {
        let exts = HashMap::from([(
            FLOW_SCHEDULED_TIME_MILLIS.to_string(),
            "not-a-number".to_string(),
        )]);
        let err = parse_scheduled_time_millis(&exts).unwrap_err();
        assert!(format!("{err}").contains(FLOW_SCHEDULED_TIME_MILLIS));
    }

    #[test]
    fn test_parse_scheduled_time_datetime_valid() {
        let exts = HashMap::from([(
            FLOW_SCHEDULED_TIME_MILLIS.to_string(),
            "1700000000000".to_string(),
        )]);
        let dt = parse_scheduled_time_datetime(&exts).unwrap().unwrap();
        assert_eq!(dt.timestamp_millis(), 1700000000000);
    }

    #[test]
    fn test_parse_scheduled_time_datetime_negative_millis() {
        let exts = HashMap::from([(FLOW_SCHEDULED_TIME_MILLIS.to_string(), "-1".to_string())]);
        let dt = parse_scheduled_time_datetime(&exts).unwrap().unwrap();
        assert_eq!(dt.timestamp_millis(), -1);
    }

    #[test]
    fn test_parse_scheduled_time_datetime_out_of_range() {
        // i64::MAX millis is well beyond representable chrono::DateTime range
        let exts = HashMap::from([(FLOW_SCHEDULED_TIME_MILLIS.to_string(), i64::MAX.to_string())]);
        let err = parse_scheduled_time_datetime(&exts).unwrap_err();
        assert!(format!("{err}").contains("Out-of-range"));
    }
}

#[cfg(test)]
mod query_options_tests {
    use super::*;

    #[test]
    fn test_parse_spill_options_from_toml() {
        let toml_str = r#"
experimental_spill_mode = "custom"
experimental_spill_path = "/tmp/spill"
experimental_spill_max_temp_directory_size = "50GiB"
experimental_spill_compression = "zstd"
experimental_memory_pool_policy = "fair"
"#;
        let opts: QueryOptions = toml::from_str(toml_str).unwrap();
        assert_eq!(opts.experimental_spill_mode, QuerySpillMode::Custom);
        assert_eq!(
            opts.experimental_spill_path,
            Some(PathBuf::from("/tmp/spill"))
        );
        assert_eq!(
            opts.experimental_spill_max_temp_directory_size,
            ReadableSize::gb(50)
        );
        assert_eq!(
            opts.experimental_spill_compression,
            QuerySpillCompression::Zstd
        );
        assert_eq!(
            opts.experimental_memory_pool_policy,
            QueryMemoryPoolPolicy::Fair
        );
    }

    /// The native PromQL route is opt-in: a query that states nothing keeps the plan the
    /// existing planner builds.
    ///
    /// The per-request `query.experimental_native_promql` extension is read by
    /// `query::promql::planner::native::native_promql_requested`; this default is the other
    /// half of that gate today, and it becomes the only gate for a query that states nothing
    /// once #10262 lands. Flipping it would expose the native route to every query that sets
    /// the extension.
    #[test]
    fn test_native_promql_is_off_by_default() {
        assert!(!QueryOptions::default().experimental_native_promql);
    }

    #[test]
    fn test_parse_invalid_spill_option_values() {
        for toml_str in [
            r#"experimental_spill_mode = "invalid""#,
            r#"experimental_spill_compression = "gzip""#,
            r#"experimental_memory_pool_policy = "none""#,
        ] {
            assert!(
                toml::from_str::<QueryOptions>(toml_str).is_err(),
                "{toml_str}"
            );
        }
    }
}
