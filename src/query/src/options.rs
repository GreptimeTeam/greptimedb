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

use std::collections::{BTreeSet, HashMap};
use std::path::PathBuf;

use chrono::{DateTime, Utc};
use common_base::memory_limit::MemoryLimit;
use common_base::readable_size::ReadableSize;
use datafusion::config::{ConfigEntry, ConfigExtension, ExtensionOptions};
use serde::{Deserialize, Serialize};
use session::context::QueryContextRef;
use store_api::storage::RegionId;
use table::metadata::TableId;

use crate::error::{Error, InvalidQueryContextExtensionSnafu, Result};

pub const FLOW_INCREMENTAL_AFTER_SEQS: &str = "flow.incremental_after_seqs";
pub const FLOW_INCREMENTAL_MODE: &str = "flow.incremental_mode";
pub const FLOW_RETURN_REGION_SEQ: &str = "flow.return_region_seq";
/// Legacy sink table id used to keep the sink table out of the flow snapshot.
/// Kept for old callers only; new callers use [`FLOW_SNAPSHOT_TABLE_IDS`].
pub const FLOW_SINK_TABLE_ID: &str = "flow.sink_table_id";
/// Table-scoped snapshot read set of a Flow query.
///
/// The wire value is a JSON array of numeric table ids. Presence of this key
/// (including an empty array) selects the table-scoped semantics: every table in
/// the set is read as one bound snapshot, every table referenced by
/// [`FLOW_INCREMENTAL_AFTER_SEQS`] is replayed incrementally, and any other table
/// keeps plain query semantics. When the key is absent, the legacy
/// [`FLOW_SINK_TABLE_ID`] semantics apply.
pub const FLOW_SNAPSHOT_TABLE_IDS: &str = "flow.snapshot_table_ids";
/// Flow scheduler binding for the logical time of one scheduled attempt.
/// Query planning, SQL/TQL parsing, range-select rewrite and DataFusion
/// execution read this extension so `now()` is stable for the whole attempt.
pub const FLOW_SCHEDULED_TIME_MILLIS: &str = "flow.scheduled_time_millis";
/// Enable by default, set to false to explicitly disable.
pub const QUERY_ENABLE_REMOTE_DYNAMIC_FILTER_PUSHDOWN: &str =
    "query.enable_remote_dynamic_filter_pushdown";

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

/// Query engine config
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct QueryOptions {
    /// Parallelism of query engine. Default to 0, which implies the number of logical CPUs.
    pub parallelism: usize,
    /// Whether to allow query fallback when push down fails.
    pub allow_query_fallback: bool,
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

/// How one table participates in a single Flow query.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlowTableScanKind {
    /// The table is read as one bound snapshot.
    Snapshot,
    /// The table is replayed incrementally from its after-sequence bounds.
    Incremental,
    /// The table is not part of the flow read and keeps plain query semantics.
    Plain,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct FlowQueryExtensions {
    /// Maps region id -> lower exclusive sequence bound for incremental reads.
    pub incremental_after_seqs: Option<HashMap<u64, u64>>,
    /// Incremental read mode requested by the caller.
    pub incremental_mode: Option<FlowIncrementalMode>,
    /// Whether the caller expects per-region watermark metadata in terminal metrics.
    pub return_region_seq: bool,
    /// Table-scoped snapshot read set.
    ///
    /// `None` keeps the legacy [`FLOW_SINK_TABLE_ID`] semantics; `Some(_)`
    /// (including an empty set) selects the table-scoped semantics described by
    /// [`FLOW_SNAPSHOT_TABLE_IDS`].
    pub snapshot_table_ids: Option<BTreeSet<TableId>>,
    /// Legacy sink table id used to distinguish source scans from sink reads.
    /// Only honored when [`Self::snapshot_table_ids`] is absent.
    pub sink_table_id: Option<TableId>,
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
            || extensions.contains_key(FLOW_SINK_TABLE_ID)
            || extensions.contains_key(FLOW_SNAPSHOT_TABLE_IDS);

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

        let snapshot_table_ids = extensions
            .get(FLOW_SNAPSHOT_TABLE_IDS)
            .map(|value| parse_snapshot_table_ids(value.as_str()))
            .transpose()?;

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

        // The legacy sink key and the table-scoped snapshot key encode the same
        // intent in two incompatible ways; refuse to guess which one wins.
        if snapshot_table_ids.is_some() && sink_table_id.is_some() {
            return Err(invalid_query_context_extension(format!(
                "{} and {} cannot be set together; use {} for table-scoped snapshot reads.",
                FLOW_SINK_TABLE_ID, FLOW_SNAPSHOT_TABLE_IDS, FLOW_SNAPSHOT_TABLE_IDS
            )));
        }

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

        if let Some(snapshot_table_ids) = &snapshot_table_ids {
            let incremental_table_ids = incremental_table_ids(incremental_after_seqs.as_ref());
            if let Some(overlap) = snapshot_table_ids
                .iter()
                .find(|table_id| incremental_table_ids.contains(table_id))
            {
                return Err(invalid_query_context_extension(format!(
                    "Table {} appears in both {} and {}; snapshot and incremental tables must be disjoint.",
                    overlap, FLOW_SNAPSHOT_TABLE_IDS, FLOW_INCREMENTAL_AFTER_SEQS
                )));
            }
        }

        Ok(Some(Self {
            incremental_after_seqs,
            incremental_mode,
            return_region_seq,
            snapshot_table_ids,
            sink_table_id,
        }))
    }

    /// Returns whether the table-scoped snapshot read semantics are active.
    ///
    /// An absent [`FLOW_SNAPSHOT_TABLE_IDS`] key keeps the legacy sink-table
    /// semantics; a present key (even `[]`) selects per-table classification.
    pub fn is_table_scoped(&self) -> bool {
        self.snapshot_table_ids.is_some()
    }

    /// Returns whether the snapshot read set is non-empty.
    fn is_snapshot_table_set_non_empty(&self) -> bool {
        self.snapshot_table_ids
            .as_ref()
            .is_some_and(|table_ids| !table_ids.is_empty())
    }

    /// Returns whether `table_id` must be read as one bound snapshot.
    pub fn is_snapshot_table(&self, table_id: TableId) -> bool {
        self.snapshot_table_ids
            .as_ref()
            .is_some_and(|table_ids| table_ids.contains(&table_id))
    }

    /// Returns the table ids that participate in incremental replay, derived from
    /// the [`FLOW_INCREMENTAL_AFTER_SEQS`] region keys.
    pub fn incremental_table_ids(&self) -> BTreeSet<TableId> {
        incremental_table_ids(self.incremental_after_seqs.as_ref())
    }

    /// Returns whether `table_id` is replayed incrementally, derived from the
    /// [`FLOW_INCREMENTAL_AFTER_SEQS`] region keys.
    fn is_incremental_table(&self, table_id: TableId) -> bool {
        self.incremental_after_seqs.as_ref().is_some_and(|seqs| {
            seqs.keys()
                .any(|region_id| RegionId::from_u64(*region_id).table_id() == table_id)
        })
    }

    /// Classifies how `table_id` participates in this Flow query.
    ///
    /// Table-scoped semantics decide purely by membership: snapshot tables are
    /// listed in [`FLOW_SNAPSHOT_TABLE_IDS`], incrementally replayed tables are
    /// referenced by [`FLOW_INCREMENTAL_AFTER_SEQS`], and everything else is a
    /// plain read. The legacy path keeps the sink table plain and reads the
    /// remaining tables as flow sources.
    pub fn classify_table(&self, table_id: TableId) -> FlowTableScanKind {
        if self.is_table_scoped() {
            if self.is_snapshot_table(table_id) {
                FlowTableScanKind::Snapshot
            } else if self.is_incremental_table(table_id) {
                FlowTableScanKind::Incremental
            } else {
                FlowTableScanKind::Plain
            }
        } else if self.sink_table_id == Some(table_id) {
            FlowTableScanKind::Plain
        } else if self.incremental_mode.is_some() || self.is_incremental_table(table_id) {
            FlowTableScanKind::Incremental
        } else {
            FlowTableScanKind::Snapshot
        }
    }

    /// Returns the exclusive lower sequence bound of an incrementally replayed region.
    ///
    /// Fails closed when the region belongs to a table classified as
    /// [`FlowTableScanKind::Incremental`] but carries no bound.
    pub fn incremental_after_seq_for_region(&self, source_region_id: RegionId) -> Result<u64> {
        self.incremental_after_seqs
            .as_ref()
            .and_then(|seqs| seqs.get(&source_region_id.as_u64()).copied())
            .ok_or_else(|| {
                invalid_query_context_extension(format!(
                    "Missing region {} in {} for incrementally replayed table {}.",
                    source_region_id,
                    FLOW_INCREMENTAL_AFTER_SEQS,
                    source_region_id.table_id()
                ))
            })
    }

    /// Legacy sink-key scan validation, kept for old callers only.
    ///
    /// New callers classify tables with [`Self::classify_table`] and use
    /// [`Self::incremental_after_seq_for_region`] for incremental tables. Unlike
    /// table-scoped classification, this legacy check fails closed for *any*
    /// scanned region missing from [`FLOW_INCREMENTAL_AFTER_SEQS`] whenever an
    /// incremental mode is set, and treats the legacy sink table as plain.
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
        if self.is_table_scoped() {
            return self.is_snapshot_table_set_non_empty()
                || self
                    .incremental_after_seqs
                    .as_ref()
                    .is_some_and(|seqs| !seqs.is_empty());
        }

        should_collect_region_watermark(
            self.return_region_seq,
            self.incremental_after_seqs.is_some(),
        )
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
    let has_snapshot_table_ids = extensions.contains_key(FLOW_SNAPSHOT_TABLE_IDS);

    should_collect_region_watermark(
        return_region_seq,
        has_incremental_after_seqs || has_snapshot_table_ids,
    )
}

/// Returns whether the region's table participates in terminal region watermark
/// collection for the given raw Flow query extensions.
///
/// Snapshot-read and incrementally replayed tables participate; plain reads
/// (including the legacy sink table and unlisted tables) do not. Malformed
/// extensions yield `false`, so a broken request can never fabricate a proof.
pub fn should_collect_region_watermark_for_region_from_extensions(
    extensions: &HashMap<String, String>,
    region_id: RegionId,
) -> bool {
    if !should_collect_region_watermark_from_extensions(extensions) {
        return false;
    }

    FlowQueryExtensions::parse_flow_extensions(extensions)
        .ok()
        .flatten()
        .is_some_and(|flow| flow.classify_table(region_id.table_id()) != FlowTableScanKind::Plain)
}

fn should_collect_region_watermark(
    return_region_seq: bool,
    has_incremental_after_seqs: bool,
) -> bool {
    return_region_seq || has_incremental_after_seqs
}

fn incremental_table_ids(after_seqs: Option<&HashMap<u64, u64>>) -> BTreeSet<TableId> {
    after_seqs
        .into_iter()
        .flat_map(|seqs| seqs.keys())
        .map(|region_id| RegionId::from_u64(*region_id).table_id())
        .collect()
}

/// Parses the table-scoped snapshot read set from a JSON array of numeric table ids.
fn parse_snapshot_table_ids(value: &str) -> Result<BTreeSet<TableId>> {
    let raw = serde_json::from_str::<Vec<serde_json::Value>>(value).map_err(|e| {
        invalid_query_context_extension(format!(
            "Invalid JSON for {}: {} ({})",
            FLOW_SNAPSHOT_TABLE_IDS, value, e
        ))
    })?;

    raw.into_iter()
        .map(|raw_table_id| {
            let raw_table_id = match raw_table_id {
                serde_json::Value::Number(num) => num,
                _ => {
                    return Err(invalid_query_context_extension(format!(
                        "Invalid table id type in {}: {}; expected a numeric table id",
                        FLOW_SNAPSHOT_TABLE_IDS, raw_table_id
                    )));
                }
            };

            let table_id = raw_table_id.as_u64().ok_or_else(|| {
                invalid_query_context_extension(format!(
                    "Invalid table id in {}: {}",
                    FLOW_SNAPSHOT_TABLE_IDS, raw_table_id
                ))
            })?;

            TableId::try_from(table_id).map_err(|_| {
                invalid_query_context_extension(format!(
                    "Table id {} in {} overflows the table id range",
                    table_id, FLOW_SNAPSHOT_TABLE_IDS
                ))
            })
        })
        .collect()
}

fn parse_incremental_after_seqs(value: &str) -> Result<HashMap<u64, u64>> {
    let raw = serde_json::from_str::<HashMap<String, serde_json::Value>>(value).map_err(|e| {
        invalid_query_context_extension(format!(
            "Invalid JSON for {}: {} ({})",
            FLOW_INCREMENTAL_AFTER_SEQS, value, e
        ))
    })?;

    raw.into_iter()
        .map(|(region_id, raw_seq)| {
            let region_id = region_id.parse::<u64>().map_err(|_| {
                invalid_query_context_extension(format!(
                    "Invalid region id in {}: {}",
                    FLOW_INCREMENTAL_AFTER_SEQS, region_id
                ))
            })?;

            let seq = match raw_seq {
                serde_json::Value::Number(num) => num.as_u64().ok_or_else(|| {
                    invalid_query_context_extension(format!(
                        "Invalid sequence value in {} for region {}: {}",
                        FLOW_INCREMENTAL_AFTER_SEQS, region_id, num
                    ))
                })?,
                serde_json::Value::String(s) => s.parse::<u64>().map_err(|_| {
                    invalid_query_context_extension(format!(
                        "Invalid sequence string in {} for region {}: {}",
                        FLOW_INCREMENTAL_AFTER_SEQS, region_id, s
                    ))
                })?,
                _ => {
                    return Err(invalid_query_context_extension(format!(
                        "Invalid sequence value type in {} for region {}",
                        FLOW_INCREMENTAL_AFTER_SEQS, region_id
                    )));
                }
            };

            Ok((region_id, seq))
        })
        .collect()
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
    use super::*;

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
            HashMap::from([(1, 10), (2, 20)])
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
            Some(HashMap::from([(1, 10)]))
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
            HashMap::from([(1, 10), (2, 20)])
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
            incremental_after_seqs: Some(HashMap::from([(1, 10)])),
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
            Some(HashMap::from([(1, 10)]))
        );
    }

    // --- table-scoped snapshot read tests ---

    #[test]
    fn test_parse_flow_extensions_snapshot_table_ids_absent_keeps_legacy_semantics() {
        let exts = HashMap::from([(FLOW_RETURN_REGION_SEQ.to_string(), "true".to_string())]);

        let parsed = FlowQueryExtensions::parse_flow_extensions(&exts)
            .unwrap()
            .unwrap();

        assert!(!parsed.is_table_scoped());
        assert_eq!(parsed.snapshot_table_ids, None);
    }

    #[test]
    fn test_parse_flow_extensions_snapshot_table_ids_empty_is_table_scoped() {
        let exts = HashMap::from([(FLOW_SNAPSHOT_TABLE_IDS.to_string(), "[]".to_string())]);

        let parsed = FlowQueryExtensions::parse_flow_extensions(&exts)
            .unwrap()
            .unwrap();

        assert!(parsed.is_table_scoped());
        assert_eq!(parsed.snapshot_table_ids, Some(BTreeSet::new()));
    }

    #[test]
    fn test_parse_flow_extensions_snapshot_table_ids_populated() {
        let exts = HashMap::from([(
            FLOW_SNAPSHOT_TABLE_IDS.to_string(),
            "[1024, 7, 7]".to_string(),
        )]);

        let parsed = FlowQueryExtensions::parse_flow_extensions(&exts)
            .unwrap()
            .unwrap();

        assert!(parsed.is_table_scoped());
        assert_eq!(parsed.snapshot_table_ids, Some(BTreeSet::from([7, 1024])));
        assert_eq!(parsed.classify_table(1024), FlowTableScanKind::Snapshot);
        assert_eq!(parsed.classify_table(7), FlowTableScanKind::Snapshot);
        assert_eq!(parsed.classify_table(8), FlowTableScanKind::Plain);
    }

    #[test]
    fn test_parse_flow_extensions_snapshot_table_ids_rejects_malformed() {
        for raw in [
            "not-json",
            "1024",
            r#"{"1024":true}"#,
            r#"["1024"]"#,
            "[1.5]",
            "[-1]",
            "[null]",
        ] {
            let exts = HashMap::from([(FLOW_SNAPSHOT_TABLE_IDS.to_string(), raw.to_string())]);

            let err = FlowQueryExtensions::parse_flow_extensions(&exts).unwrap_err();
            assert!(
                format!("{err}").contains(FLOW_SNAPSHOT_TABLE_IDS),
                "{raw}: {err}"
            );
        }
    }

    #[test]
    fn test_parse_flow_extensions_snapshot_table_ids_rejects_overflow() {
        let overflow = u64::from(TableId::MAX) + 1;
        let exts = HashMap::from([(FLOW_SNAPSHOT_TABLE_IDS.to_string(), format!("[{overflow}]"))]);

        let err = FlowQueryExtensions::parse_flow_extensions(&exts).unwrap_err();
        assert!(format!("{err}").contains("overflows the table id range"));
    }

    #[test]
    fn test_parse_flow_extensions_rejects_snapshot_incremental_overlap() {
        let incremental_region = RegionId::new(1024, 1);
        let exts = HashMap::from([
            (FLOW_SNAPSHOT_TABLE_IDS.to_string(), "[1024]".to_string()),
            (
                FLOW_INCREMENTAL_AFTER_SEQS.to_string(),
                format!(r#"{{"{}":10}}"#, incremental_region.as_u64()),
            ),
        ]);

        let err = FlowQueryExtensions::parse_flow_extensions(&exts).unwrap_err();
        let message = format!("{err}");
        assert!(message.contains(FLOW_SNAPSHOT_TABLE_IDS), "{message}");
        assert!(message.contains(FLOW_INCREMENTAL_AFTER_SEQS), "{message}");
        assert!(message.contains("both"), "{message}");
    }

    #[test]
    fn test_parse_flow_extensions_rejects_sink_and_snapshot_table_ids() {
        let exts = HashMap::from([
            (FLOW_SNAPSHOT_TABLE_IDS.to_string(), "[1024]".to_string()),
            (FLOW_SINK_TABLE_ID.to_string(), "2048".to_string()),
        ]);

        let err = FlowQueryExtensions::parse_flow_extensions(&exts).unwrap_err();
        let message = format!("{err}");
        assert!(message.contains(FLOW_SINK_TABLE_ID), "{message}");
        assert!(message.contains(FLOW_SNAPSHOT_TABLE_IDS), "{message}");
    }

    #[test]
    fn test_parse_flow_extensions_legacy_sink_key_still_supported() {
        let exts = HashMap::from([
            (FLOW_SINK_TABLE_ID.to_string(), "2048".to_string()),
            (FLOW_RETURN_REGION_SEQ.to_string(), "true".to_string()),
        ]);

        let parsed = FlowQueryExtensions::parse_flow_extensions(&exts)
            .unwrap()
            .unwrap();

        assert!(!parsed.is_table_scoped());
        assert_eq!(parsed.sink_table_id, Some(2048));
        assert_eq!(parsed.snapshot_table_ids, None);
        assert_eq!(parsed.classify_table(2048), FlowTableScanKind::Plain);
        assert_eq!(parsed.classify_table(1), FlowTableScanKind::Snapshot);
    }

    #[test]
    fn test_classify_table_table_scoped_uses_membership_only() {
        let snapshot_region = RegionId::new(2048, 1);
        let incremental_region = RegionId::new(1024, 3);
        let exts = HashMap::from([
            (FLOW_SNAPSHOT_TABLE_IDS.to_string(), "[2048]".to_string()),
            (
                FLOW_INCREMENTAL_MODE.to_string(),
                FLOW_INCREMENTAL_MODE_SEQUENCE_RANGE.to_string(),
            ),
            (
                FLOW_INCREMENTAL_AFTER_SEQS.to_string(),
                format!(r#"{{"{}":10}}"#, incremental_region.as_u64()),
            ),
        ]);

        let parsed = FlowQueryExtensions::parse_flow_extensions(&exts)
            .unwrap()
            .unwrap();

        assert!(parsed.is_table_scoped());
        assert_eq!(
            parsed.incremental_table_ids(),
            BTreeSet::from([1024]),
            "incremental table ids are derived from after-seq region keys"
        );
        assert_eq!(
            parsed.classify_table(snapshot_region.table_id()),
            FlowTableScanKind::Snapshot
        );
        assert_eq!(
            parsed.classify_table(incremental_region.table_id()),
            FlowTableScanKind::Incremental
        );
        assert_eq!(parsed.classify_table(9), FlowTableScanKind::Plain);
    }

    #[test]
    fn test_incremental_after_seq_for_region_fails_closed_when_missing() {
        let listed_region = RegionId::new(1024, 1);
        let exts = HashMap::from([
            (FLOW_SNAPSHOT_TABLE_IDS.to_string(), "[]".to_string()),
            (
                FLOW_INCREMENTAL_AFTER_SEQS.to_string(),
                format!(r#"{{"{}":10}}"#, listed_region.as_u64()),
            ),
        ]);

        let parsed = FlowQueryExtensions::parse_flow_extensions(&exts)
            .unwrap()
            .unwrap();

        assert_eq!(
            parsed
                .incremental_after_seq_for_region(listed_region)
                .unwrap(),
            10
        );
        let err = parsed
            .incremental_after_seq_for_region(RegionId::new(1024, 2))
            .unwrap_err();
        assert!(format!("{err}").contains("Missing region"));
    }

    #[test]
    fn test_should_collect_region_watermark_table_scoped() {
        let parsed = FlowQueryExtensions {
            snapshot_table_ids: Some(BTreeSet::new()),
            ..Default::default()
        };
        assert!(!parsed.should_collect_region_watermark());

        let parsed = FlowQueryExtensions {
            snapshot_table_ids: Some(BTreeSet::from([1024])),
            ..Default::default()
        };
        assert!(parsed.should_collect_region_watermark());

        let parsed = FlowQueryExtensions {
            snapshot_table_ids: Some(BTreeSet::new()),
            incremental_after_seqs: Some(HashMap::from([(1, 10)])),
            ..Default::default()
        };
        assert!(parsed.should_collect_region_watermark());
    }

    #[test]
    fn test_should_collect_region_watermark_for_region_from_extensions() {
        let snapshot_table = 1024;
        let incremental_region = RegionId::new(2048, 1);
        let exts = HashMap::from([
            (
                FLOW_SNAPSHOT_TABLE_IDS.to_string(),
                format!("[{snapshot_table}]"),
            ),
            (
                FLOW_INCREMENTAL_AFTER_SEQS.to_string(),
                format!(r#"{{"{}":10}}"#, incremental_region.as_u64()),
            ),
        ]);

        assert!(should_collect_region_watermark_for_region_from_extensions(
            &exts,
            RegionId::new(snapshot_table, 0)
        ));
        assert!(should_collect_region_watermark_for_region_from_extensions(
            &exts,
            incremental_region
        ));
        assert!(!should_collect_region_watermark_for_region_from_extensions(
            &exts,
            RegionId::new(4096, 0)
        ));

        // Legacy: the sink table never participates, source tables do.
        let legacy = HashMap::from([
            (FLOW_SINK_TABLE_ID.to_string(), "2048".to_string()),
            (FLOW_RETURN_REGION_SEQ.to_string(), "true".to_string()),
        ]);
        assert!(!should_collect_region_watermark_for_region_from_extensions(
            &legacy,
            RegionId::new(2048, 0)
        ));
        assert!(should_collect_region_watermark_for_region_from_extensions(
            &legacy,
            RegionId::new(1, 0)
        ));

        // Malformed extensions never fabricate a proof.
        let malformed =
            HashMap::from([(FLOW_SNAPSHOT_TABLE_IDS.to_string(), "not-json".to_string())]);
        assert!(!should_collect_region_watermark_for_region_from_extensions(
            &malformed,
            RegionId::new(snapshot_table, 0)
        ));
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
