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
use std::fmt::{Display, Formatter};
use std::sync::Arc;

use common_recordbatch::OrderOption;
use datafusion_expr::expr::Expr;
use datatypes::types::json_type::JsonNativeType;
use itertools::Itertools;
use strum::Display;

use crate::storage::SequenceNumber;

/// A hint on how to select rows from a time-series.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Display)]
pub enum TimeSeriesRowSelector {
    /// Only keep the last row of each time-series.
    #[strum(to_string = "LastRow {{ after_merge: {after_merge} }}")]
    LastRow {
        /// Whether selection runs after cross-source merge and deduplication.
        after_merge: bool,
    },
}

/// A hint on how to distribute time-series data on the scan output.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Display)]
pub enum TimeSeriesDistribution {
    /// Data are distributed by time window first. The scanner will
    /// return all data within one time window before moving to the next one.
    TimeWindowed,
    /// Data are organized by time-series first. The scanner will return
    /// all data for one time-series before moving to the next one.
    PerSeries,
}

/// One slice of an incremental cursor: the partition expression selecting a region's value
/// domain together with the last sequence consumed inside it, and — when the producer can
/// state it — the last sequence that delta covers.
///
/// The pair bounds the rows one round owes on the slice: `seq < number <= until`. The upper
/// end matters to a read of a file **another** region created, because such a read compares a
/// row's number against the creator's domain: without the bound, every row the creator
/// numbered above the delta is delivered as if it were part of it.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CursorShard {
    /// Identity (`PartitionExpr::as_json_str()`) of the expression selecting this slice's
    /// value domain, or `None` for the creator region's complete value domain.
    pub partition_expr: Option<String>,
    /// Last sequence consumed in the domain `partition_expr` selects (exclusive lower bound).
    pub seq: SequenceNumber,
    /// Last sequence the delta of this slice covers (inclusive upper bound), when the
    /// producer states one.
    ///
    /// `None` — also what a payload that states no bound means — leaves the slice unbounded
    /// above: a row above `seq` is live until proven otherwise. `Some(until)` says a row
    /// numbered above `until` belongs to a later delta and must not be delivered by a read of
    /// this one.
    pub until: Option<SequenceNumber>,
}

impl CursorShard {
    /// Creates a slice whose delta is unbounded above.
    pub fn new(partition_expr: Option<String>, seq: SequenceNumber) -> Self {
        Self {
            partition_expr,
            seq,
            until: None,
        }
    }

    /// Creates the slice covering a region's complete value domain, which is what a
    /// region-keyed cursor states.
    pub fn whole_domain(seq: SequenceNumber) -> Self {
        Self::new(None, seq)
    }

    /// Returns this slice with `until` as its inclusive upper bound
    /// ([`Self::until`]).
    pub fn with_until(mut self, until: SequenceNumber) -> Self {
        self.until = Some(until);
        self
    }
}

#[derive(Default, Clone, Debug, PartialEq)]
pub struct ScanRequest {
    /// Optional projection information for the scan. `None` reads all root
    /// columns.
    pub projection: Option<Vec<usize>>,
    /// Filters pushed down
    pub filters: Vec<Expr>,
    /// Expected output ordering. This is only a hint and isn't guaranteed.
    pub output_ordering: Option<Vec<OrderOption>>,
    /// limit can be used to reduce the amount scanned
    /// from the datasource as a performance optimization.
    /// If set, it contains the amount of rows needed by the caller,
    /// The data source should return *at least* this number of rows if available.
    pub limit: Option<usize>,
    /// Optional hint to select rows from time-series.
    pub series_row_selector: Option<TimeSeriesRowSelector>,
    /// Optional constraint on the sequence number of the rows to read.
    /// If set, only rows with a sequence number **lesser or equal** to this value
    /// will be returned.
    /// This is the effective memtable upper bound used by the scan, whether provided
    /// explicitly or bound on scan open.
    pub memtable_max_sequence: Option<SequenceNumber>,
    /// Optional constraint on the minimal sequence number in the memtable.
    /// If set, only the memtables that contain sequences **greater than** this value will be scanned
    pub memtable_min_sequence: Option<SequenceNumber>,
    /// Optional constraint on the minimal sequence number in the SST files.
    /// If set, only the SST files that contain sequences greater than this value will be scanned.
    pub sst_min_sequence: Option<SequenceNumber>,
    /// Whether to skip all SST files.
    /// This is stronger than `sst_min_sequence` and also skips SST files without sequence metadata.
    pub skip_sst_files: bool,
    /// Whether to bind the effective snapshot upper bound when opening the scan.
    pub snapshot_on_scan: bool,
    /// Explicit intent to read an exact row-level sequence delta `(min, max]` across memtables
    /// and all SST files (Flow's `sequence_range` incremental mode). Exact row-level filtering
    /// happens only when the region preserves per-row sequences and every participating SST
    /// file is trusted; otherwise the read **fails closed** with `SequenceRangeUnsupported`
    /// (status `InvalidArguments`, retry hint `FALLBACK_MEMTABLE_ONLY_OR_FULL_RECOMPUTE`).
    ///
    /// Historical `memtable_only` reads must never set this flag.
    pub exact_sequence_range: bool,
    /// Whether this scan reads the **physical** `__sequence` column of a foreign file and hides
    /// the rows whose number a substitution produced.
    ///
    /// The reader then keeps the column of any foreign file, filters its rows by
    /// [`Self::creator_cursor_shards`] (**fails closed** with `SequenceRangeUnsupported` when
    /// the creator's cursor is unknown), and hides the rows at or below the file's own
    /// `FileMeta::substituted_up_to` cut, reporting their timestamp ranges as
    /// `substituted_row_ranges`. Every other reader, compaction included, leaves this `false`.
    pub read_row_sequence: bool,
    /// Per-creator-region cursor slices a physical-sequence read filters with
    /// ([`Self::read_row_sequence`]), keyed by the region that allocated the numbers. The least
    /// advanced slice is the file-level lower bound, the least upper bound any slice states is
    /// its file-level upper bound ([`CursorShard::until`]); each row is compared against the
    /// slice that selects it.
    ///
    /// Empty or absent means no cursor of the creator's domain is known, and an adopted file
    /// then **fails closed** with `SequenceRangeUnsupported`.
    pub creator_cursor_shards: Option<Arc<BTreeMap<u64, Vec<CursorShard>>>>,
    /// Whether this read may cross a boundary a stale cursor can no longer explain (the flag's
    /// name says "invalid boundary"): rows whose sequence the region can no longer vouch for
    /// are returned instead of the read being refused.
    ///
    /// It applies no lower sequence bound to memtables, prunes at file granularity only
    /// (**fails closed** with `SequenceRangeUnsupported` for a file stating no admission
    /// barrier), requires no row-level exactness, and returns a superset of the windows a stale
    /// cursor can no longer explain. Ordinary incremental deltas must leave it `false`.
    pub read_across_invalid_boundary: bool,
    /// Optional hint for the distribution of time-series data.
    pub distribution: Option<TimeSeriesDistribution>,
    /// Optional hint from query-driven JSON type concretization.
    pub json_type_hint: HashMap<String, JsonNativeType>,
    /// Whether Mito should keep string primary-key columns dictionary encoded in its output.
    pub preserve_pk_dictionary_encoding: bool,
}

impl Display for ScanRequest {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        enum Delimiter {
            None,
            Init,
        }

        impl Delimiter {
            fn as_str(&mut self) -> &str {
                match self {
                    Delimiter::None => {
                        *self = Delimiter::Init;
                        ""
                    }
                    Delimiter::Init => ", ",
                }
            }
        }

        let mut delimiter = Delimiter::None;

        write!(f, "ScanRequest {{ ")?;
        if let Some(projection) = &self.projection {
            write!(f, "{}projection: {:?}", delimiter.as_str(), projection)?;
        }
        if !self.filters.is_empty() {
            write!(
                f,
                "{}filters: [{}]",
                delimiter.as_str(),
                self.filters
                    .iter()
                    .map(|f| f.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            )?;
        }
        if let Some(output_ordering) = &self.output_ordering {
            write!(
                f,
                "{}output_ordering: {:?}",
                delimiter.as_str(),
                output_ordering
            )?;
        }
        if let Some(limit) = &self.limit {
            write!(f, "{}limit: {}", delimiter.as_str(), limit)?;
        }
        if let Some(series_row_selector) = &self.series_row_selector {
            write!(
                f,
                "{}series_row_selector: {}",
                delimiter.as_str(),
                series_row_selector
            )?;
        }
        if let Some(sequence) = &self.memtable_max_sequence {
            write!(f, "{}sequence: {}", delimiter.as_str(), sequence)?;
        }
        if let Some(sst_min_sequence) = &self.sst_min_sequence {
            write!(
                f,
                "{}sst_min_sequence: {}",
                delimiter.as_str(),
                sst_min_sequence
            )?;
        }
        if self.skip_sst_files {
            write!(
                f,
                "{}skip_sst_files: {}",
                delimiter.as_str(),
                self.skip_sst_files
            )?;
        }
        if self.snapshot_on_scan {
            write!(
                f,
                "{}snapshot_on_scan: {}",
                delimiter.as_str(),
                self.snapshot_on_scan
            )?;
        }
        if self.exact_sequence_range {
            write!(f, "{}exact_sequence_range: true", delimiter.as_str())?;
        }
        if self.read_row_sequence {
            write!(f, "{}read_row_sequence: true", delimiter.as_str())?;
        }
        if self.preserve_pk_dictionary_encoding {
            write!(
                f,
                "{}preserve_pk_dictionary_encoding: true",
                delimiter.as_str()
            )?;
        }
        if let Some(distribution) = &self.distribution {
            write!(f, "{}distribution: {}", delimiter.as_str(), distribution)?;
        }
        if !self.json_type_hint.is_empty() {
            write!(
                f,
                "{}json_type_hint: {}",
                delimiter.as_str(),
                self.json_type_hint
                    .iter()
                    .map(|(column, json_type)| format!("({column}: {json_type})"))
                    .join(", ")
            )?;
        }
        write!(f, " }}")
    }
}

#[cfg(test)]
mod tests {
    use datafusion_expr::{Operator, binary_expr, col, lit};

    use super::*;

    #[test]
    fn test_display_scan_request() {
        let request = ScanRequest {
            ..Default::default()
        };
        assert_eq!(request.to_string(), "ScanRequest {  }");

        let projection = Some(vec![1, 2]);
        let request = ScanRequest {
            projection,
            filters: vec![
                binary_expr(col("i"), Operator::Gt, lit(1)),
                binary_expr(col("s"), Operator::Eq, lit("x")),
            ],
            limit: Some(10),
            ..Default::default()
        };
        assert_eq!(
            request.to_string(),
            r#"ScanRequest { projection: [1, 2], filters: [i > Int32(1), s = Utf8("x")], limit: 10 }"#
        );

        let request = ScanRequest {
            filters: vec![
                binary_expr(col("i"), Operator::Gt, lit(1)),
                binary_expr(col("s"), Operator::Eq, lit("x")),
            ],
            limit: Some(10),
            ..Default::default()
        };
        assert_eq!(
            request.to_string(),
            r#"ScanRequest { filters: [i > Int32(1), s = Utf8("x")], limit: 10 }"#
        );

        let projection = Some(vec![1, 2]);
        let request = ScanRequest {
            projection,
            limit: Some(10),
            ..Default::default()
        };
        assert_eq!(
            request.to_string(),
            "ScanRequest { projection: [1, 2], limit: 10 }"
        );

        let request = ScanRequest {
            series_row_selector: Some(TimeSeriesRowSelector::LastRow { after_merge: true }),
            snapshot_on_scan: true,
            exact_sequence_range: true,
            ..Default::default()
        };
        assert_eq!(
            request.to_string(),
            "ScanRequest { series_row_selector: LastRow { after_merge: true }, snapshot_on_scan: true, exact_sequence_range: true }"
        );

        assert_eq!(
            TimeSeriesRowSelector::LastRow { after_merge: false }.to_string(),
            "LastRow { after_merge: false }"
        );

        let request = ScanRequest {
            skip_sst_files: true,
            ..Default::default()
        };
        assert_eq!(request.to_string(), "ScanRequest { skip_sst_files: true }");
    }
}
