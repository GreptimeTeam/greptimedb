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
/// domain together with the last sequence consumed inside it.
///
/// A sequence number belongs to the domain of the region that allocated it, not to the
/// region that currently stores the rows. A cursor is therefore keyed by the **creator**
/// region plus the expression slice, so that a reader holding adopted files can compare
/// a row's number against the cursor of the domain the number came from.
///
/// The slice is carried as the *identity of its partition expression* — the JSON form
/// `PartitionExpr::as_json_str()` writes — and never as a numeric projection of that
/// expression: a projection has to invent a comparison semantics for every operator it
/// cannot express, and silently widens a slice to the whole domain when it meets one.
/// `store-api` cannot depend on the `partition` crate, so the identity stays an opaque
/// string here; the layers that can (`mito2`, `query`) decode it with
/// `PartitionExpr::from_json_str` before they reason about it.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CursorShard {
    /// Identity (`PartitionExpr::as_json_str()`) of the partition expression that
    /// selects this slice's value domain, or `None` when the slice covers the creator
    /// region's complete value domain — which is exactly what a region-keyed cursor
    /// states.
    pub partition_expr: Option<String>,
    /// Last sequence consumed in the domain `partition_expr` selects.
    pub seq: SequenceNumber,
}

impl CursorShard {
    /// Creates a slice cursor over the value domain of `partition_expr`.
    pub fn new(partition_expr: Option<String>, seq: SequenceNumber) -> Self {
        Self {
            partition_expr,
            seq,
        }
    }

    /// Creates the slice covering a region's complete value domain, which is what a
    /// region-keyed cursor states.
    pub fn whole_domain(seq: SequenceNumber) -> Self {
        Self::new(None, seq)
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
    /// Explicit intent to read an exact row-level sequence delta `(min, max]`
    /// across memtables and all SST files (Flow's `sequence_range` incremental
    /// mode). The engine performs exact row-level filtering only when the region
    /// preserves per-row sequences and every participating SST file is trusted.
    ///
    /// When it cannot, it **fails closed**: the read returns the structured
    /// `SequenceRangeUnsupported` error (`Error::SequenceRangeUnsupported`, status
    /// `InvalidArguments`, retry hint `FALLBACK_MEMTABLE_ONLY_OR_FULL_RECOMPUTE`)
    /// instead of approximating the delta. The caller decides what to do with it —
    /// the read never silently widens, narrows or partially honours the range, so a
    /// caller that ignores the error cannot mistake an approximation for the exact
    /// delta it asked for.
    ///
    /// Historical `memtable_only` reads must never set this flag.
    pub exact_sequence_range: bool,
    /// Whether this scan reads the **physical** `__sequence` column of a foreign file
    /// and hides the rows whose number a substitution produced.
    ///
    /// A file this region adopted from another region is read with every row's
    /// sequence replaced by the handle's admission barrier by default, because the
    /// bytes carry the creator's numbering and no caller may observe another
    /// region's domain. Flow's incremental read is the one caller that needs the
    /// opposite: the barrier makes every adopted row look freshly published, so an
    /// adopted file cannot be split into rows already consumed and rows not yet
    /// consumed. With this flag the reader
    ///
    /// 1. keeps the physical column of **any** foreign file instead of
    ///    virtualizing it. `FileMeta::preserve_row_sequence` describes whether the
    ///    writer kept per-row numbers; a repartition only rewrites handle metadata
    ///    and never the bytes, so it clears that flag while the column still holds
    ///    the creator's numbers. Gating the physical read on the flag would
    ///    therefore exclude exactly the files this read exists for,
    /// 2. filters a foreign file's rows by the cursor of the domain that allocated
    ///    their numbers ([`Self::creator_cursor_shards`]), never by this region's own
    ///    `(C, H]`. When this scan knows no cursor slice of the file's creator the
    ///    read **fails closed** with `SequenceRangeUnsupported` instead of
    ///    approximating: comparing the file's numbers with a bound of another domain
    ///    silently drops live rows, and reading the file unfiltered delivers every
    ///    already consumed row again on every round,
    /// 3. hides the rows whose number a substitution produced, by the file's own
    ///    `FileMeta::substituted_up_to` cut (a statement about the bytes, so it
    ///    applies to local and foreign files alike), and reports the timestamp range
    ///    of those rows to the caller as `substituted_row_ranges`, so Flow recomputes
    ///    exactly those windows.
    ///
    /// A foreign file whose creator this scan has no cursor for cannot be attributed
    /// at all and fails closed as described above. A file whose bytes were never given
    /// per-row numbers (an option-off plateau) is out of this read's scope and is
    /// refused by the region option instead. Every other reader, including compaction,
    /// leaves this `false` and reads foreign files with the admission-barrier
    /// substitution.
    pub read_row_sequence: bool,
    /// Per-creator-region cursor slices a physical-sequence read filters with
    /// ([`Self::read_row_sequence`]), keyed by the region that allocated the numbers.
    ///
    /// A scan of one region may hold files it adopted from others, and a row's number
    /// then belongs to the creator's domain: the holder's `(C, H]` cannot bound it.
    /// The reader therefore looks the file's `FileMeta::region_id` up here and keeps the
    /// slices whose partition expression can hold a row of the file's own
    /// `FileMeta::partition_expr` (an expression-overlap check, not a numeric one). The
    /// least advanced of those sequences bounds candidate **file pruning**, and every row
    /// is then compared against the cursor of the slice its own partition expression
    /// selects: a file that straddles several slices delivers each row once, from the
    /// slice that row belongs to.
    ///
    /// Empty or absent means no cursor of the creator's domain is known. An adopted
    /// file then has no provable row-level bound at all, and the read **fails
    /// closed** with `SequenceRangeUnsupported` instead of falling back to this
    /// region's own `(C, H]`: comparing the file's numbers with a bound of another
    /// domain silently drops live rows, while reading the file unfiltered delivers
    /// every already consumed row again on every round. Every reader but a
    /// `read_row_sequence` incremental delta leaves it `None`.
    pub creator_cursor_shards: Option<Arc<BTreeMap<u64, Vec<CursorShard>>>>,
    /// Whether this read may cross an invalid boundary: rows whose sequence the
    /// region can no longer vouch for are still returned instead of the read being
    /// refused for asking about them. Such a read only needs the timestamps of the
    /// rows a stale cursor may have missed, and never accumulates an incremental
    /// delta.
    ///
    /// Concretely it (1) allows the boundary to be crossed at all — refusing a cursor
    /// for being older than the region's files would deadlock recovery, and this read
    /// is what rediscovers the windows a cursor can no longer explain — and applies no
    /// lower sequence bound to memtables either (`memtable_min_sequence` is the caller's
    /// cursor, and rows at or below it are exactly the ones whose windows are being
    /// rediscovered) —, (2) prunes at
    /// file granularity only: a file whose admission barrier is at or below the cursor
    /// was fully consumed and is dropped, while a file that states no barrier cannot be
    /// proven consumed, so the read **fails closed** with `SequenceRangeUnsupported`
    /// instead of reading it — keeping it would silently turn discovery into a
    /// whole-region scan —, and (3) does not require row-level sequence exactness, so
    /// files admitted by an edit are readable even though their row sequences cannot be
    /// trusted. The result is a conservative superset of windows, which is safe: those
    /// windows are simply recomputed.
    ///
    /// Ordinary incremental deltas must leave it `false`.
    pub read_across_invalid_boundary: bool,
    /// Whether this read requires its snapshot to cover the region's flushed frontier:
    /// it reads whole windows completely under a frozen high watermark, and its result
    /// may only be committed when that watermark still covers everything flushed.
    ///
    /// It differs from [`Self::read_across_invalid_boundary`] in two ways: it must
    /// **not** prune at file granularity and must **not** force row-sequence override,
    /// otherwise the rows of a partially consumed file would be dropped and the
    /// recomputed window would be wrong. What it shares with that read is crossing the
    /// invalid boundary (an old cursor is allowed, because this read is what repairs
    /// that state) and the requirement to prove the frozen `H` still covers the
    /// region's flushed frontier before its result may be committed.
    pub require_flushed_frontier: bool,
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
