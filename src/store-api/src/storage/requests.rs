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

use std::collections::HashMap;
use std::fmt::{Display, Formatter};

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

/// A hint to keep only a deterministic fraction of the series of a scan.
///
/// The `tsid` of the metric engine is a hash of the series tags, so keeping
/// `tsid < tsid_max` with `tsid_max = num/den * 2^64` keeps each series
/// deterministically and independently of the time range, the memtable, or the
/// SST layout. Series-index entries are sorted by `(__table_id, __tsid)`, so the
/// bound also prunes whole index blocks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SeriesSample {
    /// Numerator of the kept fraction.
    pub num: u32,
    /// Denominator of the kept fraction. Callers reject `den == 0` and
    /// `num >= den` before sampling.
    pub den: u32,
}

impl SeriesSample {
    /// Returns the exclusive `tsid` upper bound of the kept series.
    ///
    /// The division is done in `u128` so the bound is exact for every
    /// `num < den`; an equal or larger numerator saturates at [`u64::MAX`].
    pub fn tsid_max(&self) -> u64 {
        let den = u128::from(self.den.max(1));
        let num = u128::from(self.num);
        u64::try_from((num << 64) / den).unwrap_or(u64::MAX)
    }
}

impl Display for SeriesSample {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.num, self.den)
    }
}

/// A hint to keep only a deterministic fraction of the rows of a scan that stores
/// its rows in encoded-primary-key order (a classic, non-metric table).
///
/// The engine picks one contiguous interval `[lo, hi]` of the encoded primary key
/// covering about `num/den` of the rows of the files the scan would read, and
/// applies it as an ordinary range predicate on the encoded key. Both bounds are
/// actual keys taken from file statistics, so the upper bound is inclusive. Encoded primary
/// keys are ordered by the storage order of a classic table, so the bound prunes
/// whole files by their manifest statistics and whole row groups by their encoded
/// key statistics instead of only filtering rows after they are read. Unlike a
/// hash of the key, a range keeps that storage-order locality.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PkSample {
    /// Numerator of the kept fraction.
    pub num: u32,
    /// Denominator of the kept fraction. Callers reject `den == 0` and
    /// `num >= den`, and `num == 0`, before sampling.
    pub den: u32,
}

impl Display for PkSample {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.num, self.den)
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
    /// preserves per-row sequences and every participating SST file is trusted;
    /// otherwise it returns a structured stale/unsupported error so the caller
    /// falls back instead of silently approximating.
    ///
    /// Historical `memtable_only` reads must never set this flag.
    pub exact_sequence_range: bool,
    /// Optional hint for the distribution of time-series data.
    pub distribution: Option<TimeSeriesDistribution>,
    /// Optional hint to keep only a deterministic fraction of the series.
    pub sample: Option<SeriesSample>,
    /// Optional hint to keep only the rows of one contiguous interval of the
    /// encoded primary key, chosen to cover about `num/den` of the rows.
    pub pk_sample: Option<PkSample>,
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
        if let Some(sample) = &self.sample {
            write!(f, "{}sample: {}", delimiter.as_str(), sample)?;
        }
        if let Some(pk_sample) = &self.pk_sample {
            write!(f, "{}pk_sample: {}", delimiter.as_str(), pk_sample)?;
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

        let request = ScanRequest {
            distribution: Some(TimeSeriesDistribution::PerSeries),
            sample: Some(SeriesSample { num: 1, den: 4 }),
            ..Default::default()
        };
        assert_eq!(
            request.to_string(),
            "ScanRequest { distribution: PerSeries, sample: 1/4 }"
        );
        // `num < den` is exact: a quarter of the `tsid` space is `2^62`.
        assert_eq!(1u64 << 62, SeriesSample { num: 1, den: 4 }.tsid_max());
        assert_eq!(
            ((3u128 << 64) / 5) as u64,
            SeriesSample { num: 3, den: 5 }.tsid_max()
        );
        // A full or degenerate fraction saturates instead of overflowing.
        assert_eq!(u64::MAX, SeriesSample { num: 4, den: 4 }.tsid_max());
        assert_eq!(u64::MAX, SeriesSample { num: 1, den: 0 }.tsid_max());

        let request = ScanRequest {
            pk_sample: Some(PkSample { num: 1, den: 4 }),
            ..Default::default()
        };
        assert_eq!(request.to_string(), "ScanRequest { pk_sample: 1/4 }");
        assert_eq!("1/4", PkSample { num: 1, den: 4 }.to_string());
    }
}
