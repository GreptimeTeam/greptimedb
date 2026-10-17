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

//! Brute-force reference for operators producing grid series: results are compared as dense
//! `[grid index][field]` matrices, so a kernel cannot pass by aligning local row numbers.

use datatypes::series::MetricName;
use datatypes::series::samples::{SampleRef, Series};
use datatypes::series::time::TimeAxis;
use rand::Rng;

/// Values by absolute grid index, then by field; `None` is missing.
pub(crate) type Dense = Vec<Vec<Option<f64>>>;

/// Expands a grid series to a dense matrix over its whole grid.
fn expand(series: &Series) -> Dense {
    let TimeAxis::Grid(range) = &series.time else {
        panic!("expected a grid series, got {:?}", series.time);
    };
    let fields = series.fields.as_slice();
    let mut dense = vec![vec![None; fields.len()]; range.grid().timestamps().len()];
    for (field_index, field) in fields.iter().enumerate() {
        for (row, sample) in field.iter().enumerate() {
            dense[range.start() + row][field_index] = match sample {
                SampleRef::Missing => None,
                SampleRef::Float(value) => Some(value),
                SampleRef::Histogram(_) => panic!("unexpected histogram sample"),
            };
        }
    }
    dense
}

/// Asserts `actual` is the trimmed form of `expected`, with identical value bits and metric.
pub(crate) fn assert_series_eq(actual: Option<&Series>, expected: &Dense, metric: &MetricName) {
    let valid_rows = || {
        expected
            .iter()
            .enumerate()
            .filter(|(_, row)| row.iter().any(Option::is_some))
            .map(|(index, _)| index)
    };
    let Some((first, last)) = valid_rows().next().zip(valid_rows().next_back()) else {
        assert!(actual.is_none(), "expected no series, got {actual:?}");
        return;
    };
    let actual = actual.unwrap_or_else(|| panic!("expected a series over [{first}, {last}]"));
    let TimeAxis::Grid(range) = &actual.time else {
        panic!("expected a grid series, got {:?}", actual.time);
    };
    assert_eq!(
        (range.start(), range.len()),
        (first, last + 1 - first),
        "grid range is not trimmed to the valid rows"
    );
    assert_eq!(range.grid().timestamps().len(), expected.len());
    assert_eq!(&actual.metric, metric);
    assert_eq!(bits(&expand(actual)), bits(expected));
}

fn bits(dense: &Dense) -> Vec<Vec<Option<u64>>> {
    dense
        .iter()
        .map(|row| row.iter().map(|value| value.map(f64::to_bits)).collect())
        .collect()
}

/// Values covering the bit patterns kernels most often mishandle.
pub(crate) fn special_value(rng: &mut impl Rng) -> f64 {
    const SPECIAL: [f64; 7] = [
        0.0,
        -0.0,
        f64::NAN,
        f64::INFINITY,
        f64::NEG_INFINITY,
        f64::MAX,
        f64::MIN_POSITIVE,
    ];
    if rng.random_bool(0.2) {
        SPECIAL[rng.random_range(0..SPECIAL.len())]
    } else {
        rng.random_range(-1e6..1e6)
    }
}

/// A dense matrix with a random start, length and holes, all other rows missing.
pub(crate) fn random_dense(rng: &mut impl Rng, grid_len: usize, fields: usize) -> Dense {
    let mut dense = vec![vec![None; fields]; grid_len];
    let start = rng.random_range(0..grid_len);
    let end = rng.random_range(start..=grid_len);
    let missing = rng.random_range(0.0..0.8);
    for row in &mut dense[start..end] {
        for value in row.iter_mut() {
            if !rng.random_bool(missing) {
                *value = Some(special_value(rng));
            }
        }
    }
    dense
}
