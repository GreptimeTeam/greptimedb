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

//! One slice of an evaluated series, as evaluators hand it to their consumers.

use datafusion::error::{DataFusionError, Result};
use datatypes::series::samples::Series;
use datatypes::series::time::GridRange;

/// One slice of one evaluated series, addressed on absolute grid positions.
///
/// A chunk carries only positions where some field holds a value (another field may be null
/// there), and its rows need not be adjacent; read them
/// through [`TimeAxis::positions`](datatypes::series::time::TimeAxis::positions). A position no
/// chunk carries is either empty or not decided yet, so only `is_last` says the series is
/// complete. A series closes exactly once, and a closing chunk is emitted even when it holds no
/// values.
#[derive(Clone, Debug)]
pub struct EvaluatedChunk {
    pub series: Series,
    pub is_last: bool,
}

impl EvaluatedChunk {
    /// Errors when `series` is not addressed on a grid.
    pub fn try_new(series: Series, is_last: bool) -> Result<Self> {
        if series.time.grid().is_none() {
            return Err(DataFusionError::Execution(
                "an evaluated chunk must be addressed on a grid".to_string(),
            ));
        }
        Ok(Self { series, is_last })
    }

    /// The grid range of this chunk's rows, or `None` when they form several runs. Ranges of one
    /// series never go backwards or overlap; an empty range is a close without rows.
    pub fn range(&self) -> Option<&GridRange> {
        self.series.time.as_grid_range()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datatypes::arrow::array::Float64Array;
    use datatypes::series::SeriesLabels;
    use datatypes::series::samples::{FieldSpec, Samples, Series, SeriesFields, SeriesSchema};
    use datatypes::series::time::{EvalGrid, GridRange, GridSpec, TimeAxis};

    use super::EvaluatedChunk;

    fn grid() -> Arc<EvalGrid> {
        Arc::new(EvalGrid::try_new(GridSpec::try_new(0, 60_000, 4).unwrap()).unwrap())
    }

    fn schema() -> SeriesSchema {
        SeriesSchema {
            fields: Arc::from([FieldSpec { name: "v0".into() }]),
            eval_grid: Some(grid()),
        }
    }

    fn metric() -> SeriesLabels {
        SeriesLabels::try_new(Some("requests".into()), Vec::new()).unwrap()
    }

    fn chunk(
        schema: &SeriesSchema,
        start: usize,
        values: Vec<f64>,
        is_last: bool,
    ) -> EvaluatedChunk {
        let samples = Samples::from_float(Float64Array::from(values));
        let range =
            GridRange::try_new(schema.eval_grid.clone().unwrap(), start, samples.len()).unwrap();
        let series = Series {
            metric: metric(),
            time: TimeAxis::Grid(range),
            fields: SeriesFields::try_from_vec(vec![samples]).unwrap(),
        };
        EvaluatedChunk::try_new(series, is_last).unwrap()
    }

    #[test]
    fn a_chunk_can_cover_no_positions() {
        let schema = schema();
        let empty = Samples::from_float(Float64Array::from(Vec::<f64>::new()));
        let range = GridRange::try_new(schema.eval_grid.clone().unwrap(), 4, 0).unwrap();
        assert!(range.is_empty());
        let series = Series {
            metric: metric(),
            time: TimeAxis::Grid(range),
            fields: SeriesFields::try_from_vec(vec![empty]).unwrap(),
        };
        let chunk = EvaluatedChunk::try_new(series, true).unwrap();
        assert!(chunk.range().expect("one run").is_empty());
        assert!(chunk.is_last);
    }

    #[test]
    fn a_range_is_absolute_on_its_grid() {
        let schema = schema();
        let first = chunk(&schema, 0, vec![1.0, 2.0], false);
        let second = chunk(&schema, 2, vec![3.0, 4.0], true);
        assert!(!first.is_last);
        assert!(second.is_last);
        let first = first.range().expect("one run");
        let second = second.range().expect("one run");
        assert_eq!((first.start(), first.len()), (0, 2));
        assert_eq!((second.start(), second.len()), (2, 2));
    }

    #[test]
    fn a_chunk_must_be_addressed_on_a_grid() {
        let samples = Samples::from_float(Float64Array::from(vec![1.0]));
        let series = Series {
            metric: metric(),
            time: TimeAxis::Explicit(datatypes::arrow::buffer::ScalarBuffer::from(vec![0i64])),
            fields: SeriesFields::try_from_vec(vec![samples]).unwrap(),
        };
        assert!(EvaluatedChunk::try_new(series, true).is_err());
    }
}
