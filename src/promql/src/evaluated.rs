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

/// One slice of one evaluated series: the rows holding a value that its evaluator handed over at
/// once, which may form several runs of consecutive positions.
///
/// The series' [`TimeAxis`](datatypes::series::time::TimeAxis) covers the positions this chunk
/// holds, and those positions are absolute on the grid: a consumer that concatenates the chunks of
/// a series never reorders them, and never has to translate a chunk-local row number into a grid
/// position. The rows of one chunk need not be adjacent -- a sparse chunk says which positions it
/// holds through [`TimeAxis::positions`](datatypes::series::time::TimeAxis::positions) -- so a
/// consumer reads them with that traversal instead of indexing from the first position.
///
/// A chunk carries only positions that hold a value. A position no chunk carries either holds no
/// value or has not been decided yet, and a consumer cannot tell the two apart, so it must not
/// infer progress from the last row it saw: only `is_last` says the series is complete. The gaps
/// this leaves between the chunks of a series are what keeps a sparse series from costing a row
/// per empty position. A single field may still hold a null at a position another field fills,
/// since the fields of one chunk are row-aligned.
///
/// A series closes exactly once, on the chunk that carries `is_last`. A closed prefix is followed
/// by a closing chunk when nothing is left, even when that chunk holds no values, so a consumer
/// can tell "nothing more here" from "no more input".
#[derive(Clone, Debug)]
pub struct EvaluatedChunk {
    /// The evaluated values, addressed on their absolute grid positions.
    pub series: Series,
    /// Whether this is the final chunk of this series.
    pub is_last: bool,
}

impl EvaluatedChunk {
    /// Wraps `series`, checking that it is addressed on a grid.
    pub fn try_new(series: Series, is_last: bool) -> Result<Self> {
        // Both grid axes are addressed on the grid; the explicit axes are not.
        if series.time.grid().is_none() {
            return Err(DataFusionError::Execution(
                "an evaluated chunk must be addressed on a grid".to_string(),
            ));
        }
        Ok(Self { series, is_last })
    }

    /// The absolute grid range this chunk holds rows for, when its rows form one run.
    ///
    /// Ranges of one series never go backwards and never overlap, and they need not be adjacent:
    /// the positions between two chunks either hold no value or have not been decided yet. An
    /// empty range is meaningful: it reports that the chunk closed the series without holding a
    /// row of its own.
    ///
    /// `None` means the chunk holds several runs: it is addressed on a grid, but its rows are not
    /// one range, and reading the positions between them would read holes. Walk the axis instead
    /// ([`TimeAxis::positions`](datatypes::series::time::TimeAxis::positions)).
    pub fn range(&self) -> Option<&GridRange> {
        // `try_new` rejects everything but a grid axis, and `series` is public, so a chunk built
        // by struct literal is answered from the axis rather than trusted.
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

    /// `values` are the floats this chunk finished evaluating; `start` is their absolute offset.
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

    /// A chunk without values is still a chunk: it is how a closed prefix is reported, and a
    /// consumer needs it to tell "nothing more here" from "no more input".
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

    /// The range is absolute, so concatenating two chunks of one series never reorders them.
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

    /// A chunk that is not addressed on a grid cannot be indexed by a consumer.
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
