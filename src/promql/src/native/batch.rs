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

//! The batches native nodes hand to each other.
//!
//! These are ordinary Rust values passed by call, not DataFusion streams and not encoded messages:
//! the Arrow arrays, buffers and shared schemas inside them are reused as they are.
//!
//! Two rules make a chunk self-describing:
//!
//! - A grid position is absolute on the chunk's grid. Batch-local row numbers never stand in for
//!   one, and an inner grid is never addressed with an outer grid's index.
//! - A series closes exactly once, on the chunk that carries [`EvaluatedChunk::is_last`]. A closed
//!   prefix still has to be followed by a final chunk even when that chunk holds no values, so a
//!   consumer can tell "nothing more here" from "no more input".

//!
//! The chunk type itself is [`crate::evaluated::EvaluatedChunk`]: one chunk protocol serves both the
//! evaluator that produces chunks and the native nodes that consume them. Defining a second one
//! here would let the two drift apart, and a chunk produced by the evaluator could not be handed to
//! a node without translation.

pub use crate::evaluated::EvaluatedChunk;

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datatypes::arrow::array::Float64Array;
    use datatypes::series::MetricName;
    use datatypes::series::samples::{Samples, Series, SeriesFields, SeriesSchema};
    use datatypes::series::time::{EvalGrid, GridRange, GridSpec, TimeAxis};

    use super::EvaluatedChunk;

    fn grid() -> Arc<EvalGrid> {
        Arc::new(EvalGrid::try_new(GridSpec::try_new(0, 60_000, 4).unwrap()).unwrap())
    }

    fn schema() -> SeriesSchema {
        SeriesSchema {
            fields: Arc::from([datatypes::series::samples::FieldSpec { name: "v0".into() }]),
            eval_grid: Some(grid()),
        }
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
            metric: MetricName::try_new(Some("requests".into()), Vec::new()).unwrap(),
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
            metric: MetricName::try_new(Some("requests".into()), Vec::new()).unwrap(),
            time: TimeAxis::Grid(range),
            fields: SeriesFields::try_from_vec(vec![empty]).unwrap(),
        };
        let chunk = EvaluatedChunk::try_new(series, true).unwrap();
        assert!(chunk.range().is_empty());
        assert!(chunk.is_last);
    }

    /// The range is absolute, so concatenating two chunks of one series never reorders them.
    #[test]
    fn a_range_is_absolute_on_its_grid() {
        let schema = schema();
        let first = chunk(&schema, 0, vec![1.0, 2.0], false);
        let second = chunk(&schema, 2, vec![3.0, 4.0], true);
        assert_eq!((first.range().start(), first.range().len()), (0, 2));
        assert_eq!((second.range().start(), second.range().len()), (2, 2));
        assert!(!first.is_last);
        assert!(second.is_last);
    }

    /// A chunk that is not addressed on a grid cannot be indexed by a native node.
    #[test]
    fn a_chunk_must_be_addressed_on_a_grid() {
        let samples = Samples::from_float(Float64Array::from(vec![1.0]));
        let series = Series {
            metric: MetricName::try_new(Some("requests".into()), Vec::new()).unwrap(),
            time: TimeAxis::Explicit(datatypes::arrow::buffer::ScalarBuffer::from(vec![0i64])),
            fields: SeriesFields::try_from_vec(vec![samples]).unwrap(),
        };
        assert!(EvaluatedChunk::try_new(series, true).is_err());
    }

    /// A schema with a grid is what marks a sample type as evaluated; the same fields without a
    /// grid are raw samples.
    #[test]
    fn a_grid_in_the_schema_marks_an_evaluated_type() {
        let evaluated = schema();
        assert!(evaluated.eval_grid.is_some());
        let raw = SeriesSchema {
            fields: evaluated.fields.clone(),
            eval_grid: None,
        };
        assert!(raw.eval_grid.is_none());
    }
}
