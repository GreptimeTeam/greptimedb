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

use std::sync::Arc;

use arrow::array::Float64Array;
use arrow::buffer::{Buffer, ScalarBuffer};

use super::*;
use crate::series::samples::{FieldSpec, SampleRef, Samples, SeriesFields, SeriesSchema};
use crate::series::time::{EvalGrid, GridSpec};
use crate::series::{Label, SeriesLabels};

fn schema(names: &[&str]) -> SeriesSchema {
    SeriesSchema {
        fields: names
            .iter()
            .map(|name| FieldSpec {
                name: Arc::from(*name),
            })
            .collect::<Vec<_>>()
            .into(),
        eval_grid: None,
    }
}

fn metric() -> SeriesLabels {
    SeriesLabels::try_new(
        Some(Arc::from("cpu")),
        vec![Label {
            name: Arc::from("host"),
            value: Arc::from("node-a"),
        }],
    )
    .unwrap()
}

fn times(values: Vec<i64>) -> RawTimestamps {
    RawTimestamps::try_new(values.into()).unwrap()
}

fn float_fields(first: Vec<Option<f64>>, second: Option<Vec<Option<f64>>>) -> SeriesFields {
    let mut fields = vec![Samples::from_float(Float64Array::from(first))];
    if let Some(second) = second {
        fields.push(Samples::from_float(Float64Array::from(second)));
    }
    SeriesFields::try_from_vec(fields).unwrap()
}

#[test]
fn constructors_reject_invalid_chunk_boundaries_and_unordered_timestamps() {
    let schema = schema(&["value"]);
    let key = Buffer::from(vec![1_u8]);
    let fields = float_fields(vec![Some(1.0), Some(2.0)], None);

    assert!(RawTimestamps::try_new(vec![1_i64, 1].into()).is_err());
    assert!(RawTimestamps::try_new(vec![2_i64, 1].into()).is_err());
    assert!(
        RawTimestamps::try_new(ScalarBuffer::from(Vec::<i64>::new()))
            .unwrap()
            .is_empty()
    );

    assert!(
        RawSeriesChunk::try_new(
            &schema,
            key.clone(),
            None,
            times(vec![1, 2]),
            float_fields(vec![Some(1.0), Some(2.0)], Some(vec![Some(3.0), Some(4.0)])),
            true,
        )
        .is_err()
    );
    assert!(
        RawSeriesChunk::try_new(
            &schema,
            key.clone(),
            None,
            times(vec![1, 2]),
            float_fields(vec![Some(1.0)], None),
            true,
        )
        .is_err()
    );
    assert!(
        RawSeriesChunk::try_new(
            &schema,
            key.clone(),
            None,
            times(vec![]),
            float_fields(vec![], None),
            true,
        )
        .is_err()
    );

    let grid = Arc::new(EvalGrid::try_new(GridSpec::try_new(10, 5, 2).unwrap()).unwrap());
    let grid_schema = SeriesSchema {
        fields: schema.fields.clone(),
        eval_grid: Some(grid),
    };
    assert!(
        RawSeriesChunk::try_new(&grid_schema, key, None, times(vec![10, 15]), fields, true,)
            .is_err()
    );
}

#[test]
fn sliced_axes_and_chunks_keep_shared_buffers_and_values_alive() {
    let time_owner: ScalarBuffer<i64> = vec![0, 10, 20, 30, 40, 80].into();
    let time_source = RawTimestamps::try_new(time_owner.clone()).unwrap();
    let time = time_source.slice(1, 4).unwrap();
    let time_buffer = time.as_buffer();
    assert_eq!(time_buffer.as_ptr(), time_owner.as_ptr().wrapping_add(1));

    let nan_payload = f64::from_bits(0x7ff8_0000_0000_0042);
    let stale = f64::from_bits(0x7ff0_0000_0000_0002);
    let float_owner = Float64Array::from(vec![
        Some(90.0),
        Some(1.0),
        None,
        Some(nan_payload),
        Some(stale),
        Some(80.0),
    ]);
    let second_owner = Float64Array::from(vec![
        Some(190.0),
        Some(11.0),
        Some(12.0),
        None,
        Some(14.0),
        Some(180.0),
    ]);
    let fields = SeriesFields::try_from_vec(vec![
        Samples::from_float(float_owner.slice(1, 4)),
        Samples::from_float(second_owner.slice(1, 4)),
    ])
    .unwrap();
    let key_source = Buffer::from(vec![0_u8, 0xA1, 0xB2, 0xC3, 0xD4, 0xFF]);
    let primary_key = key_source.slice_with_length(1, 4);
    assert_eq!(primary_key.as_ptr(), key_source.as_ptr().wrapping_add(1));

    let schema = schema(&["first", "second"]);
    let chunk =
        RawSeriesChunk::try_new(&schema, primary_key, Some(metric()), time, fields, true).unwrap();
    let Some(crate::series::samples::DenseSamplesRef::Float(first)) =
        chunk.fields.get(0).unwrap().dense()
    else {
        panic!()
    };
    assert_eq!(
        first.values().as_ptr(),
        float_owner.values().as_ptr().wrapping_add(1)
    );

    drop(key_source);
    drop(time_source);
    drop(time_owner);
    drop(float_owner);
    drop(second_owner);
    assert_eq!(chunk.primary_key.as_slice(), &[0xA1, 0xB2, 0xC3, 0xD4]);
    assert_eq!(
        (0..4)
            .map(|i| chunk.time.timestamp(i).unwrap())
            .collect::<Vec<_>>(),
        [10, 20, 30, 40]
    );
    assert!(matches!(
        chunk.fields.get(0).unwrap().sample(0),
        SampleRef::Float(1.0)
    ));
    assert!(matches!(
        chunk.fields.get(0).unwrap().sample(1),
        SampleRef::Missing
    ));
    assert!(
        matches!(chunk.fields.get(0).unwrap().sample(2), SampleRef::Float(v) if v.to_bits() == nan_payload.to_bits())
    );
    assert!(
        matches!(chunk.fields.get(0).unwrap().sample(3), SampleRef::Float(v) if v.to_bits() == stale.to_bits())
    );
    assert!(matches!(
        chunk.fields.get(1).unwrap().sample(0),
        SampleRef::Float(11.0)
    ));
    assert!(matches!(
        chunk.fields.get(1).unwrap().sample(1),
        SampleRef::Float(12.0)
    ));
    assert!(matches!(
        chunk.fields.get(1).unwrap().sample(2),
        SampleRef::Missing
    ));
    assert!(matches!(
        chunk.fields.get(1).unwrap().sample(3),
        SampleRef::Float(14.0)
    ));
    assert_eq!(chunk.metric.as_ref().unwrap().name(), Some("cpu"));
    assert_eq!(chunk.metric.as_ref().unwrap().get("host"), Some("node-a"));
    assert!(chunk.is_last);
}

#[test]
fn raw_timestamps_check_bounds_and_retain_exact_end_slices() {
    let source: ScalarBuffer<i64> = vec![4, 8, 12].into();
    let axis = RawTimestamps::try_new(source.clone()).unwrap();
    assert!(axis.slice(usize::MAX, 1).is_err());
    assert!(axis.slice(2, usize::MAX).is_err());
    assert!(axis.slice(3, 1).is_err());
    assert!(axis.slice(usize::MAX, 0).is_err());
    let empty = axis.slice(3, 0).unwrap();
    assert!(empty.is_empty());
    assert_eq!(axis.timestamp(3), None);
    let end = axis.slice(1, 2).unwrap();
    drop(axis);
    drop(source);
    assert_eq!(end.timestamp(0), Some(8));
    assert_eq!(end.timestamp(1), Some(12));
}

#[test]
fn raw_chunks_preserve_native_unmaterialized_and_explicit_metrics() {
    let schema = schema(&["value"]);
    let fields = float_fields(vec![Some(2.0)], None);
    let native = RawSeriesChunk::try_new(
        &schema,
        Buffer::from(vec![0_u8, 0x12, 0x34, 0x56]),
        None,
        times(vec![7]),
        fields.clone(),
        false,
    )
    .unwrap();
    let tagless = RawSeriesChunk::try_new(
        &schema,
        Buffer::from(Vec::<u8>::new()),
        None,
        times(vec![7]),
        fields.clone(),
        false,
    )
    .unwrap();
    let explicit = RawSeriesChunk::try_new(
        &schema,
        Buffer::from(vec![0_u8, 0xAB, 0xCD, 0xEF]),
        Some(metric()),
        times(vec![7]),
        fields,
        false,
    )
    .unwrap();
    assert_eq!(native.primary_key.as_slice(), &[0, 0x12, 0x34, 0x56]);
    assert!(native.metric.is_none());
    assert!(tagless.primary_key.is_empty());
    assert!(tagless.metric.is_none());
    assert_eq!(explicit.primary_key.as_slice(), &[0, 0xAB, 0xCD, 0xEF]);
    assert_eq!(explicit.metric.as_ref().unwrap().name(), Some("cpu"));
    assert_eq!(
        explicit.metric.as_ref().unwrap().get("host"),
        Some("node-a")
    );
    assert!(!explicit.is_last);
}
