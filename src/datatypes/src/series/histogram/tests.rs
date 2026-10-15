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

use super::*;

fn row(start: Option<i64>, hint: CounterResetHint) -> HistogramView<'static> {
    HistogramView {
        schema: 1,
        zero_threshold: 0.5,
        sum: f64::from_bits(0x7ff8_0000_0000_0042),
        count: 3.0,
        zero_count: -0.0,
        reset_hint: hint,
        start_timestamp: start,
        custom_values: &[1.25, 2.5],
        positive: BucketSideView {
            span_offsets: &[0, 1],
            span_lengths: &[2, 1],
            buckets: &[0.0, -0.0, 7.0],
        },
        negative: BucketSideView {
            span_offsets: &[-3],
            span_lengths: &[1],
            buckets: &[4.0],
        },
    }
}
fn batch() -> Arc<HistogramBatch> {
    let mut b = HistogramBatchBuilder::new();
    b.push(row(None, CounterResetHint::Unrecognized(42)))
        .unwrap();
    b.push(row(Some(123), CounterResetHint::Gauge)).unwrap();
    b.finish().unwrap()
}

#[test]
fn tiny_histogram_batch_does_not_retain_large_default_buffers() {
    let mut builder = HistogramBatchBuilder::new();
    builder.push(row(None, CounterResetHint::Unknown)).unwrap();
    let batch = builder.finish().unwrap();
    let retained = batch.as_arrow().get_buffer_memory_size();

    assert!(retained < 4096, "retained {retained} bytes");
}

#[test]
fn builder_views_and_arrow_buffer_borrowing() {
    let empty = HistogramBatchBuilder::new();
    assert!(empty.finish().unwrap().is_empty());
    let original = row(None, CounterResetHint::Unrecognized(42));
    let mut b = HistogramBatchBuilder::new();
    assert_eq!(b.push(original).unwrap(), 0);
    assert_eq!(b.push(row(Some(123), CounterResetHint::Gauge)).unwrap(), 1);
    let b = b.finish().unwrap();
    let v = b.view(0).unwrap();
    assert_eq!(
        (v.schema, v.zero_threshold, v.sum.to_bits(), v.count),
        (1, 0.5, original.sum.to_bits(), 3.0)
    );
    assert_eq!(v.reset_hint, CounterResetHint::Unrecognized(42));
    assert_eq!(v.start_timestamp, None);
    assert_eq!(v.zero_count.to_bits(), (-0.0f64).to_bits());
    assert_eq!(v.custom_values, &[1.25, 2.5]);
    assert_eq!(v.positive.span_offsets, &[0, 1]);
    assert_eq!(v.positive.span_lengths, &[2, 1]);
    assert_eq!(v.positive.buckets[1].to_bits(), (-0.0f64).to_bits());
    assert_eq!(v.negative.span_offsets, &[-3]);
    assert_eq!(v.negative.span_lengths, &[1]);
    assert_eq!(v.negative.buckets, &[4.0]);
    let second = b.view(1).unwrap();
    assert_eq!(second.start_timestamp, Some(123));
    assert_eq!(second.reset_hint, CounterResetHint::Gauge);
    assert!(b.view(usize::MAX).is_none());
    let field = b
        .as_arrow()
        .column(10)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    let values = field
        .values()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(v.positive.buckets.as_ptr(), values.values().as_ptr());
    let retained = b.as_arrow().clone();
    drop(b);
    let after = HistogramBatch::try_from_arrow(retained).unwrap();
    assert_eq!(after.view(0).unwrap().positive.buckets[2], 7.0);
}

#[test]
fn failed_push_does_not_advance_index() {
    let bad = HistogramView {
        positive: BucketSideView {
            span_offsets: &[0],
            span_lengths: &[-1],
            buckets: &[],
        },
        ..row(None, CounterResetHint::Unknown)
    };
    let mut b = HistogramBatchBuilder::new();
    assert!(b.push(bad).is_err());
    assert_eq!(b.push(row(None, CounterResetHint::Unknown)).unwrap(), 0);
    assert_eq!(
        b.finish().unwrap().view(0).unwrap().positive.buckets.len(),
        3
    );
}

#[test]
fn rejects_invalid_spans() {
    let cases = [
        (&[0][..], &[][..], &[][..]),
        (&[0][..], &[-1][..], &[][..]),
        (&[0, -1][..], &[1, 0][..], &[1.0][..]),
        (&[i32::MAX - 1, 1][..], &[1, 1][..], &[1.0, 2.0][..]),
        (&[0][..], &[2][..], &[1.0][..]),
    ];
    for (o, l, buckets) in cases {
        let r = HistogramView {
            positive: BucketSideView {
                span_offsets: o,
                span_lengths: l,
                buckets,
            },
            ..row(None, CounterResetHint::Unknown)
        };
        assert!(HistogramBatchBuilder::new().push(r).is_err());
    }
}

#[test]
fn reset_hint_codes_round_trip() {
    for (code, hint) in [
        (0, CounterResetHint::Unknown),
        (1, CounterResetHint::CounterReset),
        (2, CounterResetHint::NotCounterReset),
        (3, CounterResetHint::Gauge),
        (i32::MIN, CounterResetHint::Unrecognized(i32::MIN)),
        (99, CounterResetHint::Unrecognized(99)),
    ] {
        assert_eq!(i32::from(CounterResetHint::from(code)), code);
        assert_eq!(CounterResetHint::from(code), hint);
    }
}

#[test]
fn sliced_struct_list_and_primitive_values() {
    let b = batch();
    let source = b.as_arrow();
    let wrapped = HistogramBatch::try_from_arrow(source.slice(1, 1)).unwrap();
    assert_eq!(wrapped.view(0).unwrap().start_timestamp, Some(123));
    let mut columns = source.columns().to_vec();
    let original = columns[10].as_any().downcast_ref::<ListArray>().unwrap();
    let list = original.slice(1, 1);
    let values = list
        .values()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    let primitive = values.slice(1, values.len() - 1);
    let rebuilt = ListArray::try_new(
        Arc::new(Field::new_list_field(DataType::Float64, false)),
        OffsetBuffer::new(vec![2i32, 5].into()),
        Arc::new(primitive),
        None,
    )
    .unwrap();
    columns[10] = Arc::new(rebuilt);
    for (i, c) in columns.iter_mut().enumerate() {
        if i != 10 {
            *c = c.slice(1, 1)
        }
    }
    let struct_array = StructArray::try_new(source.fields().clone(), columns, None).unwrap();
    let got = HistogramBatch::try_from_arrow(struct_array).unwrap();
    assert_eq!(got.view(0).unwrap().positive.buckets, &[0.0, -0.0, 7.0]);
}

#[test]
fn rejects_wrong_schema_type_order_and_nullability() {
    let b = batch();
    let source = b.as_arrow();
    let mut c = source.columns().to_vec();
    let mut f = source.fields().iter().cloned().collect::<Vec<_>>();
    f.swap(0, 1);
    c.swap(0, 1);
    assert!(
        HistogramBatch::try_from_arrow(StructArray::try_new(f.into(), c, None).unwrap()).is_err()
    );
    let mut f = source.fields().iter().cloned().collect::<Vec<_>>();
    f[0] = Arc::new(Field::new("schema", DataType::Int32, true));
    assert!(
        HistogramBatch::try_from_arrow(
            StructArray::try_new(f.into(), source.columns().to_vec(), None).unwrap()
        )
        .is_err()
    );
    let mut f = source.fields().iter().cloned().collect::<Vec<_>>();
    f[0] = Arc::new(Field::new("schema", DataType::Float64, false));
    let mut c = source.columns().to_vec();
    c[0] = Arc::new(Float64Array::from(vec![1.0, 1.0]));
    assert!(
        HistogramBatch::try_from_arrow(StructArray::try_new(f.into(), c, None).unwrap()).is_err()
    );
}

#[test]
fn rejects_parent_and_list_item_nulls() {
    let b = batch();
    let source = b.as_arrow();
    let parent = StructArray::try_new(
        source.fields().clone(),
        source.columns().to_vec(),
        Some(arrow::buffer::NullBuffer::new_null(2)),
    )
    .unwrap();
    assert!(HistogramBatch::try_from_arrow(parent).is_err());
    let mut fields = source.fields().iter().cloned().collect::<Vec<_>>();
    fields[10] = Arc::new(Field::new(
        "positive_buckets",
        DataType::List(Arc::new(Field::new("item", DataType::Float64, true))),
        false,
    ));
    let list = source
        .column(10)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    let values = list
        .values()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    let mut null_bits = vec![true; values.len()];
    null_bits[1] = false;
    let nullable = Float64Array::new(
        values.values().clone(),
        Some(arrow::buffer::NullBuffer::from(null_bits)),
    );
    assert!(
        ListArray::try_new(
            Arc::new(Field::new("item", DataType::Float64, false)),
            list.offsets().clone(),
            Arc::new(nullable.clone()),
            None,
        )
        .is_err()
    );
    let rebuilt = ListArray::try_new(
        Arc::new(Field::new("item", DataType::Float64, true)),
        list.offsets().clone(),
        Arc::new(nullable),
        None,
    )
    .unwrap();
    let mut cols = source.columns().to_vec();
    cols[10] = Arc::new(rebuilt);
    let child = StructArray::try_new(fields.into(), cols, None).unwrap();
    assert!(HistogramBatch::try_from_arrow(child).is_err());
}

#[test]
fn preflight_checks_list_limits_without_large_allocations() {
    let r = row(None, CounterResetHint::Unknown);
    assert!(preflight(r, 0, [i32::MAX as usize, 0, 0, 0, 0, 0, 0]).is_err());
    assert!(preflight(r, 0, [0; 7]).is_ok());
}
