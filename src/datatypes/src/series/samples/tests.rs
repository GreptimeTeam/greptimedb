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
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow::array::{Float64Array, UInt32Array};
use arrow::buffer::{Buffer, ScalarBuffer};

use crate::series::histogram::{BucketSideView, CounterResetHint, HistogramBatchBuilder};

use crate::series::time::{EvalGrid, GridRange, GridSpec, TimeAxis};

fn batch(sum: f64, count: f64, buckets: &[f64]) -> Arc<HistogramBatch> {
    let offsets = if buckets.is_empty() { vec![] } else { vec![0] };
    let lengths = if buckets.is_empty() {
        vec![]
    } else {
        vec![buckets.len() as i32]
    };
    let side = BucketSideView {
        span_offsets: &offsets,
        span_lengths: &lengths,
        buckets,
    };
    let row = HistogramView {
        schema: 1,
        zero_threshold: 0.0,
        sum,
        count,
        zero_count: 0.0,
        reset_hint: CounterResetHint::Unknown,
        start_timestamp: None,
        custom_values: &[],
        positive: side,
        negative: BucketSideView {
            span_offsets: &[],
            span_lengths: &[],
            buckets: &[],
        },
    };
    histogram_batch(&[row])
}

fn histogram_batch(rows: &[HistogramView<'_>]) -> Arc<HistogramBatch> {
    let mut builder = HistogramBatchBuilder::new();
    for row in rows {
        builder.push(*row).unwrap();
    }
    builder.finish().unwrap()
}

struct FloatBufferOwner {
    values: Vec<f64>,
    drops: Arc<AtomicUsize>,
}

impl Drop for FloatBufferOwner {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

fn owned_floats(values: Vec<f64>, drops: Arc<AtomicUsize>) -> ScalarBuffer<f64> {
    let mut owner = FloatBufferOwner { values, drops };
    let pointer = std::ptr::NonNull::new(owner.values.as_mut_ptr().cast()).unwrap();
    let len = owner.values.len() * std::mem::size_of::<f64>();
    let element_count = owner.values.len();
    // SAFETY: the vector allocation remains stable in the Arc owner for the buffer lifetime.
    let buffer = unsafe { Buffer::from_custom_allocation(pointer, len, Arc::new(owner)) };
    ScalarBuffer::new(buffer, 0, element_count)
}

fn same_histogram(a: HistogramView<'_>, b: HistogramView<'_>) -> bool {
    a.schema == b.schema
        && a.zero_threshold.to_bits() == b.zero_threshold.to_bits()
        && a.sum.to_bits() == b.sum.to_bits()
        && a.count.to_bits() == b.count.to_bits()
        && a.zero_count.to_bits() == b.zero_count.to_bits()
        && a.reset_hint == b.reset_hint
        && a.start_timestamp == b.start_timestamp
        && a.custom_values
            .iter()
            .map(|value| value.to_bits())
            .eq(b.custom_values.iter().map(|value| value.to_bits()))
        && a.positive.span_offsets == b.positive.span_offsets
        && a.positive.span_lengths == b.positive.span_lengths
        && a.positive.buckets.iter().map(|value| value.to_bits()).eq(b
            .positive
            .buckets
            .iter()
            .map(|value| value.to_bits()))
        && a.negative.span_offsets == b.negative.span_offsets
        && a.negative.span_lengths == b.negative.span_lengths
        && a.negative.buckets.iter().map(|value| value.to_bits()).eq(b
            .negative
            .buckets
            .iter()
            .map(|value| value.to_bits()))
}
fn same_sample(a: SampleRef<'_>, b: SampleRef<'_>) -> bool {
    match (a, b) {
        (SampleRef::Missing, SampleRef::Missing) => true,
        (SampleRef::Float(a), SampleRef::Float(b)) => a.to_bits() == b.to_bits(),
        (SampleRef::Histogram(a), SampleRef::Histogram(b)) => same_histogram(a, b),
        _ => false,
    }
}

#[test]
fn float_states_and_slice_share_values_and_bitmap_after_owner_drop() {
    let stale = f64::from_bits(0x7ff0_0000_0000_0002);
    let owner = Float64Array::from(vec![Some(1.0), None, Some(f64::NAN), Some(stale)]);
    let samples = Samples::from_float(owner.clone());
    assert!(matches!(samples.sample(1), SampleRef::Missing));
    assert!(matches!(samples.sample(2), SampleRef::Float(v) if v.is_nan()));
    assert!(matches!(samples.sample(3), SampleRef::Float(v) if v.to_bits() == stale.to_bits()));
    let sliced = samples.slice(1, 3).unwrap();
    let DenseSamplesRef::Float(slice) = sliced.dense().unwrap() else {
        panic!()
    };
    assert_eq!(
        slice.values().as_ptr(),
        owner.values().as_ptr().wrapping_add(1)
    );
    assert_eq!(
        slice.nulls().unwrap().buffer().as_ptr(),
        owner.nulls().unwrap().buffer().as_ptr()
    );
    assert!(slice.is_null(0));
    drop(samples);
    drop(owner);
    assert!(matches!(sliced.sample(0), SampleRef::Missing));
    assert!(matches!(sliced.sample(1), SampleRef::Float(v) if v.is_nan()));
    assert!(matches!(sliced.sample(2), SampleRef::Float(v) if v.to_bits() == stale.to_bits()));
    assert!(sliced.slice(usize::MAX, 2).is_err());
}

#[test]
fn histogram_bounds_nulls_and_public_view_bounds_are_safe() {
    let owner = batch(2.0, 3.0, &[4.0, 5.0]);
    let refs =
        HistogramSamples::try_new(UInt32Array::from(vec![None, Some(0)]), owner.clone()).unwrap();
    assert!(refs.view_at(usize::MAX).is_none());
    assert!(refs.view_at(2).is_none());
    assert!(refs.view_at(0).is_none());
    assert_eq!(refs.view_at(1).unwrap().sum.to_bits(), 2.0f64.to_bits());
    assert!(HistogramSamples::try_new(UInt32Array::from(vec![Some(1)]), owner).is_err());
}

#[test]
fn mixed_validates_length_exclusivity_and_missing_positions() {
    let table = batch(2.0, 1.0, &[6.0]);
    let refs =
        HistogramSamples::try_new(UInt32Array::from(vec![None, Some(0), None]), table).unwrap();
    assert!(
        Samples::try_from_mixed(Float64Array::from(vec![None, Some(1.0)]), refs.clone()).is_err()
    );
    assert!(
        Samples::try_from_mixed(
            Float64Array::from(vec![None, Some(1.0), None]),
            refs.clone()
        )
        .is_err()
    );
    let mixed = Samples::try_from_mixed(Float64Array::from(vec![None, None, None]), refs).unwrap();
    assert!(matches!(mixed.sample(0), SampleRef::Missing));
    assert!(matches!(mixed.sample(1), SampleRef::Histogram(_)));
    assert!(matches!(mixed.sample(2), SampleRef::Missing));
}

#[test]
fn chunk_iterator_is_sequential_exact_and_handles_many_singletons() {
    let inner = Samples::try_from_chunks(vec![
        Samples::from_float(Float64Array::new_null(0)),
        Samples::from_float(Float64Array::from(vec![2.0])),
        Samples::from_float(Float64Array::from(vec![3.0])),
    ])
    .unwrap();
    let normalized = Samples::try_from_chunks(vec![
        Samples::from_float(Float64Array::from(vec![0.0, 1.0])),
        inner,
        Samples::from_float(Float64Array::new_null(0)),
        Samples::from_float(Float64Array::from(vec![4.0])),
    ])
    .unwrap();
    let SampleStorage::Chunks(parts, _) = &normalized.storage else {
        panic!()
    };
    assert_eq!(parts.len(), 4);
    assert_eq!(normalized.iter().len(), 5);
    for (index, part) in parts.iter().enumerate() {
        assert!(matches!(part.dense(), Some(DenseSamplesRef::Float(_))));
        assert!(matches!(part.sample(0), SampleRef::Float(v) if v == [0.0, 2.0, 3.0, 4.0][index]));
    }
    for (index, value) in normalized.iter().enumerate() {
        assert!(matches!(value, SampleRef::Float(v) if v == index as f64));
    }

    let collapsed = Samples::try_from_chunks(vec![
        Samples::from_float(Float64Array::new_null(0)),
        normalized.slice(2, 1).unwrap(),
        Samples::from_float(Float64Array::new_null(0)),
    ])
    .unwrap();
    assert!(matches!(collapsed.storage, SampleStorage::Float(_)));
    assert!(matches!(collapsed.sample(0), SampleRef::Float(v) if v == 2.0));
    let empty = Samples::try_from_chunks(vec![]).unwrap();
    assert!(empty.is_empty());
    assert!(matches!(empty.dense(), Some(DenseSamplesRef::Float(values)) if values.is_empty()));

    let chunks = Samples::try_from_chunks(
        (0..256)
            .map(|i| Samples::from_float(Float64Array::from(vec![i as f64])))
            .collect(),
    )
    .unwrap();
    let mut iter = chunks.iter();
    assert_eq!(iter.len(), 256);
    for i in 0..256 {
        assert!(matches!(iter.next(), Some(SampleRef::Float(v)) if v == i as f64));
        assert_eq!(iter.len(), 255 - i);
    }
    assert!(iter.next().is_none());
}

#[test]
fn densifying_slices_keeps_shared_histogram_table() {
    let table = batch(2.0, 3.0, &[4.0, 5.0]);
    for index in [Some(0), None] {
        let source =
            Samples::try_from_histogram(UInt32Array::from(vec![index; 64]), table.clone()).unwrap();
        let chunks = (0..64)
            .map(|sample_index| source.slice(sample_index, 1).unwrap())
            .collect();
        let dense = Samples::try_from_chunks(chunks)
            .unwrap()
            .into_dense()
            .unwrap();
        drop(source);

        assert_eq!(dense.len(), 64);
        let DenseSamplesRef::Histogram(values) = dense.dense().unwrap() else {
            panic!()
        };
        assert_eq!(values.batch().len(), 1);
        assert!(Arc::ptr_eq(values.batch(), &table));
        for sample_index in 0..64 {
            match index {
                Some(0) => {
                    assert_eq!(values.indices().value(sample_index), 0);
                    assert!(same_sample(
                        dense.sample(sample_index),
                        SampleRef::Histogram(table.view(0).unwrap())
                    ));
                }
                None => {
                    assert!(values.indices().is_null(sample_index));
                    assert!(matches!(dense.sample(sample_index), SampleRef::Missing));
                }
                _ => unreachable!(),
            }
        }
    }
}

#[test]
fn densifying_histogram_and_mixed_chunks_keeps_shared_table() {
    let stale = f64::from_bits(0x7ff0_0000_0000_0002);
    let nan = f64::from_bits(0x7ff8_0000_0000_0042);
    let table = batch(2.0, 3.0, &[4.0, 5.0]);
    let histograms =
        Samples::try_from_histogram(UInt32Array::from(vec![Some(0), None]), table.clone()).unwrap();
    let mixed_histograms =
        HistogramSamples::try_new(UInt32Array::from(vec![None, Some(0)]), table.clone()).unwrap();
    let mixed =
        Samples::try_from_mixed(Float64Array::from(vec![Some(-0.0), None]), mixed_histograms)
            .unwrap();
    let chunked = Samples::try_from_chunks(vec![
        Samples::from_float(Float64Array::from(vec![Some(stale), None, Some(nan)])),
        histograms,
        mixed,
    ])
    .unwrap();
    let expected = [
        SampleRef::Float(stale),
        SampleRef::Missing,
        SampleRef::Float(nan),
        SampleRef::Histogram(table.view(0).unwrap()),
        SampleRef::Missing,
        SampleRef::Float(-0.0),
        SampleRef::Histogram(table.view(0).unwrap()),
    ];
    let dense = chunked.into_dense().unwrap();

    assert_eq!(dense.len(), 7);
    let DenseSamplesRef::Mixed(values) = dense.dense().unwrap() else {
        panic!()
    };
    assert_eq!(values.histograms.batch().len(), 1);
    assert!(Arc::ptr_eq(values.histograms.batch(), &table));
    for (index, expected) in expected.into_iter().enumerate() {
        assert!(
            same_sample(expected, dense.sample(index)),
            "position {index}"
        );
    }
    assert_eq!(values.histograms.indices().value(3), 0);
    assert_eq!(values.histograms.indices().value(6), 0);

    let all_null_mixed = Samples::try_from_mixed(
        Float64Array::new_null(2),
        HistogramSamples::try_new(UInt32Array::from(vec![None, Some(0)]), table.clone()).unwrap(),
    )
    .unwrap();
    let dense = Samples::try_from_chunks(vec![
        Samples::try_from_histogram(UInt32Array::from(vec![None]), table.clone()).unwrap(),
        all_null_mixed,
    ])
    .unwrap()
    .into_dense()
    .unwrap();
    let DenseSamplesRef::Mixed(values) = dense.dense().unwrap() else {
        panic!()
    };
    assert_eq!(dense.len(), 3);
    assert!(Arc::ptr_eq(values.histograms.batch(), &table));
    assert_eq!(values.floats.null_count(), 3);
    assert!(values.histograms.indices().is_null(0));
    assert!(values.histograms.indices().is_null(1));
    assert_eq!(values.histograms.indices().value(2), 0);
    assert!(matches!(dense.sample(0), SampleRef::Missing));
    assert!(matches!(dense.sample(1), SampleRef::Missing));
    assert!(same_sample(
        dense.sample(2),
        SampleRef::Histogram(table.view(0).unwrap())
    ));
}

#[test]
fn densification_preserves_distinct_shared_table_owner_values() {
    let first = batch(2.0, 3.0, &[4.0, 5.0]);
    let second = batch(6.0, 7.0, &[8.0, 9.0]);

    for owners in [
        vec![first.clone(), second.clone()],
        vec![first.clone(), first.clone(), second.clone()],
    ] {
        let expected: Vec<_> = owners
            .iter()
            .flat_map(|owner| {
                [
                    SampleRef::Missing,
                    SampleRef::Histogram(owner.view(0).unwrap()),
                ]
            })
            .collect();
        let chunks = owners
            .iter()
            .map(|owner| {
                Samples::try_from_histogram(UInt32Array::from(vec![None, Some(0)]), owner.clone())
                    .unwrap()
            })
            .collect();
        let dense = Samples::try_from_chunks(chunks)
            .unwrap()
            .into_dense()
            .unwrap();

        assert_eq!(dense.len(), expected.len());
        let DenseSamplesRef::Histogram(values) = dense.dense().unwrap() else {
            panic!()
        };
        for (index, expected) in expected.into_iter().enumerate() {
            if !values.indices().is_null(index) {
                assert!((values.indices().value(index) as usize) < values.batch().len());
            }
            assert!(
                same_sample(expected, dense.sample(index)),
                "position {index}"
            );
        }
    }
}

#[test]
fn densification_preserves_values_nulls_nan_and_remaps_multitable_histograms() {
    let stale = f64::from_bits(0x7ff0_0000_0000_0002);
    let float = Samples::from_float(Float64Array::from(vec![Some(1.0), None, Some(stale)]));
    let first_buckets = [13.0, 14.0];
    let first_positive_offsets = [0];
    let first_positive_lengths = [2];
    let first_negative_offsets = [0];
    let first_negative_lengths = [1];
    let first_negative_buckets = [15.0];
    let first_custom = [16.0, 17.0];
    let first_rows = [
        HistogramView {
            schema: 2,
            zero_threshold: 0.25,
            sum: f64::from_bits(0x7ff0_0000_0000_0002),
            count: 12.5,
            zero_count: 1.5,
            reset_hint: CounterResetHint::Unrecognized(9),
            start_timestamp: Some(123),
            custom_values: &first_custom,
            positive: BucketSideView {
                span_offsets: &first_positive_offsets,
                span_lengths: &first_positive_lengths,
                buckets: &first_buckets,
            },
            negative: BucketSideView {
                span_offsets: &first_negative_offsets,
                span_lengths: &first_negative_lengths,
                buckets: &first_negative_buckets,
            },
        },
        HistogramView {
            schema: 1,
            zero_threshold: 0.0,
            sum: -21.0,
            count: 22.0,
            zero_count: 0.0,
            reset_hint: CounterResetHint::Unknown,
            start_timestamp: None,
            custom_values: &[],
            positive: BucketSideView {
                span_offsets: &[0],
                span_lengths: &[1],
                buckets: &[23.0],
            },
            negative: BucketSideView {
                span_offsets: &[],
                span_lengths: &[],
                buckets: &[],
            },
        },
    ];
    let first_table = histogram_batch(&first_rows);
    let first =
        Samples::try_from_histogram(UInt32Array::from(vec![None, Some(1)]), first_table.clone())
            .unwrap();
    let second_positive_offsets = [0];
    let second_positive_lengths = [1];
    let second_positive_buckets = [31.0];
    let second_rows = [
        HistogramView {
            schema: 3,
            zero_threshold: 0.5,
            sum: 32.0,
            count: f64::from_bits(0x7ff0_0000_0000_0002),
            zero_count: 2.25,
            reset_hint: CounterResetHint::CounterReset,
            start_timestamp: Some(-456),
            custom_values: &[33.0],
            positive: BucketSideView {
                span_offsets: &second_positive_offsets,
                span_lengths: &second_positive_lengths,
                buckets: &second_positive_buckets,
            },
            negative: BucketSideView {
                span_offsets: &[0],
                span_lengths: &[1],
                buckets: &[34.0],
            },
        },
        HistogramView {
            schema: 4,
            zero_threshold: 0.75,
            sum: 35.0,
            count: 36.0,
            zero_count: 3.5,
            reset_hint: CounterResetHint::Gauge,
            start_timestamp: None,
            custom_values: &[37.0, 38.0],
            positive: BucketSideView {
                span_offsets: &[0],
                span_lengths: &[2],
                buckets: &[39.0, 40.0],
            },
            negative: BucketSideView {
                span_offsets: &[],
                span_lengths: &[],
                buckets: &[],
            },
        },
        HistogramView {
            schema: 5,
            zero_threshold: 1.0,
            sum: 41.0,
            count: 42.0,
            zero_count: 4.5,
            reset_hint: CounterResetHint::NotCounterReset,
            start_timestamp: Some(789),
            custom_values: &[],
            positive: BucketSideView {
                span_offsets: &[],
                span_lengths: &[],
                buckets: &[],
            },
            negative: BucketSideView {
                span_offsets: &[0],
                span_lengths: &[2],
                buckets: &[43.0, 44.0],
            },
        },
    ];
    let second_table = histogram_batch(&second_rows);
    let second_refs =
        HistogramSamples::try_new(UInt32Array::from(vec![Some(2), None]), second_table.clone())
            .unwrap();
    let mixed =
        Samples::try_from_mixed(Float64Array::from(vec![None, Some(f64::NAN)]), second_refs)
            .unwrap();
    let chunked = Samples::try_from_chunks(vec![float, first.clone(), mixed]).unwrap();
    let expected: Vec<_> = chunked.iter().collect();
    let dense = chunked.clone().into_dense().unwrap();
    assert_eq!(dense.len(), expected.len());
    for (i, before) in expected.into_iter().enumerate() {
        assert!(same_sample(before, dense.sample(i)), "position {i}");
    }

    let float_only = Samples::try_from_chunks(vec![
        Samples::from_float(Float64Array::from(vec![Some(1.0), None])),
        Samples::from_float(Float64Array::from(vec![Some(f64::NAN), Some(stale)])),
    ])
    .unwrap();
    let float_expected: Vec<_> = float_only.iter().collect();
    let float_dense = float_only.clone().into_dense().unwrap();
    for (index, expected) in float_expected.into_iter().enumerate() {
        assert!(same_sample(expected, float_dense.sample(index)));
    }
    let DenseSamplesRef::Mixed(dense_mixed) = dense.dense().unwrap() else {
        panic!()
    };
    assert!(
        dense_mixed.histograms().indices().get_buffer_memory_size()
            + dense_mixed.floats().get_buffer_memory_size()
            < 4096,
        "retained numeric buffer bytes: {}",
        dense_mixed.histograms().indices().get_buffer_memory_size()
            + dense_mixed.floats().get_buffer_memory_size()
    );
    assert_eq!(dense_mixed.histograms.indices().value(4), 1);
    assert_eq!(dense_mixed.histograms.indices().value(5), 4);
    assert_eq!(dense_mixed.histograms.batch().len(), 5);
    for index in 0..5 {
        let actual = dense_mixed.histograms.batch().view(index).unwrap();
        let expected = if index < 2 {
            first_table.view(index).unwrap()
        } else {
            second_table.view(index - 2).unwrap()
        };
        assert!(same_sample(
            SampleRef::Histogram(actual),
            SampleRef::Histogram(expected)
        ));
    }

    let pure_hist = Samples::try_from_chunks(vec![
        first.clone(),
        Samples::try_from_histogram(UInt32Array::from(vec![Some(0)]), second_table).unwrap(),
    ])
    .unwrap();
    let dense_hist = pure_hist.clone().into_dense().unwrap();
    assert!(matches!(
        dense_hist.dense(),
        Some(DenseSamplesRef::Histogram(_))
    ));
    for index in 0..pure_hist.len() {
        assert!(same_sample(
            pure_hist.sample(index),
            dense_hist.sample(index)
        ));
    }

    let SampleStorage::Histogram(first_refs) = &first.storage else {
        panic!()
    };
    let sliced = first_refs.slice(1, 1);
    assert!(Arc::ptr_eq(sliced.batch(), &first_table));
    assert_eq!(
        sliced.indices().values().as_ptr(),
        first_refs.indices().values().as_ptr().wrapping_add(1)
    );
}

#[test]
fn chunk_slices_keep_indices_tables_and_numeric_buffers_alive() {
    let f = Float64Array::from(vec![Some(1.0), None, Some(3.0)]);
    let float_samples = Samples::from_float(f.clone());
    let hist_table = batch(7.0, 8.0, &[9.0]);
    let hist_samples = Samples::try_from_histogram(
        UInt32Array::from(vec![None, Some(0), None]),
        hist_table.clone(),
    )
    .unwrap();
    let mixed_table = batch(17.0, 18.0, &[19.0]);
    let mixed_values = Float64Array::from(vec![Some(20.0), None, None]);
    let mixed = Samples::try_from_mixed(
        mixed_values.clone(),
        HistogramSamples::try_new(
            UInt32Array::from(vec![None, None, Some(0)]),
            mixed_table.clone(),
        )
        .unwrap(),
    )
    .unwrap();
    let chunks = Samples::try_from_chunks(vec![
        float_samples.clone(),
        hist_samples.clone(),
        mixed.clone(),
    ])
    .unwrap();
    let sliced = chunks.slice(1, 8).unwrap();
    for index in 0..8 {
        assert!(same_sample(chunks.sample(index + 1), sliced.sample(index)));
    }
    let SampleStorage::Chunks(parts, _) = &sliced.storage else {
        panic!()
    };
    let SampleStorage::Float(fpart) = &parts[0].storage else {
        panic!()
    };
    assert_eq!(fpart.values().as_ptr(), f.values().as_ptr().wrapping_add(1));
    assert_eq!(
        fpart.nulls().unwrap().buffer().as_ptr(),
        f.nulls().unwrap().buffer().as_ptr()
    );
    let SampleStorage::Histogram(hpart) = &parts[1].storage else {
        panic!()
    };
    assert!(Arc::ptr_eq(hpart.batch(), &hist_table));
    let SampleStorage::Mixed(mpart) = &parts[2].storage else {
        panic!()
    };
    assert!(Arc::ptr_eq(mpart.histograms.batch(), &mixed_table));
    assert_eq!(
        mpart.floats.values().as_ptr(),
        mixed_values.values().as_ptr()
    );
    drop(chunks);
    drop(float_samples);
    drop(hist_samples);
    drop(mixed);
    drop(f);
    drop(mixed_values);
    drop(hist_table);
    drop(mixed_table);
    assert!(matches!(sliced.sample(1), SampleRef::Float(v) if v == 3.0));
    assert!(matches!(sliced.sample(7), SampleRef::Histogram(_)));
}

#[test]
fn fields_clone_buffers_and_series_validate_schema_alignment_and_time_order() {
    let sample = Samples::from_float(Float64Array::from(vec![1.0, 2.0]));
    let fields = SeriesFields::try_from_vec(vec![sample.clone(), sample.clone()]).unwrap();
    assert!(matches!(fields.storage, Fields::Many(_)));
    let cloned = fields.clone();
    let DenseSamplesRef::Float(a) = fields.get(0).unwrap().dense().unwrap() else {
        panic!()
    };
    let DenseSamplesRef::Float(b) = cloned.get(0).unwrap().dense().unwrap() else {
        panic!()
    };
    assert_eq!(a.values().as_ptr(), b.values().as_ptr());
    assert!(SeriesFields::try_from_vec(vec![]).is_err());
    assert!(
        SeriesFields::try_from_vec(vec![
            sample.clone(),
            Samples::from_float(Float64Array::from(vec![1.0]))
        ])
        .is_err()
    );
    let schema = SeriesSchema {
        fields: Arc::from([
            FieldSpec {
                name: Arc::from("a"),
            },
            FieldSpec {
                name: Arc::from("b"),
            },
        ]),
        eval_grid: None,
    };
    let metric = MetricName::try_new(None, vec![]).unwrap();
    let time = TimeAxis::Explicit(ScalarBuffer::from(vec![1, 2]));
    assert!(
        Series::try_new(
            &schema,
            metric.clone(),
            time.clone(),
            SeriesFields::one(sample.clone())
        )
        .is_err()
    );
    assert!(
        Series::try_new(
            &schema,
            metric.clone(),
            time.clone(),
            SeriesFields::try_from_vec(vec![
                Samples::from_float(Float64Array::from(vec![1.0])),
                Samples::from_float(Float64Array::from(vec![2.0]))
            ])
            .unwrap()
        )
        .is_err()
    );
    let unordered = TimeAxis::Explicit(ScalarBuffer::from(vec![2, 1]));
    assert!(Series::try_new(&schema, metric.clone(), unordered, fields.clone()).is_err());
    assert!(Series::try_new(&schema, metric, time, fields).is_ok());
}

#[test]
fn float_fields_clones_and_series_keep_real_arrow_buffer_owner_alive() {
    let drops = Arc::new(AtomicUsize::new(0));
    let values = Float64Array::new(owned_floats(vec![7.0, 8.0], drops.clone()), None);
    let samples = Samples::from_float(values);
    let fields = SeriesFields::one(samples);
    let cloned = fields.clone();
    let schema = SeriesSchema {
        fields: Arc::from([FieldSpec {
            name: Arc::from("value"),
        }]),
        eval_grid: None,
    };
    let series = Series::try_new(
        &schema,
        MetricName::try_new(None, vec![]).unwrap(),
        TimeAxis::Explicit(ScalarBuffer::from(vec![1_i64, 2])),
        fields,
    )
    .unwrap();
    let DenseSamplesRef::Float(cloned_values) = cloned.get(0).unwrap().dense().unwrap() else {
        panic!()
    };
    let DenseSamplesRef::Float(series_values) = series.fields.get(0).unwrap().dense().unwrap()
    else {
        panic!()
    };
    assert_eq!(
        cloned_values.values().as_ptr(),
        series_values.values().as_ptr()
    );
    assert_eq!(cloned_values.values(), &[7.0, 8.0]);

    drop(series);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(cloned);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[test]
fn series_enforces_grid_policy_and_matches_grid_spec_by_value() {
    let spec = GridSpec::try_new(100, 10, 3).unwrap();
    let schema_grid = Arc::new(EvalGrid::try_new(spec).unwrap());
    let equal_grid = Arc::new(EvalGrid::try_new(spec).unwrap());
    let range = GridRange::try_new(equal_grid, 1, 1).unwrap();
    let fields = SeriesFields::one(Samples::from_float(Float64Array::from(vec![1.0])));
    let metric = MetricName::try_new(None, vec![]).unwrap();
    let grid_schema = SeriesSchema {
        fields: Arc::from([FieldSpec {
            name: Arc::from("value"),
        }]),
        eval_grid: Some(schema_grid.clone()),
    };

    assert!(
        Series::try_new(
            &grid_schema,
            metric.clone(),
            TimeAxis::Grid(range),
            fields.clone()
        )
        .is_ok()
    );
    // Timestamp 110 overlaps, but a different grid specification is not interchangeable.
    let different_grid =
        Arc::new(EvalGrid::try_new(GridSpec::try_new(110, 5, 3).unwrap()).unwrap());
    assert!(
        Series::try_new(
            &grid_schema,
            metric.clone(),
            TimeAxis::Grid(GridRange::try_new(different_grid, 0, 1).unwrap()),
            fields.clone(),
        )
        .is_err()
    );

    let explicit_schema = SeriesSchema {
        fields: grid_schema.fields.clone(),
        eval_grid: None,
    };
    assert!(
        Series::try_new(
            &explicit_schema,
            metric.clone(),
            TimeAxis::Explicit(ScalarBuffer::from(vec![110])),
            fields.clone(),
        )
        .is_ok()
    );
    assert!(
        Series::try_new(
            &explicit_schema,
            metric.clone(),
            TimeAxis::Grid(GridRange::try_new(schema_grid.clone(), 0, 1).unwrap()),
            fields.clone(),
        )
        .is_err()
    );
    assert!(
        Series::try_new(
            &grid_schema,
            metric.clone(),
            TimeAxis::Explicit(ScalarBuffer::from(vec![110])),
            fields.clone(),
        )
        .is_err()
    );
    for (different_spec, range_start) in [
        (GridSpec::try_new(90, 10, 3).unwrap(), 1),
        (GridSpec::try_new(100, 5, 3).unwrap(), 2),
        (GridSpec::try_new(100, 10, 4).unwrap(), 1),
    ] {
        let other_grid = Arc::new(EvalGrid::try_new(different_spec).unwrap());
        assert!(
            Series::try_new(
                &grid_schema,
                metric.clone(),
                TimeAxis::Grid(GridRange::try_new(other_grid, range_start, 1).unwrap()),
                fields.clone(),
            )
            .is_err()
        );
    }

    let empty_fields = SeriesFields::one(Samples::from_float(Float64Array::new_null(0)));
    assert!(
        Series::try_new(
            &grid_schema,
            metric.clone(),
            TimeAxis::Grid(GridRange::try_new(schema_grid, 3, 0).unwrap()),
            empty_fields.clone(),
        )
        .is_ok()
    );
    assert!(
        Series::try_new(
            &explicit_schema,
            metric.clone(),
            TimeAxis::Explicit(ScalarBuffer::from(Vec::<i64>::new())),
            empty_fields.clone(),
        )
        .is_ok()
    );
    assert!(
        Series::try_new(
            &grid_schema,
            metric.clone(),
            TimeAxis::Explicit(ScalarBuffer::from(Vec::<i64>::new())),
            empty_fields.clone(),
        )
        .is_err()
    );
    assert!(
        Series::try_new(
            &explicit_schema,
            metric,
            TimeAxis::Grid(
                GridRange::try_new(grid_schema.eval_grid.as_ref().unwrap().clone(), 3, 0).unwrap()
            ),
            empty_fields,
        )
        .is_err()
    );
}

#[test]
fn short_multifield_series_uses_its_grid_range_and_checks_explicit_chunk_order() {
    let grid = Arc::new(EvalGrid::try_new(GridSpec::try_new(0, 1, 2017).unwrap()).unwrap());
    let float = Samples::from_float(Float64Array::from(
        (0..20)
            .map(|i| {
                if i == 4 {
                    None
                } else if i == 7 {
                    Some(f64::NAN)
                } else {
                    Some(i as f64)
                }
            })
            .collect::<Vec<_>>(),
    ));
    let histogram_table = batch(2.0, 3.0, &[4.0]);
    let histogram = Samples::try_from_histogram(
        UInt32Array::from(vec![Some(0); 20]),
        histogram_table.clone(),
    )
    .unwrap();
    let fields = SeriesFields::try_from_vec(vec![float, histogram]).unwrap();
    let schema = SeriesSchema {
        fields: Arc::from([
            FieldSpec {
                name: Arc::from("float"),
            },
            FieldSpec {
                name: Arc::from("histogram"),
            },
        ]),
        eval_grid: Some(grid.clone()),
    };
    let series = Series::try_new(
        &schema,
        MetricName::try_new(None, vec![]).unwrap(),
        TimeAxis::Grid(GridRange::try_new(grid, 800, 20).unwrap()),
        fields.clone(),
    )
    .unwrap();
    assert_eq!(series.time.len(), 20);
    assert!(matches!(
        series.fields.get(0).unwrap().sample(4),
        SampleRef::Missing
    ));
    assert!(matches!(series.fields.get(0).unwrap().sample(7), SampleRef::Float(v) if v.is_nan()));
    assert!(matches!(
        series.fields.get(1).unwrap().sample(19),
        SampleRef::Histogram(_)
    ));
    assert!(Arc::ptr_eq(
        match series.fields.get(1).unwrap().dense().unwrap() {
            DenseSamplesRef::Histogram(samples) => samples.batch(),
            _ => unreachable!(),
        },
        &histogram_table,
    ));

    let explicit_schema = SeriesSchema {
        fields: schema.fields.clone(),
        eval_grid: None,
    };
    let explicit_fields = SeriesFields::try_from_vec(vec![
        Samples::from_float(Float64Array::new_null(4)),
        Samples::try_from_histogram(UInt32Array::from(vec![None; 4]), histogram_table).unwrap(),
    ])
    .unwrap();
    let out_of_order = TimeAxis::ExplicitChunks(Arc::from([
        ScalarBuffer::from(vec![1_i64, 3]),
        ScalarBuffer::from(Vec::<i64>::new()),
        ScalarBuffer::from(vec![2_i64, 4]),
    ]));
    assert!(
        Series::try_new(
            &explicit_schema,
            MetricName::try_new(None, vec![]).unwrap(),
            out_of_order,
            explicit_fields,
        )
        .is_err()
    );
    let ordered_chunks = TimeAxis::ExplicitChunks(Arc::from([
        ScalarBuffer::from(vec![1_i64, 3]),
        ScalarBuffer::from(Vec::<i64>::new()),
        ScalarBuffer::from(vec![5_i64]),
    ]));
    let ordered_fields = SeriesFields::try_from_vec(vec![
        Samples::from_float(Float64Array::from(vec![1.0, 2.0, 3.0])),
        Samples::from_float(Float64Array::from(vec![4.0, 5.0, 6.0])),
    ])
    .unwrap();
    assert!(
        Series::try_new(
            &explicit_schema,
            MetricName::try_new(None, vec![]).unwrap(),
            ordered_chunks.clone(),
            ordered_fields.clone(),
        )
        .is_ok()
    );
    let grid = Arc::new(EvalGrid::try_new(GridSpec::try_new(1, 2, 3).unwrap()).unwrap());
    let grid_schema = SeriesSchema {
        fields: explicit_schema.fields.clone(),
        eval_grid: Some(grid),
    };
    assert!(
        Series::try_new(
            &grid_schema,
            MetricName::try_new(None, vec![]).unwrap(),
            ordered_chunks,
            ordered_fields,
        )
        .is_err()
    );
}
