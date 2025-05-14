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

use std::str::FromStr;
use std::sync::Arc;

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use datatypes::arrow;
use datatypes::arrow::array::{ArrayRef, RecordBatch, TimestampMillisecondArray};
use datatypes::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datatypes::arrow_array::StringArray;
use mito2::memtable::{filter_record_batch, BulkPart};

fn random_array(num: usize) -> BulkPart {
    let mut min = i64::MAX;
    let mut max = i64::MIN;

    let ts_data: Vec<_> = (0..num)
        .map(|_| {
            let val = rand::random::<i64>();
            min = min.min(val);
            max = max.max(val);
            val
        })
        .collect();

    let val = StringArray::from_iter_values(ts_data.iter().map(|v| v.to_string()));
    let ts = TimestampMillisecondArray::from(ts_data);

    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("val", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(ts) as ArrayRef, Arc::new(val) as ArrayRef],
    )
    .unwrap();
    BulkPart {
        batch,
        num_rows: num,
        max_ts: max,
        min_ts: min,
        sequence: 0,
        timestamp_index: 0,
    }
}

fn filter_arrow_impl(part: &BulkPart, min: i64, max: i64) -> Option<BulkPart> {
    let ts_array = part.timestamps();
    let gt_eq =
        arrow::compute::kernels::cmp::gt_eq(ts_array, &TimestampMillisecondArray::new_scalar(min))
            .unwrap();
    let lt =
        arrow::compute::kernels::cmp::lt(ts_array, &TimestampMillisecondArray::new_scalar(max))
            .unwrap();
    let predicate = arrow::compute::and(&gt_eq, &lt).unwrap();

    let ts_filtered = arrow::compute::filter(ts_array, &predicate).unwrap();
    if ts_filtered.is_empty() {
        return None;
    }

    let num_rows_filtered = ts_filtered.len();
    let i64array = ts_filtered
        .as_any()
        .downcast_ref::<TimestampMillisecondArray>()
        .unwrap();
    let max = arrow::compute::max(i64array).unwrap();
    let min = arrow::compute::min(i64array).unwrap();

    let batch = arrow::compute::filter_record_batch(&part.batch, &predicate).unwrap();
    Some(BulkPart {
        batch,
        num_rows: num_rows_filtered,
        max_ts: max,
        min_ts: min,
        sequence: 0,
        timestamp_index: part.timestamp_index,
    })
}

fn filter_batch_by_time_range(c: &mut Criterion) {
    let num = std::env::var("BENCH_FILTER_BATCH_TIME_RANGE_NUM")
        .ok()
        .and_then(|v| usize::from_str(&v).ok())
        .unwrap_or(10000);
    let part = random_array(num);
    let min = 0;
    let max = 10000;

    let mut group = c.benchmark_group("filter_by_time_range");
    group.bench_function("arrow_impl", |b| {
        b.iter(|| {
            let _ = black_box(filter_arrow_impl(&part, min, max));
        });
    });

    group.bench_function("manual_impl", |b| {
        b.iter(|| {
            let _ = black_box(filter_record_batch(&part, min, max));
        });
    });

    group.finish();
}

criterion_group!(benches, filter_batch_by_time_range);
criterion_main!(benches);
