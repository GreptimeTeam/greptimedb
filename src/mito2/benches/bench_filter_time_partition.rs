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

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use datatypes::arrow;
use datatypes::arrow::array::{ArrayRef, BooleanArray, Int64Array};
use datatypes::arrow::buffer::{BooleanBuffer, MutableBuffer};
use datatypes::arrow::util::bit_util::ceil;

fn random_array(num: usize) -> Int64Array {
    let data: Vec<_> = (0..num).map(|_| rand::random::<i64>()).collect();
    Int64Array::from(data)
}

fn filter_arrow_impl(array: &Int64Array, min: i64, max: i64) -> (ArrayRef, i64, i64) {
    let gt_eq = arrow::compute::kernels::cmp::gt_eq(array, &Int64Array::new_scalar(min)).unwrap();
    let lt = arrow::compute::kernels::cmp::lt(array, &Int64Array::new_scalar(max)).unwrap();
    let res = arrow::compute::and(&gt_eq, &lt).unwrap();
    let array = arrow::compute::filter(array, &res).unwrap();
    let i64array = array.as_any().downcast_ref::<Int64Array>().unwrap();
    let max = arrow::compute::max(i64array).unwrap_or(i64::MIN);
    let min = arrow::compute::min(i64array).unwrap_or(i64::MAX);
    (array, min, max)
}

fn filter_manual_impl(array: &Int64Array, min: i64, max: i64) -> (ArrayRef, i64, i64) {
    let len = array.len();
    let mut buffer = MutableBuffer::new(ceil(len, 64) * 8);

    let f = |idx: usize| -> bool {
        unsafe {
            let val = array.value_unchecked(idx);
            val >= min && val < max
        }
    };

    let chunks = len / 64;
    let remainder = len % 64;
    for chunk in 0..chunks {
        let mut packed = 0;
        for bit_idx in 0..64 {
            let i = bit_idx + chunk * 64;
            packed |= (f(i) as u64) << bit_idx;
        }
        // SAFETY: Already allocated sufficient capacity
        unsafe { buffer.push_unchecked(packed) }
    }

    if remainder != 0 {
        let mut packed = 0;
        for bit_idx in 0..remainder {
            let i = bit_idx + chunks * 64;
            packed |= (f(i) as u64) << bit_idx;
        }

        // SAFETY: Already allocated sufficient capacity
        unsafe { buffer.push_unchecked(packed) }
    }
    let res = arrow::compute::filter(
        array,
        &BooleanArray::new(BooleanBuffer::new(buffer.into(), 0, len), None),
    )
    .unwrap();
    let i64array = res.as_any().downcast_ref::<Int64Array>().unwrap();
    let max = arrow::compute::max(i64array).unwrap_or(i64::MIN);
    let min = arrow::compute::min(i64array).unwrap_or(i64::MAX);
    (res, min, max)
}

fn filter_batch_by_time_range(c: &mut Criterion) {
    let num = std::env::var("BENCH_FILTER_BATCH_TIME_RANGE_NUM")
        .ok()
        .and_then(|v| usize::from_str(&v).ok())
        .unwrap_or(10000);
    let array = random_array(num);
    let min = 0;
    let max = 10000;

    let mut group = c.benchmark_group("filter_by_time_range");
    group.bench_function("arrow_impl", |b| {
        b.iter(|| {
            let _ = black_box(filter_arrow_impl(&array, min, max));
        });
    });

    group.bench_function("manual_impl", |b| {
        b.iter(|| {
            let _ = black_box(filter_manual_impl(&array, min, max));
        });
    });

    group.finish();
}

criterion_group!(benches, filter_batch_by_time_range);
criterion_main!(benches);
