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

use std::hint::black_box;

use bytes::Bytes;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use index::bloom_filter::reader::bytes_to_u64_vec;
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;

/// Generate test data that is guaranteed to be aligned to 8-byte boundary
fn generate_aligned_data(size: usize) -> Bytes {
    let mut rng = ChaCha8Rng::seed_from_u64(42);
    let u64_count = size / 8; // Number of u64 values

    // Generate random u64 values directly - this guarantees alignment
    let mut u64_data: Vec<u64> = Vec::with_capacity(u64_count);
    for _ in 0..u64_count {
        u64_data.push(rng.random::<u64>());
    }

    // Transmute Vec<u64> to Vec<u8> while preserving alignment
    let byte_vec = unsafe {
        let ptr = u64_data.as_mut_ptr() as *mut u8;
        let len = u64_data.len() * std::mem::size_of::<u64>();
        let cap = u64_data.capacity() * std::mem::size_of::<u64>();
        std::mem::forget(u64_data); // Prevent dropping the original Vec
        Vec::from_raw_parts(ptr, len, cap)
    };

    Bytes::from(byte_vec)
}

/// Generate test data that is guaranteed to be unaligned
fn generate_unaligned_data(size: usize) -> Bytes {
    let mut rng = ChaCha8Rng::seed_from_u64(42);
    let u64_count = size / 8; // Number of u64 values

    // Generate random u64 values - start with aligned data
    let mut u64_data: Vec<u64> = Vec::with_capacity(u64_count);
    for _ in 0..u64_count {
        u64_data.push(rng.random::<u64>());
    }

    // Transmute Vec<u64> to Vec<u8>
    let byte_vec = unsafe {
        let ptr = u64_data.as_mut_ptr() as *mut u8;
        let len = u64_data.len() * std::mem::size_of::<u64>();
        let cap = u64_data.capacity() * std::mem::size_of::<u64>();
        std::mem::forget(u64_data); // Prevent dropping the original Vec
        Vec::from_raw_parts(ptr, len, cap)
    };

    let unaligned_bytes = Bytes::from(byte_vec);
    unaligned_bytes.slice(1..)
}

fn benchmark_convert(c: &mut Criterion) {
    let sizes = vec![1024, 16384, 262144, 1048576]; // 1KB to 1MB

    let mut group = c.benchmark_group("bytes_to_u64_vec");

    for size in sizes {
        let data = generate_aligned_data(size);
        group.throughput(Throughput::Bytes(data.len() as u64));
        group.bench_with_input(BenchmarkId::new("aligned", size), &data, |b, data| {
            b.iter(|| {
                let result = bytes_to_u64_vec(black_box(data));
                black_box(result);
            });
        });

        let data = generate_unaligned_data(size);
        group.throughput(Throughput::Bytes(data.len() as u64));
        group.bench_with_input(BenchmarkId::new("unaligned", size), &data, |b, data| {
            b.iter(|| {
                let result = bytes_to_u64_vec(black_box(data));
                black_box(result);
            });
        });
    }

    group.finish();
}

criterion_group!(benches, benchmark_convert);
criterion_main!(benches);
