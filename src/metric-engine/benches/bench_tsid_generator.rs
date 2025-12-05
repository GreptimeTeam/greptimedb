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

use std::hash::Hasher;

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use fxhash::FxHasher;
use mur3::Hasher128;

// A random number (from original implementation)
const TSID_HASH_SEED: u32 = 846793005;

/// Original TSID generator using mur3::Hasher128
/// Hashes both label name and value for each label pair
struct OriginalTsidGenerator {
    hasher: Hasher128,
}

impl OriginalTsidGenerator {
    fn new() -> Self {
        Self {
            hasher: Hasher128::with_seed(TSID_HASH_SEED),
        }
    }

    /// Writes a label pair (name and value) to the generator.
    fn write_label(&mut self, name: &str, value: &str) {
        use std::hash::Hash;
        name.hash(&mut self.hasher);
        value.hash(&mut self.hasher);
    }

    /// Generates a new TSID.
    fn finish(&mut self) -> u64 {
        // TSID is 64 bits, simply truncate the 128 bits hash
        let (hash, _) = self.hasher.finish128();
        hash
    }
}

/// Current TSID generator using fxhash::FxHasher
/// Fast path: pre-computes label name hash, only hashes values
struct CurrentTsidGenerator {
    hasher: FxHasher,
}

impl CurrentTsidGenerator {
    fn new() -> Self {
        Self {
            hasher: FxHasher::default(),
        }
    }

    fn new_with_label_name_hash(label_name_hash: u64) -> Self {
        let mut hasher = FxHasher::default();
        hasher.write_u64(label_name_hash);
        Self { hasher }
    }

    /// Writes a label value to the generator.
    fn write_str(&mut self, value: &str) {
        self.hasher.write(value.as_bytes());
        self.hasher.write_u8(0xff);
    }

    /// Generates a new TSID.
    fn finish(&mut self) -> u64 {
        self.hasher.finish()
    }
}

/// Pre-computes label name hash (used in fast path)
fn compute_label_name_hash(labels: &[(&str, &str)]) -> u64 {
    let mut hasher = FxHasher::default();
    for (name, _) in labels {
        hasher.write(name.as_bytes());
        hasher.write_u8(0xff);
    }
    hasher.finish()
}

fn bench_tsid_generator_small(c: &mut Criterion) {
    let labels = vec![("namespace", "greptimedb"), ("host", "127.0.0.1")];

    let mut group = c.benchmark_group("tsid_generator_small_2_labels");
    group.bench_function("original_mur3", |b| {
        b.iter(|| {
            let mut tsid_gen = OriginalTsidGenerator::new();
            for (name, value) in &labels {
                tsid_gen.write_label(black_box(name), black_box(value));
            }
            black_box(tsid_gen.finish())
        })
    });

    let label_name_hash = compute_label_name_hash(&labels);
    group.bench_function("current_fxhash_fast_path", |b| {
        b.iter(|| {
            let mut tsid_gen =
                CurrentTsidGenerator::new_with_label_name_hash(black_box(label_name_hash));
            for (_, value) in &labels {
                tsid_gen.write_str(black_box(value));
            }
            black_box(tsid_gen.finish())
        })
    });

    group.finish();
}

fn bench_tsid_generator_medium(c: &mut Criterion) {
    let labels = vec![
        ("namespace", "greptimedb"),
        ("host", "127.0.0.1"),
        ("region", "us-west-2"),
        ("env", "production"),
        ("service", "api"),
    ];

    let mut group = c.benchmark_group("tsid_generator_medium_5_labels");
    group.bench_function("original_mur3", |b| {
        b.iter(|| {
            let mut tsid_gen = OriginalTsidGenerator::new();
            for (name, value) in &labels {
                tsid_gen.write_label(black_box(name), black_box(value));
            }
            black_box(tsid_gen.finish())
        })
    });

    let label_name_hash = compute_label_name_hash(&labels);
    group.bench_function("current_fxhash_fast_path", |b| {
        b.iter(|| {
            let mut tsid_gen =
                CurrentTsidGenerator::new_with_label_name_hash(black_box(label_name_hash));
            for (_, value) in &labels {
                tsid_gen.write_str(black_box(value));
            }
            black_box(tsid_gen.finish())
        })
    });

    group.finish();
}

fn bench_tsid_generator_large(c: &mut Criterion) {
    let labels = vec![
        ("namespace", "greptimedb"),
        ("host", "127.0.0.1"),
        ("region", "us-west-2"),
        ("env", "production"),
        ("service", "api"),
        ("version", "v1.0.0"),
        ("cluster", "cluster-1"),
        ("dc", "dc1"),
        ("rack", "rack-1"),
        ("pod", "pod-123"),
    ];

    let mut group = c.benchmark_group("tsid_generator_large_10_labels");
    group.bench_function("original_mur3", |b| {
        b.iter(|| {
            let mut tsid_gen = OriginalTsidGenerator::new();
            for (name, value) in &labels {
                tsid_gen.write_label(black_box(name), black_box(value));
            }
            black_box(tsid_gen.finish())
        })
    });

    let label_name_hash = compute_label_name_hash(&labels);
    group.bench_function("current_fxhash_fast_path", |b| {
        b.iter(|| {
            let mut tsid_gen =
                CurrentTsidGenerator::new_with_label_name_hash(black_box(label_name_hash));
            for (_, value) in &labels {
                tsid_gen.write_str(black_box(value));
            }
            black_box(tsid_gen.finish())
        })
    });

    group.finish();
}

fn bench_tsid_generator_slow_path(c: &mut Criterion) {
    // Simulate slow path: some labels have null values (empty strings)
    let labels_with_nulls = vec![
        ("namespace", "greptimedb"),
        ("host", "127.0.0.1"),
        ("region", ""), // null
        ("env", "production"),
    ];

    let labels_all_non_null = vec![
        ("namespace", "greptimedb"),
        ("host", "127.0.0.1"),
        ("env", "production"),
    ];

    let mut group = c.benchmark_group("tsid_generator_slow_path_with_nulls");

    // Original: always hashes name and value
    group.bench_function("original_mur3_with_nulls", |b| {
        b.iter(|| {
            let mut tsid_gen = OriginalTsidGenerator::new();
            for (name, value) in &labels_with_nulls {
                if !value.is_empty() {
                    tsid_gen.write_label(black_box(name), black_box(value));
                }
            }
            black_box(tsid_gen.finish())
        })
    });

    // Current slow path: recomputes label name hash
    group.bench_function("current_fxhash_slow_path", |b| {
        b.iter(|| {
            // Step 1: Compute label name hash for non-null labels
            let mut name_hasher = CurrentTsidGenerator::new();
            for (name, value) in &labels_with_nulls {
                if !value.is_empty() {
                    name_hasher.write_str(black_box(name));
                }
            }
            let label_name_hash = name_hasher.finish();

            // Step 2: Use label name hash and hash values
            let mut tsid_gen = CurrentTsidGenerator::new_with_label_name_hash(label_name_hash);
            for (_, value) in &labels_with_nulls {
                if !value.is_empty() {
                    tsid_gen.write_str(black_box(value));
                }
            }
            black_box(tsid_gen.finish())
        })
    });

    // Current fast path: pre-computed (for comparison)
    let label_name_hash = compute_label_name_hash(&labels_all_non_null);
    group.bench_function("current_fxhash_fast_path_no_nulls", |b| {
        b.iter(|| {
            let mut tsid_gen =
                CurrentTsidGenerator::new_with_label_name_hash(black_box(label_name_hash));
            for (_, value) in &labels_all_non_null {
                tsid_gen.write_str(black_box(value));
            }
            black_box(tsid_gen.finish())
        })
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_tsid_generator_small,
    bench_tsid_generator_medium,
    bench_tsid_generator_large,
    bench_tsid_generator_slow_path
);
criterion_main!(benches);
