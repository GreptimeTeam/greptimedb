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

use criterion::{criterion_group, criterion_main, Criterion};
use mito2::memtable::merge_tree::{MergeTreeConfig, MergeTreeMemtable};
use mito2::memtable::Memtable;
use mito2::test_util::memtable_util;

fn write_merge_tree(c: &mut Criterion) {
    let metadata = memtable_util::metadata_with_primary_key(vec![1, 0], true);
    let timestamps = (0..100).collect::<Vec<_>>();

    let memtable = MergeTreeMemtable::new(1, metadata.clone(), None, &MergeTreeConfig::default());

    let _ = c.bench_function("write_merge_tree", |b| {
        let kvs =
            memtable_util::build_key_values(&metadata, "hello".to_string(), 42, &timestamps, 1);
        b.iter(|| {
            memtable.write(&kvs).unwrap();
        });
    });
}

criterion_group!(benches, write_merge_tree);
criterion_main!(benches);
