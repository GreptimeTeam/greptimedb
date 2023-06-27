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

use storage::memtable::{IterContext, KeyValues, MemtableRef};

use crate::memtable::util::new_memtable;

pub struct BenchContext {
    memtable: MemtableRef,
}
impl Default for BenchContext {
    fn default() -> Self {
        BenchContext::new()
    }
}
impl BenchContext {
    pub fn new() -> BenchContext {
        BenchContext {
            memtable: new_memtable(),
        }
    }

    pub fn write(&self, kvs: &KeyValues) {
        self.memtable.write(kvs).unwrap();
    }

    pub fn read(&self, batch_size: usize) -> usize {
        let mut read_count = 0;
        let iter_ctx = IterContext {
            batch_size,
            ..Default::default()
        };
        let iter = self.memtable.iter(iter_ctx).unwrap();
        for batch in iter {
            let _ = batch.unwrap();
            read_count += batch_size;
        }
        read_count
    }
}
