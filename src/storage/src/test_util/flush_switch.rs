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

use std::sync::atomic::{AtomicBool, Ordering};

use crate::flush::FlushStrategy;
use crate::region::SharedDataRef;

/// Controls whether to flush a region while writing the region.
/// Disable flush by default.
#[derive(Debug, Default)]
pub struct FlushSwitch {
    should_flush: AtomicBool,
}

impl FlushSwitch {
    pub fn set_should_flush(&self, should_flush: bool) {
        self.should_flush.store(should_flush, Ordering::Relaxed);
    }
}

impl FlushStrategy for FlushSwitch {
    fn should_flush(
        &self,
        _shared: &SharedDataRef,
        _bytes_mutable: usize,
        _bytes_total: usize,
    ) -> bool {
        self.should_flush.load(Ordering::Relaxed)
    }
}

pub fn has_parquet_file(sst_dir: &str) -> bool {
    for entry in std::fs::read_dir(sst_dir).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        if !path.is_dir() {
            assert_eq!("parquet", path.extension().unwrap());
            return true;
        }
    }

    false
}
