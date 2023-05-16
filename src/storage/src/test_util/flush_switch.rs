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

use std::sync::Mutex;

use crate::flush::{FlushStrategy, FlushType, RegionStatus};

/// Controls whether to flush a region while writing the region.
/// Disable flush by default.
#[derive(Debug, Default)]
pub struct FlushSwitch {
    flush_type: Mutex<Option<FlushType>>,
}

impl FlushSwitch {
    pub fn set_should_flush(&self, should_flush: bool) {
        if should_flush {
            *self.flush_type.lock().unwrap() = Some(FlushType::Region);
        } else {
            *self.flush_type.lock().unwrap() = None;
        }
    }

    pub fn set_flush_type(&self, flush_type: FlushType) {
        *self.flush_type.lock().unwrap() = Some(flush_type);
    }
}

impl FlushStrategy for FlushSwitch {
    fn should_flush(&self, _status: RegionStatus) -> Option<FlushType> {
        *self.flush_type.lock().unwrap()
    }

    fn reserve_mem(&self, _mem: usize) {}

    fn schedule_free_mem(&self, _mem: usize) {}

    fn free_mem(&self, _mem: usize) {}
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
