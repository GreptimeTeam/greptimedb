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

use chrono::{FixedOffset, Offset};

#[derive(Debug, Clone)]
pub struct TimeZone {
    offset_secs: i32,
}

impl TimeZone {
    pub fn new(offset_hours: i32, offset_mins: u32) -> Self {
        let offset_secs = if offset_hours > 0 {
            offset_hours * 3600 + offset_mins as i32 * 60
        } else {
            offset_hours * 3600 - offset_mins as i32 * 60
        };
        Self { offset_secs }
    }
}

impl Offset for TimeZone {
    fn fix(&self) -> FixedOffset {
        FixedOffset::east_opt(self.offset_secs).unwrap()
    }
}
