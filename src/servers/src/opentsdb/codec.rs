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

#[derive(Debug, Clone)]
pub struct DataPoint {
    metric: String,
    ts_millis: i64,
    value: f64,
    tags: Vec<(String, String)>,
}

impl DataPoint {
    pub fn new(metric: String, ts_millis: i64, value: f64, tags: Vec<(String, String)>) -> Self {
        Self {
            metric,
            ts_millis,
            value,
            tags,
        }
    }

    pub fn metric(&self) -> &str {
        &self.metric
    }

    pub fn tags(&self) -> &Vec<(String, String)> {
        &self.tags
    }

    pub fn tags_mut(&mut self) -> &mut Vec<(String, String)> {
        &mut self.tags
    }

    pub fn ts_millis(&self) -> i64 {
        self.ts_millis
    }

    pub fn value(&self) -> f64 {
        self.value
    }

    pub fn timestamp_to_millis(t: i64) -> i64 {
        // 9999999999999 (13 digits) is of date "Sat Nov 20 2286 17:46:39 UTC",
        // 999999999999 (12 digits) is "Sun Sep 09 2001 01:46:39 UTC",
        // so timestamp digits less than 13 means we got seconds here.
        // (We are not expecting to store data that is 21 years ago, are we?)
        if t.abs().to_string().len() < 13 {
            t * 1000
        } else {
            t
        }
    }
}
