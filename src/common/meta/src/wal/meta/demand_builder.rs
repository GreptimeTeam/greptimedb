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

use crate::wal::meta::WalMetaDemand;

#[derive(Default)]
pub struct WalMetaDemandBuilder {
    num_topics: Option<usize>,
}

impl WalMetaDemandBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_num_topics(&mut self, num_topics: usize) -> &mut Self {
        self.num_topics = Some(num_topics);
        self
    }

    pub fn build(&self) -> WalMetaDemand {
        WalMetaDemand {
            num_topics: self.num_topics,
        }
    }
}
