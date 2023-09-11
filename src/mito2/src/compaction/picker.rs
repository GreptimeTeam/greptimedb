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

use std::fmt::Debug;
use std::sync::Arc;

use crate::compaction::CompactionRequest;

pub type CompactionPickerRef = Arc<dyn Picker + Send + Sync>;

#[async_trait::async_trait]
pub trait CompactionTask: Debug + Send + Sync + 'static {
    async fn run(&mut self);
}

/// Picker picks input SST files and builds the compaction task.
/// Different compaction strategy may implement different pickers.
pub trait Picker: Debug + Send + 'static {
    fn pick(&self, req: CompactionRequest) -> Option<Box<dyn CompactionTask>>;
}

pub struct PickerContext {
    compaction_time_window: Option<i64>,
}

impl PickerContext {
    pub fn with(compaction_time_window: Option<i64>) -> Self {
        Self {
            compaction_time_window,
        }
    }

    pub fn compaction_time_window(&self) -> Option<i64> {
        self.compaction_time_window
    }
}
