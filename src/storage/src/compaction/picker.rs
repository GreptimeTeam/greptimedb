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

use crate::compaction::scheduler::CompactionRequest;
use crate::compaction::task::{CompactionTask, CompactionTaskImpl};

/// Picker picks input SST files and build the compaction task.
/// Different compaction strategy may implement different pickers.
pub(crate) trait Picker<R, T: CompactionTask>: 'static {
    fn pick(&self, req: &R) -> crate::error::Result<T>;
}

/// L0 -> L1 all-to-all compaction based on time windows.
pub(crate) struct SimplePicker {}

impl SimplePicker {
    pub fn new() -> Self {
        Self {}
    }
}

impl Picker<CompactionRequest, CompactionTaskImpl> for SimplePicker {
    fn pick(&self, _req: &CompactionRequest) -> crate::error::Result<CompactionTaskImpl> {
        todo!()
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::compaction::task::tests::{CallbackRef, NoopCompactionTask};

    pub(crate) struct MockPicker {
        pub cbs: Vec<CallbackRef>,
    }

    impl Picker<CompactionRequest, NoopCompactionTask> for MockPicker {
        fn pick(&self, _req: &CompactionRequest) -> crate::error::Result<NoopCompactionTask> {
            Ok(NoopCompactionTask::new(self.cbs.clone()))
        }
    }
}
