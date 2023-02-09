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

use crate::compaction::scheduler::CompactionRequestImpl;
use crate::compaction::task::{CompactionTask, CompactionTaskImpl};

/// Picker picks input SST files and build the compaction task.
/// Different compaction strategy may implement different pickers.
pub trait Picker<R, T: CompactionTask>: Send + 'static {
    fn pick(&self, req: &R) -> crate::error::Result<T>;
}

/// L0 -> L1 all-to-all compaction based on time windows.
pub(crate) struct SimplePicker {}

#[allow(unused)]
impl SimplePicker {
    pub fn new() -> Self {
        Self {}
    }
}

impl Picker<CompactionRequestImpl, CompactionTaskImpl> for SimplePicker {
    fn pick(&self, _req: &CompactionRequestImpl) -> crate::error::Result<CompactionTaskImpl> {
        todo!()
    }
}

#[cfg(test)]
pub mod tests {
    use std::marker::PhantomData;

    use super::*;
    use crate::compaction::scheduler::CompactionRequest;
    use crate::compaction::task::tests::{CallbackRef, NoopCompactionTask};

    pub(crate) struct MockPicker<R: CompactionRequest> {
        pub cbs: Vec<CallbackRef>,
        _phantom_data: PhantomData<R>,
    }

    impl<R: CompactionRequest> MockPicker<R> {
        pub fn new(cbs: Vec<CallbackRef>) -> Self {
            Self {
                cbs,
                _phantom_data: Default::default(),
            }
        }
    }

    impl<R: CompactionRequest> Picker<R, NoopCompactionTask> for MockPicker<R> {
        fn pick(&self, _req: &R) -> crate::error::Result<NoopCompactionTask> {
            Ok(NoopCompactionTask::new(self.cbs.clone()))
        }
    }
}
