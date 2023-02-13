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

use common_telemetry::debug;

use crate::compaction::scheduler::CompactionRequestImpl;
use crate::compaction::strategy::StrategyRef;
use crate::compaction::task::{CompactionTask, CompactionTaskImpl};

/// Picker picks input SST files and builds the compaction task.
/// Different compaction strategy may implement different pickers.
pub trait Picker<R, T: CompactionTask>: Send + 'static {
    fn pick(&self, ctx: &PickerContext, req: &R) -> crate::error::Result<Option<T>>;
}

pub struct PickerContext {}

/// L0 -> L1 all-to-all compaction based on time windows.
pub(crate) struct SimplePicker {
    strategy: StrategyRef,
}

#[allow(unused)]
impl SimplePicker {
    pub fn new(strategy: StrategyRef) -> Self {
        Self { strategy }
    }
}

impl Picker<CompactionRequestImpl, CompactionTaskImpl> for SimplePicker {
    fn pick(
        &self,
        ctx: &PickerContext,
        req: &CompactionRequestImpl,
    ) -> crate::error::Result<Option<CompactionTaskImpl>> {
        let levels = req.levels();

        for level_num in 0..levels.level_num() {
            let level = levels.level(level_num as u8);
            let outputs = self.strategy.pick(ctx, level);

            if outputs.is_empty() {
                debug!("No SST file can be compacted at level {}", level_num);
                return Ok(None);
            }

            debug!("Found SST files to compact {:?}", outputs);
            // TODO(hl): build compaction task
        }

        Ok(None)
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
        fn pick(
            &self,
            _ctx: &PickerContext,
            _req: &R,
        ) -> crate::error::Result<Option<NoopCompactionTask>> {
            Ok(Some(NoopCompactionTask::new(self.cbs.clone())))
        }
    }
}
