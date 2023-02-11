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

use std::marker::PhantomData;
use std::sync::Arc;

use common_telemetry::debug;
use store_api::logstore::LogStore;

use crate::compaction::scheduler::CompactionRequestImpl;
use crate::compaction::strategy::{SimpleTimeWindowStrategy, StrategyRef};
use crate::compaction::task::{CompactionTask, CompactionTaskImpl};

/// Picker picks input SST files and builds the compaction task.
/// Different compaction strategy may implement different pickers.
pub trait Picker<R, T: CompactionTask>: Send + 'static {
    fn pick(&self, ctx: &PickerContext, req: &R) -> crate::error::Result<Option<T>>;
}

pub struct PickerContext {}

/// L0 -> L1 compaction based on time windows.
pub struct SimplePicker<S> {
    strategy: StrategyRef,
    _phantom_data: PhantomData<S>,
}

impl<S> Default for SimplePicker<S> {
    fn default() -> Self {
        Self::new(Arc::new(SimpleTimeWindowStrategy {}))
    }
}

impl<S> SimplePicker<S> {
    pub fn new(strategy: StrategyRef) -> Self {
        Self {
            strategy,
            _phantom_data: Default::default(),
        }
    }
}

impl<S: LogStore> Picker<CompactionRequestImpl<S>, CompactionTaskImpl<S>> for SimplePicker<S> {
    fn pick(
        &self,
        ctx: &PickerContext,
        req: &CompactionRequestImpl<S>,
    ) -> crate::error::Result<Option<CompactionTaskImpl<S>>> {
        let levels = &req.levels;

        for level_num in 0..levels.level_num() {
            let level = levels.level(level_num as u8);
            let outputs = self.strategy.pick(ctx, level);

            if outputs.is_empty() {
                debug!("No SST file can be compacted at level {}", level_num);
                continue;
            }

            debug!(
                "Found SST files to compact {:?} on level: {}",
                outputs, level_num
            );
            return Ok(Some(CompactionTaskImpl {
                schema: req.schema(),
                sst_layer: req.sst_layer.clone(),
                outputs,
                writer: req.writer.clone(),
                shared_data: req.shared.clone(),
                wal: req.wal.clone(),
                manifest: req.manifest.clone(),
            }));
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
