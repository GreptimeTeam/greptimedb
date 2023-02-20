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
use std::time::Duration;

use common_telemetry::{debug, error};
use common_time::Timestamp;
use snafu::ResultExt;
use store_api::logstore::LogStore;

use crate::compaction::scheduler::CompactionRequestImpl;
use crate::compaction::strategy::{SimpleTimeWindowStrategy, StrategyRef};
use crate::compaction::task::{CompactionTask, CompactionTaskImpl};
use crate::error::TtlCalculationSnafu;
use crate::scheduler::Request;
use crate::sst::{FileHandle, Level};
use crate::version::LevelMetasRef;

/// Picker picks input SST files and builds the compaction task.
/// Different compaction strategy may implement different pickers.
pub trait Picker: Send + 'static {
    type Request: Request;
    type Task: CompactionTask;

    fn pick(
        &self,
        ctx: &PickerContext,
        req: &Self::Request,
    ) -> crate::error::Result<Option<Self::Task>>;
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

    fn get_expired_ssts(
        &self,
        levels: &LevelMetasRef,
        ttl: Option<Duration>,
    ) -> crate::error::Result<Vec<FileHandle>> {
        let Some(ttl) = ttl else { return Ok(vec![]); };

        let expire_time = Timestamp::current_millis()
            .context(TtlCalculationSnafu)?
            .sub(ttl)
            .context(TtlCalculationSnafu)?;

        let mut expired_ssts = vec![];
        for level in 0..levels.level_num() {
            expired_ssts.extend(levels.level(level as Level).get_expired_files(&expire_time));
        }
        Ok(expired_ssts)
    }
}

impl<S: LogStore> Picker for SimplePicker<S> {
    type Request = CompactionRequestImpl<S>;
    type Task = CompactionTaskImpl<S>;

    fn pick(
        &self,
        ctx: &PickerContext,
        req: &CompactionRequestImpl<S>,
    ) -> crate::error::Result<Option<CompactionTaskImpl<S>>> {
        let levels = &req.levels();
        let expired_ssts = self
            .get_expired_ssts(levels, req.ttl)
            .map_err(|e| {
                error!(e;"Failed to get region expired SST files, ttl: {:?}", req.ttl);
                e
            })
            .unwrap_or(vec![]);

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
                expired_ssts,
            }));
        }

        Ok(None)
    }
}
