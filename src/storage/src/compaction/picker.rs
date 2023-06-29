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

use common_telemetry::{debug, error, info};
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

    fn pick(&self, req: &Self::Request) -> crate::error::Result<Option<Self::Task>>;

    fn get_expired_ssts(
        &self,
        levels: &LevelMetasRef,
        ttl: Option<Duration>,
    ) -> crate::error::Result<Vec<FileHandle>> {
        let Some(ttl) = ttl else { return Ok(vec![]); };

        let expire_time = Timestamp::current_millis()
            .sub_duration(ttl)
            .context(TtlCalculationSnafu)?;

        let mut expired_ssts = vec![];
        for level in 0..levels.level_num() {
            expired_ssts.extend(levels.level(level as Level).get_expired_files(&expire_time));
        }
        Ok(expired_ssts)
    }
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

impl<S: LogStore> Picker for SimplePicker<S> {
    type Request = CompactionRequestImpl<S>;
    type Task = CompactionTaskImpl<S>;

    fn pick(
        &self,
        req: &CompactionRequestImpl<S>,
    ) -> crate::error::Result<Option<CompactionTaskImpl<S>>> {
        let levels = &req.levels();
        let expired_ssts = self
            .get_expired_ssts(levels, req.ttl)
            .map_err(|e| {
                error!(e;"Failed to get region expired SST files, region: {}, ttl: {:?}", req.region_id, req.ttl);
                e
            })
            .unwrap_or_default();

        if !expired_ssts.is_empty() {
            info!(
                "Expired SSTs in region {}: {:?}",
                req.region_id, expired_ssts
            );
            // here we mark expired SSTs as compacting to avoid them being picked.
            expired_ssts.iter().for_each(|f| f.mark_compacting(true));
        }

        let ctx = &PickerContext::with(req.compaction_time_window);

        for level_num in 0..levels.level_num() {
            let level = levels.level(level_num as u8);
            let (compaction_time_window, outputs) = self.strategy.pick(ctx, level);

            if outputs.is_empty() {
                debug!("No SST file can be compacted at level {}", level_num);
                continue;
            }

            debug!(
                "Found SST files to compact {:?} on level: {}, compaction window: {:?}",
                outputs, level_num, compaction_time_window,
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
                sst_write_buffer_size: req.sst_write_buffer_size,
                compaction_time_window,
            }));
        }

        Ok(None)
    }
}
