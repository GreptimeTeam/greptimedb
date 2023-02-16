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

use std::fmt::{Debug, Formatter};
use std::marker::PhantomData;

use store_api::storage::RegionId;

use crate::compaction::{CompactionScheduler, CompactionTask, Picker, PickerContext, Request};

pub struct NoopCompactionScheduler<R> {
    _phantom_data: PhantomData<R>,
}

impl<R> Default for NoopCompactionScheduler<R> {
    fn default() -> Self {
        Self {
            _phantom_data: Default::default(),
        }
    }
}

impl<R> Debug for NoopCompactionScheduler<R> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NoopCompactionScheduler<...>").finish()
    }
}

#[derive(Default, Debug)]
pub struct NoopCompactionRequest;

#[derive(Default, Debug)]
pub struct NoopCompactionPicker;

impl<R, T: CompactionTask> Picker<R, T> for NoopCompactionPicker {
    fn pick(&self, _ctx: &PickerContext, _req: &R) -> crate::error::Result<Option<T>> {
        Ok(None)
    }
}

#[derive(Debug)]
pub struct NoopCompactionTask;

#[async_trait::async_trait]
impl CompactionTask for NoopCompactionTask {
    async fn run(self) -> crate::error::Result<()> {
        Ok(())
    }
}

impl Request<RegionId> for NoopCompactionRequest {
    fn key(&self) -> RegionId {
        0
    }
}

#[async_trait::async_trait]
impl<R: Request<RegionId>> CompactionScheduler<R> for NoopCompactionScheduler<R> {
    async fn schedule(&self, _request: R) -> crate::error::Result<bool> {
        Ok(true)
    }

    async fn stop(&self) -> crate::error::Result<()> {
        Ok(())
    }
}
