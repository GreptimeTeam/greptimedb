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
use std::any::Any;

use store_api::storage::RegionId;

use crate::compaction::{CompactionTask, Picker};
use crate::error::Result;
use crate::scheduler::{Request, Scheduler, Key};

pub struct NoopCompactionScheduler {
    _phantom_data: PhantomData<dyn Request>,
}

impl Default for NoopCompactionScheduler {
    fn default() -> Self {
        Self {
            _phantom_data: Default::default(),
        }
    }
}

impl Debug for NoopCompactionScheduler {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NoopCompactionScheduler<...>").finish()
    }
}

#[derive(Default, Debug)]
pub struct NoopCompactionRequest;

#[derive(Default, Debug)]
pub struct NoopCompactionPicker;

impl Picker for NoopCompactionPicker {
    type Request = NoopCompactionRequest;
    type Task = NoopCompactionTask;

    fn pick(&self, _req: &Self::Request) -> Result<Option<Self::Task>> {
        Ok(None)
    }
}

#[derive(Debug)]
pub struct NoopCompactionTask;

#[async_trait::async_trait]
impl CompactionTask for NoopCompactionTask {
    async fn run(self) -> Result<()> {
        Ok(())
    }
}

impl Request for NoopCompactionRequest {
    fn as_any(&self) -> Box<dyn Any + '_> {
        Box::new(self)
    }

    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self as _
    }
    
    fn key(&self) -> Key {
        Key::RegionKey(RegionId::from(0))
    }

    fn complete(self, _result: Result<()>) {}
}

#[async_trait::async_trait]
impl Scheduler for NoopCompactionScheduler {

    fn schedule(&self, _request: Box<dyn Request>) -> Result<bool> {
        Ok(true)
    }

    async fn stop(&self, _await_termination: bool) -> Result<()> {
        Ok(())
    }
}
