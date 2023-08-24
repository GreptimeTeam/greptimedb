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

//! Flush related utilities and structs.

use std::sync::Arc;

/// Global write buffer (memtable) manager.
///
/// Tracks write buffer (memtable) usages and decide whether the engine needs to flush.
pub trait WriteBufferManager: Send + Sync + std::fmt::Debug {
    /// Returns whether to trigger the engine.
    fn should_flush_engine(&self) -> bool;

    /// Reserves `mem` bytes.
    fn reserve_mem(&self, mem: usize);

    /// Tells the manager we are freeing `mem` bytes.
    ///
    /// We are in the process of freeing `mem` bytes, so it is not considered
    /// when checking the soft limit.
    fn schedule_free_mem(&self, mem: usize);

    /// We have freed `mem` bytes.
    fn free_mem(&self, mem: usize);
}

pub type WriteBufferManagerRef = Arc<dyn WriteBufferManager>;

#[derive(Debug)]
pub struct WriteBufferManagerImpl {}

impl WriteBufferManager for WriteBufferManagerImpl {
    fn should_flush_engine(&self) -> bool {
        todo!()
    }

    fn reserve_mem(&self, _mem: usize) {
        todo!()
    }

    fn schedule_free_mem(&self, _mem: usize) {
        todo!()
    }

    fn free_mem(&self, _mem: usize) {
        todo!()
    }
}
