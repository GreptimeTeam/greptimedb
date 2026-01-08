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

use std::ops::Range;
use std::sync::Arc;

use crate::error::Result;

pub type ResourceIdAllocatorRef = Arc<dyn ResourceIdAllocator>;

#[async_trait::async_trait]
pub trait ResourceIdAllocator: Send + Sync {
    /// Returns the next value and increments the sequence.
    async fn next(&self) -> Result<u64>;

    /// Returns the current value stored in the remote storage without incrementing the sequence.
    async fn peek(&self) -> Result<u64>;

    /// Jumps to the given value.
    async fn jump_to(&self, next: u64) -> Result<()>;

    /// Returns the range of available sequences.
    async fn min_max(&self) -> Range<u64>;
}
