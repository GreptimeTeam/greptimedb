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

mod intersection_apply;
mod keys_apply;

pub use intersection_apply::IntersectionFstApplier;
pub use keys_apply::KeysFstApplier;

use crate::inverted_index::FstMap;

/// A trait for objects that can process a finite state transducer (FstMap) and return
/// associated values.
#[mockall::automock]
pub trait FstApplier: Send + Sync {
    /// Retrieves values from an FstMap.
    ///
    /// * `fst`: A reference to the FstMap from which the values will be fetched.
    ///
    /// Returns a `Vec<u64>`, with each u64 being a value from the FstMap.
    fn apply(&self, fst: &FstMap) -> Vec<u64>;

    /// Returns the memory usage of the applier.
    fn memory_usage(&self) -> usize;
}
