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

//! Write ahead log of the engine.

use std::sync::Arc;

/// Write ahead log.
///
/// All regions in the engine shares the same WAL instance.
#[derive(Debug)]
pub struct Wal<S> {
    /// The underlying log store.
    store: Arc<S>,
}

impl<S> Wal<S> {
    /// Creates a new [Wal] from the log store.
    pub fn new(store: Arc<S>) -> Self {
        Self { store }
    }
}
