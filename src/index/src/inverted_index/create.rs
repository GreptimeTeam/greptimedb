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

mod sort;
mod sort_create;

use async_trait::async_trait;

use crate::inverted_index::error::Result;
use crate::inverted_index::Bytes;

/// `IndexCreator` provides functionality to construct and finalize an index
#[async_trait]
pub trait IndexCreator {
    /// Adds a value to the named index. A `None` value represents an absence of data (null)
    ///
    /// - `index_name`: Identifier for the index being built
    /// - `value`: The data to be indexed, or `None` for a null entry
    ///
    /// Note: Call this method for each row in the dataset
    async fn push_with_name(&mut self, index_name: &str, value: Option<Bytes>) -> Result<()>;

    /// Finalizes the index creation process, ensuring all data is properly indexed and stored
    async fn finish(&mut self) -> Result<()>;
}
