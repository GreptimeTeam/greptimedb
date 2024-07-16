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

mod tantivy;

use async_trait::async_trait;
pub use tantivy::{TantivyFulltextIndexCreator, TEXT_FIELD_NAME};

use crate::fulltext_index::error::Result;

/// `FulltextIndexCreator` is for creating a fulltext index.
#[async_trait]
pub trait FulltextIndexCreator: Send {
    /// Pushes a text to the index.
    async fn push_text(&mut self, text: &str) -> Result<()>;

    /// Finalizes the creation of the index.
    async fn finish(&mut self) -> Result<()>;

    /// Returns the memory usage in bytes during the creation of the index.
    fn memory_usage(&self) -> usize;
}
