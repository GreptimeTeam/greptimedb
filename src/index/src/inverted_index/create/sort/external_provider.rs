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

use async_trait::async_trait;
use futures::{AsyncRead, AsyncWrite};

use crate::inverted_index::error::Result;

/// Trait for managing intermediate files during external sorting for a particular index.
#[mockall::automock]
#[async_trait]
pub trait ExternalTempFileProvider: Send + Sync {
    /// Creates and opens a new intermediate file associated with a specific index for writing.
    /// The implementation should ensure that the file does not already exist.
    ///
    /// - `index_name`: the name of the index for which the file will be associated
    /// - `file_id`: a unique identifier for the new file
    async fn create(
        &self,
        index_name: &str,
        file_id: &str,
    ) -> Result<Box<dyn AsyncWrite + Unpin + Send>>;

    /// Retrieves all intermediate files associated with a specific index for an external sorting operation.
    ///
    /// `index_name`: the name of the index to retrieve intermediate files for
    async fn read_all(&self, index_name: &str) -> Result<Vec<Box<dyn AsyncRead + Unpin + Send>>>;
}
