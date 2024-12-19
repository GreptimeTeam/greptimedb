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

use crate::error::Error;

pub type Writer = Box<dyn AsyncWrite + Unpin + Send>;
pub type Reader = Box<dyn AsyncRead + Unpin + Send>;

/// Trait for managing intermediate files to control memory usage for a particular index.
#[mockall::automock]
#[async_trait]
pub trait ExternalTempFileProvider: Send + Sync {
    /// Creates and opens a new intermediate file associated with a specific `file_group` for writing.
    /// The implementation should ensure that the file does not already exist.
    ///
    /// - `file_group`: a unique identifier for the group of files
    /// - `file_id`: a unique identifier for the new file
    async fn create(&self, file_group: &str, file_id: &str) -> Result<Writer, Error>;

    /// Retrieves all intermediate files and their associated file identifiers for a specific `file_group`.
    ///
    /// `file_group` is a unique identifier for the group of files.
    async fn read_all(&self, file_group: &str) -> Result<Vec<(String, Reader)>, Error>;
}
