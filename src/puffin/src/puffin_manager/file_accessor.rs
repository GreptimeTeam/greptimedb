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
use futures::{AsyncRead, AsyncSeek, AsyncWrite};

use crate::error::Result;

/// `PuffinFileAccessor` is for opening readers and writers for puffin files.
#[async_trait]
#[auto_impl::auto_impl(Arc)]
pub trait PuffinFileAccessor: Send + Sync + 'static {
    type Reader: AsyncRead + AsyncSeek + Unpin + Send + Sync;
    type Writer: AsyncWrite + Unpin + Send;

    /// Opens a reader for the given puffin file.
    async fn reader(&self, puffin_file_name: &str) -> Result<Self::Reader>;

    /// Creates a writer for the given puffin file.
    async fn writer(&self, puffin_file_name: &str) -> Result<Self::Writer>;
}
