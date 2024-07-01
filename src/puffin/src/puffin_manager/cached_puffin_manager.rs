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

mod dir_meta;
mod reader;
mod writer;

use async_trait::async_trait;
use futures::{AsyncRead, AsyncSeek, AsyncWrite};
pub use reader::CachedPuffinReader;
pub use writer::CachedPuffinWriter;

use super::{BlobGuard, DirGuard};
use crate::error::Result;
use crate::puffin_manager::cache_manager::CacheManagerRef;
use crate::puffin_manager::file_accessor::PuffinFileAccessorRef;
use crate::puffin_manager::PuffinManager;

/// `CachedPuffinManager` is a `PuffinManager` that provides cached readers and writers for puffin files.
pub struct CachedPuffinManager<B, D, AR, AW> {
    /// The cache manager.
    cache_manager: CacheManagerRef<B, D>,

    /// The puffin file accessor.
    puffin_file_accessor: PuffinFileAccessorRef<AR, AW>,
}

impl<B, D, AR, AW> CachedPuffinManager<B, D, AR, AW> {
    pub fn new(
        cache_manager: CacheManagerRef<B, D>,
        puffin_file_accessor: PuffinFileAccessorRef<AR, AW>,
    ) -> Self {
        Self {
            cache_manager,
            puffin_file_accessor,
        }
    }
}

#[async_trait]
impl<B, D, AR, AW> PuffinManager for CachedPuffinManager<B, D, AR, AW>
where
    B: BlobGuard,
    D: DirGuard,
    AR: AsyncRead + AsyncSeek + Send + Unpin + 'static,
    AW: AsyncWrite + Send + Unpin + 'static,
{
    type Reader = CachedPuffinReader<B, D, AR, AW>;
    type Writer = CachedPuffinWriter<B, D, AW>;

    async fn reader(&self, puffin_file_name: &str) -> Result<Self::Reader> {
        Ok(CachedPuffinReader::new(
            puffin_file_name.to_string(),
            self.cache_manager.clone(),
            self.puffin_file_accessor.clone(),
        ))
    }

    async fn writer(&self, puffin_file_name: &str) -> Result<Self::Writer> {
        let writer = self.puffin_file_accessor.writer(puffin_file_name).await?;
        Ok(CachedPuffinWriter::new(
            puffin_file_name.to_string(),
            self.cache_manager.clone(),
            writer,
        ))
    }
}
