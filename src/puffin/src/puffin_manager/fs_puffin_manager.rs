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
pub use reader::FsPuffinReader;
pub use writer::FsPuffinWriter;

use crate::error::Result;
use crate::puffin_manager::PuffinManager;
use crate::puffin_manager::cache::PuffinMetadataCacheRef;
use crate::puffin_manager::file_accessor::PuffinFileAccessor;
use crate::puffin_manager::stager::Stager;

/// `FsPuffinManager` is a `PuffinManager` that provides readers and writers for puffin data in filesystem.
#[derive(Clone)]
pub struct FsPuffinManager<S, F> {
    /// The stager.
    stager: S,
    /// The puffin file accessor.
    puffin_file_accessor: F,
    /// The puffin metadata cache.
    puffin_metadata_cache: Option<PuffinMetadataCacheRef>,
}

impl<S, F> FsPuffinManager<S, F> {
    /// Creates a new `FsPuffinManager` with the specified `stager` and `puffin_file_accessor`,
    /// and optionally with a `puffin_metadata_cache`.
    pub fn new(stager: S, puffin_file_accessor: F) -> Self {
        Self {
            stager,
            puffin_file_accessor,
            puffin_metadata_cache: None,
        }
    }

    /// Sets the puffin metadata cache.
    pub fn with_puffin_metadata_cache(
        mut self,
        puffin_metadata_cache: Option<PuffinMetadataCacheRef>,
    ) -> Self {
        self.puffin_metadata_cache = puffin_metadata_cache;
        self
    }

    pub fn file_accessor(&self) -> &F {
        &self.puffin_file_accessor
    }
}

#[async_trait]
impl<S, F> PuffinManager for FsPuffinManager<S, F>
where
    F: PuffinFileAccessor + Clone,
    S: Stager<FileHandle = F::FileHandle> + Clone + 'static,
{
    type Reader = FsPuffinReader<S, F>;
    type Writer = FsPuffinWriter<S, F::Writer>;
    type FileHandle = F::FileHandle;

    async fn reader(&self, handle: &Self::FileHandle) -> Result<Self::Reader> {
        Ok(FsPuffinReader::new(
            handle.clone(),
            self.stager.clone(),
            self.puffin_file_accessor.clone(),
            self.puffin_metadata_cache.clone(),
        ))
    }

    async fn writer(&self, handle: &Self::FileHandle) -> Result<Self::Writer> {
        let writer = self.puffin_file_accessor.writer(handle).await?;
        Ok(FsPuffinWriter::new(
            handle.clone(),
            self.stager.clone(),
            writer,
        ))
    }
}
