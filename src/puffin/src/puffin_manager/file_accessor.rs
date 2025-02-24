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
use common_base::range_read::{FileReader, SizeAwareRangeReader};
use common_test_util::temp_dir::{create_temp_dir, TempDir};
use futures::AsyncWrite;
use tokio::fs::File;
use tokio_util::compat::{Compat, TokioAsyncReadCompatExt};

use crate::error::Result;

/// `PuffinFileAccessor` is for opening readers and writers for puffin files.
#[async_trait]
#[auto_impl::auto_impl(Arc)]
pub trait PuffinFileAccessor: Send + Sync + 'static {
    type Reader: SizeAwareRangeReader + Sync;
    type Writer: AsyncWrite + Unpin + Send;
    type FileHandle: ToString + Clone + Send + Sync;

    /// Opens a reader for the given puffin file handle.
    async fn reader(&self, handle: &Self::FileHandle) -> Result<Self::Reader>;

    /// Creates a writer for the given puffin file handle.
    async fn writer(&self, handle: &Self::FileHandle) -> Result<Self::Writer>;
}

pub struct MockFileAccessor {
    tempdir: TempDir,
}

impl MockFileAccessor {
    pub fn new(prefix: &str) -> Self {
        let tempdir = create_temp_dir(prefix);
        Self { tempdir }
    }
}

#[async_trait]
impl PuffinFileAccessor for MockFileAccessor {
    type Reader = FileReader;
    type Writer = Compat<File>;
    type FileHandle = String;

    async fn reader(&self, handle: &String) -> Result<Self::Reader> {
        Ok(FileReader::new(self.tempdir.path().join(handle))
            .await
            .unwrap())
    }

    async fn writer(&self, handle: &String) -> Result<Self::Writer> {
        let p = self.tempdir.path().join(handle);
        if let Some(p) = p.parent() {
            if !tokio::fs::try_exists(p).await.unwrap() {
                tokio::fs::create_dir_all(p).await.unwrap();
            }
        }
        let f = tokio::fs::File::create(p).await.unwrap();
        Ok(f.compat())
    }
}
