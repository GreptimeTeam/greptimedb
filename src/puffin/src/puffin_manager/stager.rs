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

mod bounded_stager;

use std::path::PathBuf;

use async_trait::async_trait;
pub use bounded_stager::{BoundedStager, FsBlobGuard, FsDirGuard};
use futures::future::BoxFuture;
use futures::AsyncWrite;

use crate::error::Result;
use crate::puffin_manager::{BlobGuard, DirGuard};

pub type BoxWriter = Box<dyn AsyncWrite + Unpin + Send>;

/// Result containing the number of bytes written (u64).
pub type WriteResult = BoxFuture<'static, Result<u64>>;

/// `DirWriterProvider` provides a way to write files into a directory.
#[async_trait]
pub trait DirWriterProvider {
    /// Creates a writer for the given relative path.
    async fn writer(&self, relative_path: &str) -> Result<BoxWriter>;
}

pub type DirWriterProviderRef = Box<dyn DirWriterProvider + Send>;

/// Function that initializes a blob.
///
/// `Stager` will provide a `BoxWriter` that the caller of `get_blob`
/// can use to write the blob into the staging area.
pub trait InitBlobFn = FnOnce(BoxWriter) -> WriteResult;

/// Function that initializes a directory.
///
/// `Stager` will provide a `DirWriterProvider` that the caller of `get_dir`
/// can use to write files inside the directory into the staging area.
pub trait InitDirFn = FnOnce(DirWriterProviderRef) -> WriteResult;

/// `Stager` manages the staging area for the puffin files.
#[async_trait]
#[auto_impl::auto_impl(Arc)]
pub trait Stager: Send + Sync {
    type Blob: BlobGuard + Sync;
    type Dir: DirGuard;

    /// Retrieves a blob, initializing it if necessary using the provided `init_fn`.
    ///
    /// The returned `BlobGuard` is used to access the blob reader.
    /// The caller is responsible for holding the `BlobGuard` until they are done with the blob.
    async fn get_blob<'a>(
        &self,
        puffin_file_name: &str,
        key: &str,
        init_factory: Box<dyn InitBlobFn + Send + Sync + 'a>,
    ) -> Result<Self::Blob>;

    /// Retrieves a directory, initializing it if necessary using the provided `init_fn`.
    ///
    /// The returned `DirGuard` is used to access the directory in the filesystem.
    /// The caller is responsible for holding the `DirGuard` until they are done with the directory.
    async fn get_dir<'a>(
        &self,
        puffin_file_name: &str,
        key: &str,
        init_fn: Box<dyn InitDirFn + Send + Sync + 'a>,
    ) -> Result<Self::Dir>;

    /// Stores a directory in the staging area.
    async fn put_dir(
        &self,
        puffin_file_name: &str,
        key: &str,
        dir_path: PathBuf,
        dir_size: u64,
    ) -> Result<()>;
}
