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

use std::result::Result as StdResult;

use bytes::Bytes;
use futures::FutureExt;
use futures::future::BoxFuture;
use object_store::ObjectStore;
use parquet::arrow::async_reader::MetadataFetch;
use parquet::errors::{ParquetError, Result as ParquetResult};
use parquet::file::metadata::{ParquetMetaData, ParquetMetaDataReader};
use snafu::{IntoError as _, ResultExt};

use crate::error::{self, Result};

/// The estimated size of the footer and metadata need to read from the end of parquet file.
const DEFAULT_PREFETCH_SIZE: u64 = 64 * 1024;

pub(crate) struct MetadataLoader<'a> {
    // An object store that supports async read
    object_store: ObjectStore,
    // The path of parquet file
    file_path: &'a str,
    // The size of parquet file
    file_size: u64,
}

impl<'a> MetadataLoader<'a> {
    /// Create a new parquet metadata loader.
    pub fn new(
        object_store: ObjectStore,
        file_path: &'a str,
        file_size: u64,
    ) -> MetadataLoader<'a> {
        Self {
            object_store,
            file_path,
            file_size,
        }
    }

    /// Get the size of parquet file. If file_size is 0, stat the object store to get the size.
    async fn get_file_size(&self) -> Result<u64> {
        let file_size = match self.file_size {
            0 => self
                .object_store
                .stat(self.file_path)
                .await
                .context(error::OpenDalSnafu)?
                .content_length(),
            other => other,
        };
        Ok(file_size)
    }

    pub async fn load(&self) -> Result<ParquetMetaData> {
        let path = self.file_path;
        let file_size = self.get_file_size().await?;
        let reader =
            ParquetMetaDataReader::new().with_prefetch_hint(Some(DEFAULT_PREFETCH_SIZE as usize));

        let fetch = ObjectStoreFetch {
            object_store: &self.object_store,
            file_path: self.file_path,
        };

        reader
            .load_and_finish(fetch, file_size)
            .await
            .map_err(|e| match unbox_external_error(e) {
                Ok(os_err) => error::OpenDalSnafu {}.into_error(os_err),
                Err(parquet_err) => {
                    error::LoadParquetMetadataSnafu { path }.into_error(parquet_err)
                }
            })
    }
}

/// Unpack ParquetError to get object_store::Error if possible.
fn unbox_external_error(e: ParquetError) -> StdResult<object_store::Error, ParquetError> {
    match e {
        ParquetError::External(boxed_err) => match boxed_err.downcast::<object_store::Error>() {
            Ok(os_err) => Ok(*os_err),
            Err(parquet_error) => Err(ParquetError::External(parquet_error)),
        },
        other => Err(other),
    }
}

struct ObjectStoreFetch<'a> {
    object_store: &'a ObjectStore,
    file_path: &'a str,
}

impl MetadataFetch for ObjectStoreFetch<'_> {
    fn fetch(&mut self, range: std::ops::Range<u64>) -> BoxFuture<'_, ParquetResult<Bytes>> {
        async move {
            let data = self
                .object_store
                .read_with(self.file_path)
                .range(range)
                .await
                .map_err(|e| ParquetError::External(Box::new(e)))?;
            Ok(data.to_bytes())
        }
        .boxed()
    }
}
