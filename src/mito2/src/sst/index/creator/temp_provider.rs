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
use common_error::ext::BoxedError;
use common_telemetry::warn;
use futures::{AsyncRead, AsyncWrite};
use index::inverted_index::create::sort::external_provider::ExternalTempFileProvider;
use index::inverted_index::error as index_error;
use index::inverted_index::error::Result as IndexResult;
use snafu::ResultExt;

use crate::error::Result;
use crate::metrics::{INDEX_INTERMEDIATE_READ_BYTES_TOTAL, INDEX_INTERMEDIATE_WRITE_BYTES_TOTAL};
use crate::sst::index::object_store::InstrumentedObjectStore;
use crate::sst::location::IntermediateLocation;

pub(crate) struct TempFileProvider {
    location: IntermediateLocation,
    object_store: InstrumentedObjectStore,
}

#[async_trait]
impl ExternalTempFileProvider for TempFileProvider {
    async fn create(
        &self,
        column_name: &str,
        file_id: &str,
    ) -> IndexResult<Box<dyn AsyncWrite + Unpin + Send>> {
        let path = self.location.file_path(column_name, file_id);
        let writer = self
            .object_store
            .writer(&path, &INDEX_INTERMEDIATE_WRITE_BYTES_TOTAL)
            .await
            .map_err(BoxedError::new)
            .context(index_error::ExternalSnafu)?;
        Ok(Box::new(writer))
    }

    async fn read_all(
        &self,
        column_name: &str,
    ) -> IndexResult<Vec<Box<dyn AsyncRead + Unpin + Send>>> {
        let dir = self.location.column_dir(column_name);
        let entries = self
            .object_store
            .list(&dir)
            .await
            .map_err(BoxedError::new)
            .context(index_error::ExternalSnafu)?;
        let mut readers = Vec::with_capacity(entries.len());

        for entry in entries {
            if entry.metadata().is_dir() {
                warn!("Unexpected entry in index temp dir: {:?}", entry.path());
                continue;
            }

            let reader = self
                .object_store
                .reader(entry.path(), &INDEX_INTERMEDIATE_READ_BYTES_TOTAL)
                .await
                .map_err(BoxedError::new)
                .context(index_error::ExternalSnafu)?;
            readers.push(Box::new(reader) as _);
        }

        Ok(readers)
    }
}

impl TempFileProvider {
    pub fn new(location: IntermediateLocation, object_store: InstrumentedObjectStore) -> Self {
        Self {
            location,
            object_store,
        }
    }

    pub async fn cleanup(&self) -> Result<()> {
        self.object_store.remove_all(self.location.root_dir()).await
    }
}
