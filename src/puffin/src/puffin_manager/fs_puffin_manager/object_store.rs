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

/// A `PuffinFileAccessor` implementation that uses an object store as the underlying storage.
#[derive(Clone)]
pub(crate) struct ObjectStorePuffinFileAccessor {
    object_store: InstrumentedStore,
}

impl ObjectStorePuffinFileAccessor {
    pub fn new(object_store: InstrumentedStore) -> Self {
        Self { object_store }
    }
}

#[async_trait]
impl PuffinFileAccessor for ObjectStorePuffinFileAccessor {
    type Reader = InstrumentedRangeReader;
    type Writer = InstrumentedAsyncWrite;

    async fn reader(&self, puffin_file_name: &str) -> PuffinResult<Self::Reader> {
        self.object_store
            .range_reader(
                puffin_file_name,
                &INDEX_PUFFIN_READ_BYTES_TOTAL,
                &INDEX_PUFFIN_READ_OP_TOTAL,
            )
            .await
            .map_err(BoxedError::new)
            .context(puffin_error::ExternalSnafu)
    }

    async fn writer(&self, puffin_file_name: &str) -> PuffinResult<Self::Writer> {
        self.object_store
            .writer(
                puffin_file_name,
                &INDEX_PUFFIN_WRITE_BYTES_TOTAL,
                &INDEX_PUFFIN_WRITE_OP_TOTAL,
                &INDEX_PUFFIN_FLUSH_OP_TOTAL,
            )
            .await
            .map_err(BoxedError::new)
            .context(puffin_error::ExternalSnafu)
    }
}
