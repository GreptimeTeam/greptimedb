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

use std::io::BufReader;
use std::sync::Arc;
use std::task::Poll;

use arrow::datatypes::SchemaRef;
use arrow::json::reader::{infer_json_schema_from_iterator, ValueIter};
use arrow::json::RawReaderBuilder;
use async_trait::async_trait;
use bytes::{Buf, Bytes};
use common_runtime;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::physical_plan::file_format::{FileMeta, FileOpenFuture, FileOpener};
use futures::{ready, StreamExt};
use object_store::ObjectStore;
use snafu::ResultExt;
use tokio_util::io::SyncIoBridge;

use crate::compression::CompressionType;
use crate::error::{self, Result};
use crate::file_format::{self, FileFormat};

#[derive(Debug)]
pub struct JsonFormat {
    pub schema_infer_max_record: Option<usize>,
    pub compression_type: CompressionType,
}

impl Default for JsonFormat {
    fn default() -> Self {
        Self {
            schema_infer_max_record: Some(file_format::DEFAULT_SCHEMA_INFER_MAX_RECORD),
            compression_type: CompressionType::UNCOMPRESSED,
        }
    }
}

#[async_trait]
impl FileFormat for JsonFormat {
    async fn infer_schema(&self, store: &ObjectStore, path: String) -> Result<SchemaRef> {
        let reader = store
            .reader(&path)
            .await
            .context(error::ReadObjectSnafu { path: &path })?;

        let decoded = self.compression_type.convert_async_read(reader);

        let schema_infer_max_record = self.schema_infer_max_record;

        common_runtime::spawn_blocking_read(move || {
            let mut reader = BufReader::new(SyncIoBridge::new(decoded));

            let iter = ValueIter::new(&mut reader, schema_infer_max_record);

            let schema = infer_json_schema_from_iterator(iter)
                .context(error::InferSchemaSnafu { path: &path })?;

            Ok(Arc::new(schema))
        })
        .await
        .context(error::JoinHandleSnafu)?
    }
}

#[derive(Debug, Clone)]
pub struct JsonOpener {
    batch_size: usize,
    projected_schema: SchemaRef,
    object_store: Arc<ObjectStore>,
    compression_type: CompressionType,
}

impl JsonOpener {
    /// Return a new [`JsonOpener`]. Any fields not present in `projected_schema` will be ignored.
    pub fn new(
        batch_size: usize,
        projected_schema: SchemaRef,
        object_store: ObjectStore,
        compression_type: CompressionType,
    ) -> Self {
        Self {
            batch_size,
            projected_schema,
            object_store: Arc::new(object_store),
            compression_type,
        }
    }
}

impl FileOpener for JsonOpener {
    fn open(&self, meta: FileMeta) -> DataFusionResult<FileOpenFuture> {
        let projected_schema = self.projected_schema.clone();
        let path = meta.location().to_string();
        let compression_type = self.compression_type;
        let object_store = self.object_store.clone();
        let batch_size = self.batch_size;

        Ok(Box::pin(async move {
            let reader = object_store
                .reader(&path)
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            let mut upstream = compression_type.convert_stream(reader).fuse();

            let mut buffered = Bytes::new();

            let mut decoder = RawReaderBuilder::new(projected_schema)
                .with_batch_size(batch_size)
                .build_decoder()
                .map_err(DataFusionError::from)?;

            let stream = futures::stream::poll_fn(move |cx| {
                loop {
                    if buffered.is_empty() {
                        if let Some(result) = ready!(upstream.poll_next_unpin(cx)) {
                            buffered = result?;
                        };
                    }

                    let decoded = decoder.decode(buffered.as_ref())?;

                    if decoded == 0 {
                        break;
                    } else {
                        buffered.advance(decoded);
                    }
                }

                Poll::Ready(decoder.flush().transpose())
            });

            Ok(stream.boxed())
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file_format::FileFormat;
    use crate::test_util::{self, format_schema, test_store};

    fn test_data_root() -> String {
        test_util::get_data_dir("tests/json").display().to_string()
    }

    #[tokio::test]
    async fn infer_schema_basic() {
        let json = JsonFormat::default();
        let store = test_store(&test_data_root());
        let schema = json
            .infer_schema(&store, "simple.json".to_string())
            .await
            .unwrap();
        let formatted: Vec<_> = format_schema(schema);

        assert_eq!(
            vec![
                "a: Int64: NULL",
                "b: Float64: NULL",
                "c: Boolean: NULL",
                "d: Utf8: NULL",
            ],
            formatted
        );
    }

    #[tokio::test]
    async fn infer_schema_with_limit() {
        let json = JsonFormat {
            schema_infer_max_record: Some(3),
            ..JsonFormat::default()
        };
        let store = test_store(&test_data_root());
        let schema = json
            .infer_schema(&store, "schema_infer_limit.json".to_string())
            .await
            .unwrap();
        let formatted: Vec<_> = format_schema(schema);

        assert_eq!(
            vec!["a: Int64: NULL", "b: Float64: NULL", "c: Boolean: NULL"],
            formatted
        );
    }
}
