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

use std::sync::Arc;

use arrow_schema::{ArrowError, Schema, SchemaRef};
use async_trait::async_trait;
use bytes::Bytes;
use common_recordbatch::adapter::RecordBatchStreamTypeAdapter;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::{FileMeta, FileOpenFuture, FileOpener};
use datafusion::error::{DataFusionError, Result as DfResult};
use futures::future::BoxFuture;
use futures::{FutureExt, StreamExt, TryStreamExt};
use object_store::ObjectStore;
use orc_rust::arrow_reader::ArrowReaderBuilder;
use orc_rust::async_arrow_reader::ArrowStreamReader;
use orc_rust::reader::AsyncChunkReader;
use snafu::ResultExt;

use crate::error::{self, Result};
use crate::file_format::FileFormat;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct OrcFormat;

#[derive(Clone)]
pub struct ReaderAdapter {
    reader: object_store::Reader,
    len: u64,
}

impl ReaderAdapter {
    pub fn new(reader: object_store::Reader, len: u64) -> Self {
        Self { reader, len }
    }
}

impl AsyncChunkReader for ReaderAdapter {
    fn len(&mut self) -> BoxFuture<'_, std::io::Result<u64>> {
        async move { Ok(self.len) }.boxed()
    }

    fn get_bytes(
        &mut self,
        offset_from_start: u64,
        length: u64,
    ) -> BoxFuture<'_, std::io::Result<Bytes>> {
        async move {
            let bytes = self
                .reader
                .read(offset_from_start..offset_from_start + length)
                .await?;
            Ok(bytes.to_bytes())
        }
        .boxed()
    }
}

pub async fn new_orc_stream_reader(
    reader: ReaderAdapter,
) -> Result<ArrowStreamReader<ReaderAdapter>> {
    let reader_build = ArrowReaderBuilder::try_new_async(reader)
        .await
        .context(error::OrcReaderSnafu)?;
    Ok(reader_build.build_async())
}

pub async fn infer_orc_schema(reader: ReaderAdapter) -> Result<Schema> {
    let reader = new_orc_stream_reader(reader).await?;
    Ok(reader.schema().as_ref().clone())
}

#[async_trait]
impl FileFormat for OrcFormat {
    async fn infer_schema(&self, store: &ObjectStore, path: &str) -> Result<Schema> {
        let meta = store
            .stat(path)
            .await
            .context(error::ReadObjectSnafu { path })?;
        let reader = store
            .reader(path)
            .await
            .context(error::ReadObjectSnafu { path })?;
        let schema = infer_orc_schema(ReaderAdapter::new(reader, meta.content_length())).await?;
        Ok(schema)
    }
}

#[derive(Debug, Clone)]
pub struct OrcOpener {
    object_store: Arc<ObjectStore>,
    output_schema: SchemaRef,
    projection: Option<Vec<usize>>,
}

impl OrcOpener {
    pub fn new(
        object_store: ObjectStore,
        output_schema: SchemaRef,
        projection: Option<Vec<usize>>,
    ) -> Self {
        Self {
            object_store: Arc::from(object_store),
            output_schema,
            projection,
        }
    }
}

impl FileOpener for OrcOpener {
    fn open(&self, meta: FileMeta, _: PartitionedFile) -> DfResult<FileOpenFuture> {
        let object_store = self.object_store.clone();
        let projected_schema = if let Some(projection) = &self.projection {
            let projected_schema = self
                .output_schema
                .project(projection)
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            Arc::new(projected_schema)
        } else {
            self.output_schema.clone()
        };
        let projection = self.projection.clone();
        Ok(Box::pin(async move {
            let path = meta.location().to_string();

            let meta = object_store
                .stat(&path)
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            let reader = object_store
                .reader(&path)
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            let stream_reader =
                new_orc_stream_reader(ReaderAdapter::new(reader, meta.content_length()))
                    .await
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;

            let stream =
                RecordBatchStreamTypeAdapter::new(projected_schema, stream_reader, projection);

            let adopted = stream.map_err(|e| ArrowError::ExternalError(Box::new(e)));
            Ok(adopted.boxed())
        }))
    }
}

#[cfg(test)]
mod tests {
    use common_test_util::find_workspace_path;

    use super::*;
    use crate::file_format::FileFormat;
    use crate::test_util::{format_schema, test_store};

    fn test_data_root() -> String {
        find_workspace_path("/src/common/datasource/tests/orc")
            .display()
            .to_string()
    }

    #[tokio::test]
    async fn test_orc_infer_schema() {
        let store = test_store(&test_data_root());
        let schema = OrcFormat.infer_schema(&store, "test.orc").await.unwrap();
        let formatted: Vec<_> = format_schema(schema);

        assert_eq!(
            vec![
                "double_a: Float64: NULL",
                "a: Float32: NULL",
                "b: Boolean: NULL",
                "str_direct: Utf8: NULL",
                "d: Utf8: NULL",
                "e: Utf8: NULL",
                "f: Utf8: NULL",
                "int_short_repeated: Int32: NULL",
                "int_neg_short_repeated: Int32: NULL",
                "int_delta: Int32: NULL",
                "int_neg_delta: Int32: NULL",
                "int_direct: Int32: NULL",
                "int_neg_direct: Int32: NULL",
                "bigint_direct: Int64: NULL",
                "bigint_neg_direct: Int64: NULL",
                "bigint_other: Int64: NULL",
                "utf8_increase: Utf8: NULL",
                "utf8_decrease: Utf8: NULL",
                "timestamp_simple: Timestamp(Nanosecond, None): NULL",
                "date_simple: Date32: NULL"
            ],
            formatted
        );
    }
}
