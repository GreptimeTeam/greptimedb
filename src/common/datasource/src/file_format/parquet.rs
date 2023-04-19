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

use arrow_schema::Schema;
use async_trait::async_trait;
use datafusion::error::{DataFusionError, Result as DatafusionResult};
use datafusion::parquet::arrow::async_reader::AsyncFileReader;
use datafusion::parquet::arrow::parquet_to_arrow_schema;
use datafusion::physical_plan::file_format::{FileMeta, ParquetFileReaderFactory};
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use object_store::ObjectStore;
use snafu::ResultExt;

use crate::error::{self, Result};
use crate::file_format::FileFormat;

#[derive(Debug, Default)]
pub struct ParquetFormat {}

#[async_trait]
impl FileFormat for ParquetFormat {
    async fn infer_schema(&self, store: &ObjectStore, path: String) -> Result<Schema> {
        let mut reader = store
            .reader(&path)
            .await
            .context(error::ReadObjectSnafu { path: &path })?;

        let metadata = reader
            .get_metadata()
            .await
            .context(error::ReadParquetSnafuSnafu)?;

        let file_metadata = metadata.file_metadata();
        let schema = parquet_to_arrow_schema(
            file_metadata.schema_descr(),
            file_metadata.key_value_metadata(),
        )
        .context(error::ParquetToSchemaSnafu)?;

        Ok(schema)
    }
}

#[derive(Debug, Clone)]
pub struct DefaultParquetFileReaderFactory {
    object_store: ObjectStore,
}

/// Returns a AsyncFileReader factory
impl DefaultParquetFileReaderFactory {
    pub fn new(object_store: ObjectStore) -> Self {
        Self { object_store }
    }
}

impl ParquetFileReaderFactory for DefaultParquetFileReaderFactory {
    // TODO(weny): Supports [`metadata_size_hint`].
    // The upstream has a implementation supports [`metadata_size_hint`],
    // however it coupled with Box<dyn ObjectStore>.
    fn create_reader(
        &self,
        _partition_index: usize,
        file_meta: FileMeta,
        _metadata_size_hint: Option<usize>,
        _metrics: &ExecutionPlanMetricsSet,
    ) -> DatafusionResult<Box<dyn AsyncFileReader + Send>> {
        let path = file_meta.location().to_string();

        let object_store = self.object_store.clone();

        let reader = std::thread::spawn(move || {
            common_runtime::block_on_read(async move {
                object_store
                    .reader(&path)
                    .await
                    .map_err(|e| DataFusionError::External(Box::new(e)))
                    .map(Box::new)
            })
        })
        .join()
        .unwrap();

        Ok(reader?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file_format::FileFormat;
    use crate::test_util::{self, format_schema, test_store};

    fn test_data_root() -> String {
        test_util::get_data_dir("tests/parquet")
            .display()
            .to_string()
    }

    #[tokio::test]
    async fn infer_schema_basic() {
        let json = ParquetFormat::default();
        let store = test_store(&test_data_root());
        let schema = json
            .infer_schema(&store, "basic.parquet".to_string())
            .await
            .unwrap();
        let formatted: Vec<_> = format_schema(schema);

        assert_eq!(vec!["num: Int64: NULL", "str: Utf8: NULL"], formatted);
    }
}
