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

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;

use async_compat::CompatExt;
use common_base::readable_size::ReadableSize;
use common_datasource::file_format::csv::{CsvConfigBuilder, CsvOpener};
use common_datasource::file_format::json::JsonOpener;
use common_datasource::file_format::{FileFormat, Format};
use common_datasource::lister::{Lister, Source};
use common_datasource::object_store::{build_backend, parse_url};
use common_datasource::util::find_dir_and_filename;
use common_query::Output;
use common_recordbatch::adapter::ParquetRecordBatchStreamAdapter;
use common_recordbatch::DfSendableRecordBatchStream;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::parquet::arrow::ParquetRecordBatchStreamBuilder;
use datafusion::physical_plan::file_format::{FileOpener, FileScanConfig, FileStream};
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datatypes::arrow::datatypes::{DataType, SchemaRef};
use datatypes::vectors::Helper;
use futures_util::StreamExt;
use object_store::{Entry, EntryMode, Metakey, ObjectStore};
use regex::Regex;
use snafu::ResultExt;
use table::engine::TableReference;
use table::requests::{CopyTableRequest, InsertRequest};
use tokio::io::BufReader;

use crate::error::{self, IntoVectorsSnafu, Result};
use crate::statement::StatementExecutor;

const DEFAULT_BATCH_SIZE: usize = 8192;

impl StatementExecutor {
    async fn list_copy_from_entries(
        &self,
        req: &CopyTableRequest,
    ) -> Result<(ObjectStore, Vec<Entry>)> {
        let (_schema, _host, path) = parse_url(&req.location).context(error::ParseUrlSnafu)?;

        let object_store =
            build_backend(&req.location, &req.connection).context(error::BuildBackendSnafu)?;

        let (dir, filename) = find_dir_and_filename(&path);
        let regex = req
            .pattern
            .as_ref()
            .map(|x| Regex::new(x))
            .transpose()
            .context(error::BuildRegexSnafu)?;

        let source = if let Some(filename) = filename {
            Source::Filename(filename)
        } else {
            Source::Dir
        };

        let lister = Lister::new(object_store.clone(), source, dir, regex);

        let entries = lister.list().await.context(error::ListObjectsSnafu)?;

        Ok((object_store, entries))
    }

    async fn infer_schema(
        &self,
        format: &Format,
        object_store: ObjectStore,
        path: &str,
    ) -> Result<SchemaRef> {
        match format {
            Format::Csv(format) => Ok(Arc::new(
                format
                    .infer_schema(&object_store, path)
                    .await
                    .context(error::InferSchemaSnafu { path })?,
            )),
            Format::Json(format) => Ok(Arc::new(
                format
                    .infer_schema(&object_store, path)
                    .await
                    .context(error::InferSchemaSnafu { path })?,
            )),
            Format::Parquet(_) => {
                let reader = object_store
                    .reader(path)
                    .await
                    .context(error::ReadObjectSnafu { path })?;

                let buf_reader = BufReader::new(reader);

                let builder = ParquetRecordBatchStreamBuilder::new(buf_reader)
                    .await
                    .context(error::ReadParquetSnafu)?;
                Ok(builder.schema().clone())
            }
        }
    }

    async fn build_file_stream<F: FileOpener + Send + 'static>(
        &self,
        opener: F,
        filename: &str,
        file_schema: SchemaRef,
    ) -> Result<DfSendableRecordBatchStream> {
        let stream = FileStream::new(
            &FileScanConfig {
                object_store_url: ObjectStoreUrl::parse("empty://").unwrap(), // won't be used
                file_schema,
                file_groups: vec![vec![PartitionedFile::new(filename.to_string(), 10)]],
                statistics: Default::default(),
                projection: None,
                limit: None,
                table_partition_cols: vec![],
                output_ordering: None,
                infinite_source: false,
            },
            0,
            opener,
            &ExecutionPlanMetricsSet::new(),
        )
        .context(error::BuildFileStreamSnafu)?;

        Ok(Box::pin(stream))
    }

    async fn build_read_stream(
        &self,
        format: &Format,
        object_store: ObjectStore,
        path: &str,
        schema: SchemaRef,
    ) -> Result<DfSendableRecordBatchStream> {
        match format {
            Format::Csv(format) => {
                let csv_conf = CsvConfigBuilder::default()
                    .batch_size(DEFAULT_BATCH_SIZE)
                    .file_schema(schema.clone())
                    .build()
                    .context(error::BuildCsvConfigSnafu)?;

                self.build_file_stream(
                    CsvOpener::new(csv_conf, object_store, format.compression_type),
                    path,
                    schema,
                )
                .await
            }
            Format::Json(format) => {
                self.build_file_stream(
                    JsonOpener::new(
                        DEFAULT_BATCH_SIZE,
                        schema.clone(),
                        object_store,
                        format.compression_type,
                    ),
                    path,
                    schema,
                )
                .await
            }
            Format::Parquet(_) => {
                let reader = object_store
                    .reader(path)
                    .await
                    .context(error::ReadObjectSnafu { path })?;

                let buf_reader = BufReader::new(reader.compat());

                let builder = ParquetRecordBatchStreamBuilder::new(buf_reader)
                    .await
                    .context(error::ReadParquetSnafu)?;

                let upstream = builder
                    .build()
                    .context(error::BuildParquetRecordBatchStreamSnafu)?;

                Ok(Box::pin(ParquetRecordBatchStreamAdapter::new(upstream)))
            }
        }
    }

    pub(crate) async fn copy_table_from(&self, req: CopyTableRequest) -> Result<Output> {
        let table_ref = TableReference {
            catalog: &req.catalog_name,
            schema: &req.schema_name,
            table: &req.table_name,
        };
        let table = self.get_table(&table_ref).await?;

        let format = Format::try_from(&req.with).context(error::ParseFileFormatSnafu)?;

        let fields = table
            .schema()
            .arrow_schema()
            .fields()
            .iter()
            .map(|f| f.name().to_string())
            .collect::<Vec<_>>();

        let (object_store, entries) = self.list_copy_from_entries(&req).await?;

        let mut files = Vec::with_capacity(entries.len());

        for entry in entries.iter() {
            let metadata = object_store
                .metadata(entry, Metakey::Mode)
                .await
                .context(error::ReadObjectSnafu { path: entry.path() })?;
            if metadata.mode() != EntryMode::FILE {
                continue;
            }
            let path = entry.path();
            let file_schema = self
                .infer_schema(&format, object_store.clone(), path)
                .await?;

            ensure_schema_matches_ignore_timezone(&file_schema, table.schema().arrow_schema())?;

            files.push((file_schema, path))
        }

        let mut rows_inserted = 0;
        for (schema, path) in files {
            let mut stream = self
                .build_read_stream(&format, object_store.clone(), path, schema)
                .await?;

            // TODO(hl): make this configurable through options.
            let pending_mem_threshold = ReadableSize::mb(32).as_bytes();
            let mut pending_mem_size = 0;
            let mut pending = vec![];

            while let Some(r) = stream.next().await {
                let record_batch = r.context(error::ReadRecordBatchSnafu)?;
                let vectors =
                    Helper::try_into_vectors(record_batch.columns()).context(IntoVectorsSnafu)?;

                pending_mem_size += vectors.iter().map(|v| v.memory_size()).sum::<usize>();

                let columns_values = fields
                    .iter()
                    .cloned()
                    .zip(vectors.into_iter())
                    .collect::<HashMap<_, _>>();

                pending.push(table.insert(InsertRequest {
                    catalog_name: req.catalog_name.to_string(),
                    schema_name: req.schema_name.to_string(),
                    table_name: req.table_name.to_string(),
                    columns_values,
                    //TODO: support multi-regions
                    region_number: 0,
                }));

                if pending_mem_size as u64 >= pending_mem_threshold {
                    rows_inserted +=
                        batch_insert(&mut pending, &mut pending_mem_size, &req.table_name).await?;
                }
            }

            if !pending.is_empty() {
                rows_inserted +=
                    batch_insert(&mut pending, &mut pending_mem_size, &req.table_name).await?;
            }
        }

        Ok(Output::AffectedRows(rows_inserted))
    }
}

/// Executes all pending inserts all at once, drain pending requests and reset pending bytes.
async fn batch_insert(
    pending: &mut Vec<impl Future<Output = table::error::Result<usize>>>,
    pending_bytes: &mut usize,
    table_name: &str,
) -> Result<usize> {
    let batch = pending.drain(..);
    let res: usize = futures::future::try_join_all(batch)
        .await
        .context(error::InsertSnafu { table_name })?
        .iter()
        .sum();
    *pending_bytes = 0;
    Ok(res)
}

fn ensure_schema_matches_ignore_timezone(left: &SchemaRef, right: &SchemaRef) -> Result<()> {
    let not_match = left
        .fields
        .iter()
        .zip(right.fields.iter())
        .map(|(l, r)| (l.data_type(), r.data_type()))
        .enumerate()
        .find(|(_, (l, r))| !data_type_equals_ignore_timezone(l, r));

    if let Some((index, _)) = not_match {
        error::InvalidSchemaSnafu {
            index,
            table_schema: left.to_string(),
            file_schema: right.to_string(),
        }
        .fail()
    } else {
        Ok(())
    }
}

fn data_type_equals_ignore_timezone(l: &DataType, r: &DataType) -> bool {
    match (l, r) {
        (DataType::List(a), DataType::List(b))
        | (DataType::LargeList(a), DataType::LargeList(b)) => {
            a.is_nullable() == b.is_nullable()
                && data_type_equals_ignore_timezone(a.data_type(), b.data_type())
        }
        (DataType::FixedSizeList(a, a_size), DataType::FixedSizeList(b, b_size)) => {
            a_size == b_size
                && a.is_nullable() == b.is_nullable()
                && data_type_equals_ignore_timezone(a.data_type(), b.data_type())
        }
        (DataType::Struct(a), DataType::Struct(b)) => {
            a.len() == b.len()
                && a.iter().zip(b).all(|(a, b)| {
                    a.is_nullable() == b.is_nullable()
                        && data_type_equals_ignore_timezone(a.data_type(), b.data_type())
                })
        }
        (DataType::Map(a_field, a_is_sorted), DataType::Map(b_field, b_is_sorted)) => {
            a_field == b_field && a_is_sorted == b_is_sorted
        }
        (DataType::Timestamp(l_unit, _), DataType::Timestamp(r_unit, _)) => l_unit == r_unit,
        _ => l == r,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datatypes::arrow::datatypes::{Field, Schema};

    use super::*;

    fn test_schema_matches(l: (DataType, bool), r: (DataType, bool), matches: bool) {
        let s1 = Arc::new(Schema::new(vec![Field::new("col", l.0, l.1)]));
        let s2 = Arc::new(Schema::new(vec![Field::new("col", r.0, r.1)]));
        let res = ensure_schema_matches_ignore_timezone(&s1, &s2);
        assert_eq!(matches, res.is_ok())
    }

    #[test]
    fn test_ensure_datatype_matches_ignore_timezone() {
        test_schema_matches(
            (
                DataType::Timestamp(datatypes::arrow::datatypes::TimeUnit::Second, None),
                true,
            ),
            (
                DataType::Timestamp(datatypes::arrow::datatypes::TimeUnit::Second, None),
                true,
            ),
            true,
        );

        test_schema_matches(
            (
                DataType::Timestamp(
                    datatypes::arrow::datatypes::TimeUnit::Second,
                    Some("UTC".into()),
                ),
                true,
            ),
            (
                DataType::Timestamp(datatypes::arrow::datatypes::TimeUnit::Second, None),
                true,
            ),
            true,
        );

        test_schema_matches(
            (
                DataType::Timestamp(
                    datatypes::arrow::datatypes::TimeUnit::Second,
                    Some("UTC".into()),
                ),
                true,
            ),
            (
                DataType::Timestamp(
                    datatypes::arrow::datatypes::TimeUnit::Second,
                    Some("PDT".into()),
                ),
                true,
            ),
            true,
        );

        test_schema_matches(
            (
                DataType::Timestamp(
                    datatypes::arrow::datatypes::TimeUnit::Second,
                    Some("UTC".into()),
                ),
                true,
            ),
            (
                DataType::Timestamp(
                    datatypes::arrow::datatypes::TimeUnit::Millisecond,
                    Some("UTC".into()),
                ),
                true,
            ),
            false,
        );

        test_schema_matches((DataType::Int8, true), (DataType::Int8, true), true);

        test_schema_matches((DataType::Int8, true), (DataType::Int16, true), false);
    }
}
