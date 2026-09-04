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
use std::sync::Arc;

use client::OutputData;
use common_base::readable_size::ReadableSize;
use common_datasource::file_format::Format;
use common_datasource::file_format::csv::stream_to_csv;
use common_datasource::file_format::json::stream_to_json;
use common_datasource::file_format::parquet::{
    PARQUET_TABLE_NAME_KEY, stream_to_parquet_with_metadata,
};
use common_datasource::object_store::build_backend_for_write_with_path;
use common_query::Output;
use common_recordbatch::adapter::DfRecordBatchStreamAdapter;
use common_recordbatch::{
    RecordBatchStream, SendableRecordBatchMapper, SendableRecordBatchStream,
    map_json_type_to_string, map_json_type_to_string_schema,
};
use common_telemetry::{debug, tracing};
use datafusion::datasource::DefaultTableSource;
use datafusion_common::TableReference as DfTableReference;
use datafusion_expr::LogicalPlanBuilder;
use object_store::ObjectStore;
use session::context::QueryContextRef;
use snafu::{OptionExt, ResultExt};
use table::requests::CopyTableRequest;
use table::table::adapter::DfTableProviderAdapter;
use table::table_reference::TableReference;

use crate::error::{self, BuildDfLogicalPlanSnafu, ExecLogicalPlanSnafu, Result};
use crate::statement::StatementExecutor;

// The buffer size should be greater than 5MB (minimum multipart upload size).
/// Buffer size to flush data to object stores.
const WRITE_BUFFER_THRESHOLD: ReadableSize = ReadableSize::mb(8);

/// Default number of concurrent write, it only works on object store backend(e.g., S3).
const WRITE_CONCURRENCY: usize = 8;

fn parquet_metadata(format: &Format, table_name: Option<&str>) -> Option<(String, String)> {
    match (format, table_name) {
        (Format::Parquet(_), Some(table_name)) => {
            Some((PARQUET_TABLE_NAME_KEY.to_string(), table_name.to_string()))
        }
        _ => None,
    }
}

impl StatementExecutor {
    async fn stream_to_file(
        &self,
        stream: SendableRecordBatchStream,
        format: &Format,
        parquet_table_name: Option<&str>,
        object_store: ObjectStore,
        path: &str,
    ) -> Result<usize> {
        let threshold = WRITE_BUFFER_THRESHOLD.as_bytes() as usize;

        let stream = Box::pin(SendableRecordBatchMapper::new(
            stream,
            map_json_type_to_string,
            map_json_type_to_string_schema,
        ));
        match format {
            Format::Csv(format) => stream_to_csv(
                Box::pin(DfRecordBatchStreamAdapter::new(stream)),
                object_store,
                path,
                threshold,
                WRITE_CONCURRENCY,
                format,
            )
            .await
            .context(error::WriteStreamToFileSnafu { path }),
            Format::Json(format) => stream_to_json(
                Box::pin(DfRecordBatchStreamAdapter::new(stream)),
                object_store,
                path,
                threshold,
                WRITE_CONCURRENCY,
                format,
            )
            .await
            .context(error::WriteStreamToFileSnafu { path }),
            Format::Parquet(_) => {
                let schema = stream.schema();
                stream_to_parquet_with_metadata(
                    Box::pin(DfRecordBatchStreamAdapter::new(stream)),
                    schema,
                    object_store,
                    path,
                    WRITE_CONCURRENCY,
                    parquet_metadata(format, parquet_table_name),
                )
                .await
                .context(error::WriteStreamToFileSnafu { path })
            }
            _ => error::UnsupportedFormatSnafu {
                format: format.clone(),
            }
            .fail(),
        }
    }

    #[tracing::instrument(skip_all)]
    pub(crate) async fn copy_table_to(
        &self,
        req: CopyTableRequest,
        query_ctx: QueryContextRef,
    ) -> Result<usize> {
        let table_ref = TableReference::full(&req.catalog_name, &req.schema_name, &req.table_name);
        let table = self.get_table(&table_ref).await?;
        let table_id = table.table_info().table_id();
        let format = Format::try_from(&req.with).context(error::ParseFileFormatSnafu)?;

        let df_table_ref = DfTableReference::from(table_ref);

        let filters = table
            .schema()
            .timestamp_column()
            .and_then(|c| {
                common_query::logical_plan::build_filter_from_timestamp(
                    &c.name,
                    req.timestamp_range.as_ref(),
                )
            })
            .into_iter()
            .collect::<Vec<_>>();

        let table_provider = Arc::new(DfTableProviderAdapter::new(table));
        let table_source = Arc::new(DefaultTableSource::new(table_provider));

        let mut builder = LogicalPlanBuilder::scan_with_filters(
            df_table_ref,
            table_source,
            None,
            filters.clone(),
        )
        .context(BuildDfLogicalPlanSnafu)?;
        for f in filters {
            builder = builder.filter(f).context(BuildDfLogicalPlanSnafu)?;
        }
        let plan = builder.build().context(BuildDfLogicalPlanSnafu)?;

        let output = self
            .query_engine
            .execute(plan, query_ctx)
            .await
            .context(ExecLogicalPlanSnafu)?;

        let CopyTableRequest {
            location,
            connection,
            ..
        } = &req;

        debug!("Copy table: {table_id} to location: {location}");
        self.copy_to_file(&format, Some(&req.table_name), output, location, connection)
            .await
    }

    pub(crate) async fn copy_to_file(
        &self,
        format: &Format,
        parquet_table_name: Option<&str>,
        output: Output,
        location: &str,
        connection: &HashMap<String, String>,
    ) -> Result<usize> {
        let output = output
            .map_dictionary_to_values()
            .context(error::BuildRecordBatchSnafu)?;
        let stream = match output.data {
            OutputData::Stream(stream) => stream,
            OutputData::RecordBatches(record_batches) => record_batches.as_stream(),
            _ => unreachable!(),
        };

        let backend =
            build_backend_for_write_with_path(location, connection, &self.local_file_access)
                .await
                .context(error::BuildBackendSnafu)?;
        let filename = backend.object_path.context(error::UnexpectedSnafu {
            violated: format!("Expected filename, path: {location}"),
        })?;
        self.stream_to_file(
            stream,
            format,
            parquet_table_name,
            backend.object_store,
            &filename,
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use common_datasource::file_format::Format;
    use common_datasource::file_format::json::JsonFormat;
    use common_datasource::file_format::parquet::{PARQUET_TABLE_NAME_KEY, ParquetFormat};

    use super::parquet_metadata;

    #[test]
    fn parquet_metadata_routes_only_table_parquet_exports() {
        let parquet = Format::Parquet(ParquetFormat::default());
        let json = Format::Json(JsonFormat::default());

        assert_eq!(
            Some((PARQUET_TABLE_NAME_KEY.to_string(), "my_table".to_string())),
            parquet_metadata(&parquet, Some("my_table"))
        );
        assert!(parquet_metadata(&parquet, None).is_none());
        assert!(parquet_metadata(&json, Some("my_table")).is_none());
    }
}
