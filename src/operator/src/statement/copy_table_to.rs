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

use client::OutputData;
use common_base::readable_size::ReadableSize;
use common_datasource::file_format::csv::stream_to_csv;
use common_datasource::file_format::json::stream_to_json;
use common_datasource::file_format::parquet::stream_to_parquet;
use common_datasource::file_format::Format;
use common_datasource::object_store::{build_backend, parse_url};
use common_datasource::util::find_dir_and_filename;
use common_recordbatch::adapter::DfRecordBatchStreamAdapter;
use common_recordbatch::SendableRecordBatchStream;
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

impl StatementExecutor {
    async fn stream_to_file(
        &self,
        stream: SendableRecordBatchStream,
        format: &Format,
        object_store: ObjectStore,
        path: &str,
    ) -> Result<usize> {
        let threshold = WRITE_BUFFER_THRESHOLD.as_bytes() as usize;

        match format {
            Format::Csv(_) => stream_to_csv(
                Box::pin(DfRecordBatchStreamAdapter::new(stream)),
                object_store,
                path,
                threshold,
                WRITE_CONCURRENCY,
            )
            .await
            .context(error::WriteStreamToFileSnafu { path }),
            Format::Json(_) => stream_to_json(
                Box::pin(DfRecordBatchStreamAdapter::new(stream)),
                object_store,
                path,
                threshold,
                WRITE_CONCURRENCY,
            )
            .await
            .context(error::WriteStreamToFileSnafu { path }),
            Format::Parquet(_) => {
                let schema = stream.schema();
                stream_to_parquet(
                    Box::pin(DfRecordBatchStreamAdapter::new(stream)),
                    schema,
                    object_store,
                    path,
                    WRITE_CONCURRENCY,
                )
                .await
                .context(error::WriteStreamToFileSnafu { path })
            }
            _ => error::UnsupportedFormatSnafu { format: *format }.fail(),
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
        let stream = match output.data {
            OutputData::Stream(stream) => stream,
            OutputData::RecordBatches(record_batches) => record_batches.as_stream(),
            _ => unreachable!(),
        };

        let (_schema, _host, path) = parse_url(&req.location).context(error::ParseUrlSnafu)?;
        let (_, filename) = find_dir_and_filename(&path);
        let filename = filename.context(error::UnexpectedSnafu {
            violated: format!("Expected filename, path: {path}"),
        })?;
        let object_store =
            build_backend(&req.location, &req.connection).context(error::BuildBackendSnafu)?;
        debug!("Copy table: {table_id} to path: {path}");
        let rows_copied = self
            .stream_to_file(stream, &format, object_store, &filename)
            .await?;

        Ok(rows_copied)
    }
}
