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

use common_base::readable_size::ReadableSize;
use common_datasource::file_format::csv::stream_to_csv;
use common_datasource::file_format::json::stream_to_json;
use common_datasource::file_format::Format;
use common_datasource::object_store::{build_backend, parse_url};
use common_recordbatch::adapter::DfRecordBatchStreamAdapter;
use common_recordbatch::SendableRecordBatchStream;
use object_store::ObjectStore;
use snafu::ResultExt;
use storage::sst::SstInfo;
use storage::{ParquetWriter, Source};
use store_api::storage::ScanRequest;
use table::engine::TableReference;
use table::requests::CopyTableRequest;

use crate::error::{self, Result, WriteParquetSnafu};
use crate::statement::StatementExecutor;

impl StatementExecutor {
    async fn stream_to_file(
        &self,
        stream: SendableRecordBatchStream,
        format: &Format,
        object_store: ObjectStore,
        path: &str,
    ) -> Result<usize> {
        let threshold = ReadableSize::mb(4).as_bytes() as usize;

        match format {
            Format::Csv(_) => stream_to_csv(
                Box::pin(DfRecordBatchStreamAdapter::new(stream)),
                object_store,
                path,
                threshold,
            )
            .await
            .context(error::WriteStreamToFileSnafu { path }),
            Format::Json(_) => stream_to_json(
                Box::pin(DfRecordBatchStreamAdapter::new(stream)),
                object_store,
                path,
                threshold,
            )
            .await
            .context(error::WriteStreamToFileSnafu { path }),
            Format::Parquet(_) => {
                let writer = ParquetWriter::new(path, Source::Stream(stream), object_store);
                let rows_copied = writer
                    .write_sst(&storage::sst::WriteOptions::default())
                    .await
                    .context(WriteParquetSnafu)?
                    .map(|SstInfo { num_rows, .. }| num_rows)
                    .unwrap_or(0);

                Ok(rows_copied)
            }
            _ => error::UnsupportedFormatSnafu { format: *format }.fail(),
        }
    }

    pub(crate) async fn copy_table_to(&self, req: CopyTableRequest) -> Result<usize> {
        let table_ref = TableReference {
            catalog: &req.catalog_name,
            schema: &req.schema_name,
            table: &req.table_name,
        };
        let table = self.get_table(&table_ref).await?;

        let format = Format::try_from(&req.with).context(error::ParseFileFormatSnafu)?;

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

        let scan_req = ScanRequest {
            filters,
            ..Default::default()
        };
        let stream =
            table
                .scan_to_stream(scan_req)
                .await
                .with_context(|_| error::CopyTableSnafu {
                    table_name: table_ref.to_string(),
                })?;

        let (_schema, _host, path) = parse_url(&req.location).context(error::ParseUrlSnafu)?;
        let object_store =
            build_backend(&req.location, &req.connection).context(error::BuildBackendSnafu)?;

        let rows_copied = self
            .stream_to_file(stream, &format, object_store, &path)
            .await?;

        Ok(rows_copied)
    }
}
