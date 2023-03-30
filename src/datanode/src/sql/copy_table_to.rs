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

use std::pin::Pin;

use common_datasource;
use common_datasource::object_store::{build_backend, parse_url};
use common_query::physical_plan::SessionContext;
use common_query::Output;
use common_recordbatch::adapter::DfRecordBatchStreamAdapter;
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::parquet::basic::{Compression, Encoding, ZstdLevel};
use datafusion::parquet::file::properties::WriterProperties;
use datafusion::physical_plan::RecordBatchStream;
use futures::TryStreamExt;
use object_store::ObjectStore;
use snafu::ResultExt;
use table::engine::TableReference;
use table::requests::CopyTableRequest;

use crate::error::{self, Result};
use crate::sql::SqlHandler;

impl SqlHandler {
    pub(crate) async fn copy_table_to(&self, req: CopyTableRequest) -> Result<Output> {
        let table_ref = TableReference {
            catalog: &req.catalog_name,
            schema: &req.schema_name,
            table: &req.table_name,
        };
        let table = self.get_table(&table_ref).await?;

        let stream = table
            .scan(None, &[], None)
            .await
            .with_context(|_| error::CopyTableSnafu {
                table_name: table_ref.to_string(),
            })?;

        let stream = stream
            .execute(0, SessionContext::default().task_ctx())
            .context(error::TableScanExecSnafu)?;
        let stream = Box::pin(DfRecordBatchStreamAdapter::new(stream));

        let (_schema, _host, path) = parse_url(&req.location).context(error::ParseUrlSnafu)?;
        let object_store =
            build_backend(&req.location, req.connection).context(error::BuildBackendSnafu)?;

        let mut parquet_writer = ParquetWriter::new(path.to_string(), stream, object_store);
        // TODO(jiachun):
        // For now, COPY is implemented synchronously.
        // When copying large table, it will be blocked for a long time.
        // Maybe we should make "copy" runs in background?
        // Like PG: https://www.postgresql.org/docs/current/sql-copy.html
        let rows = parquet_writer.flush().await?;

        Ok(Output::AffectedRows(rows))
    }
}

type DfRecordBatchStream = Pin<Box<DfRecordBatchStreamAdapter>>;

struct ParquetWriter {
    file_name: String,
    stream: DfRecordBatchStream,
    object_store: ObjectStore,
    max_row_group_size: usize,
    max_rows_in_segment: usize,
}

impl ParquetWriter {
    pub fn new(file_name: String, stream: DfRecordBatchStream, object_store: ObjectStore) -> Self {
        Self {
            file_name,
            stream,
            object_store,
            // TODO(jiachun): make these configurable: WITH (max_row_group_size=xxx, max_rows_in_segment=xxx)
            max_row_group_size: 4096,
            max_rows_in_segment: 5000000, // default 5M rows per segment
        }
    }

    pub async fn flush(&mut self) -> Result<usize> {
        let schema = self.stream.as_ref().schema();
        let writer_props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(ZstdLevel::default()))
            .set_encoding(Encoding::PLAIN)
            .set_max_row_group_size(self.max_row_group_size)
            .build();
        let mut total_rows = 0;
        loop {
            let mut buf = vec![];
            let mut arrow_writer =
                ArrowWriter::try_new(&mut buf, schema.clone(), Some(writer_props.clone()))
                    .context(error::WriteParquetSnafu)?;

            let mut rows = 0;
            let mut end_loop = true;
            // TODO(hl & jiachun): Since OpenDAL's writer is async and ArrowWriter requires a `std::io::Write`,
            // here we use a Vec<u8> to buffer all parquet bytes in memory and write to object store
            // at a time. Maybe we should find a better way to bridge ArrowWriter and OpenDAL's object.
            while let Some(batch) = self
                .stream
                .try_next()
                .await
                .context(error::PollStreamSnafu)?
            {
                arrow_writer
                    .write(&batch)
                    .context(error::WriteParquetSnafu)?;
                rows += batch.num_rows();
                if rows >= self.max_rows_in_segment {
                    end_loop = false;
                    break;
                }
            }

            let start_row_num = total_rows + 1;
            total_rows += rows;
            arrow_writer.close().context(error::WriteParquetSnafu)?;

            // if rows == 0, we just end up with an empty file.
            //
            // file_name like:
            // "file_name_1_1000000"        (row num: 1 ~ 1000000),
            // "file_name_1000001_xxx"      (row num: 1000001 ~ xxx)
            let file_name = format!("{}_{}_{}", self.file_name, start_row_num, total_rows);
            self.object_store
                .write(&file_name, buf)
                .await
                .context(error::WriteObjectSnafu { path: file_name })?;

            if end_loop {
                return Ok(total_rows);
            }
        }
    }
}
