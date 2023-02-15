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

use common_query::physical_plan::SessionContext;
use common_query::Output;
use common_recordbatch::adapter::DfRecordBatchStreamAdapter;
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::parquet::basic::{Compression, Encoding};
use datafusion::parquet::file::properties::WriterProperties;
use datafusion::physical_plan::RecordBatchStream;
use futures::TryStreamExt;
use object_store::services::fs::Builder;
use object_store::ObjectStore;
use snafu::{OptionExt, ResultExt};
use table::engine::{EngineContext, TableReference};
use table::requests::CopyTableRequest;

use crate::error::{self, Result};
use crate::sql::SqlHandler;

impl SqlHandler {
    pub(crate) async fn copy_table(&self, req: CopyTableRequest) -> Result<Output> {
        let table_ref = TableReference {
            catalog: &req.catalog_name.to_string(),
            schema: &req.schema_name.to_string(),
            table: &req.table_name.to_string(),
        };

        let table = self
            .table_engine()
            .get_table(&EngineContext::default(), &table_ref)
            .context(error::GetTableSnafu {
                table_name: table_ref.to_string(),
            })?
            .context(error::TableNotFoundSnafu {
                table_name: table_ref.to_string(),
            })?;

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

        let accessor = Builder::default().build().unwrap();
        let object_store = ObjectStore::new(accessor);

        let mut parquet_writer = ParquetWriter::new(req.file_name, stream, object_store);

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
            // TODO(jiachun): make these configurable
            max_row_group_size: 4096,
            max_rows_in_segment: 1000000,
        }
    }

    pub async fn flush(&mut self) -> Result<usize> {
        let schema = self.stream.as_ref().schema();
        let writer_props = WriterProperties::builder()
            .set_compression(Compression::ZSTD)
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
            // at a time. Maybe we should find a better way to brige ArrowWriter and OpenDAL's object.
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

            if rows == 0 {
                return Ok(total_rows);
            }

            total_rows += rows;
            arrow_writer.close().context(error::WriteParquetSnafu)?;

            let file_name = format!("{}_{}", self.file_name, total_rows);
            let object = self.object_store.object(&file_name);
            object.write(buf).await.context(error::WriteObjectSnafu {
                path: object.path(),
            })?;

            if end_loop {
                return Ok(total_rows);
            }
        }
    }
}
