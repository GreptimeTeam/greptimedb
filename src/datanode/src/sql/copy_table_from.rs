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

use async_compat::CompatExt;
use common_datasource::lister::{Lister, Source};
use common_datasource::object_store::{build_backend, parse_url};
use common_datasource::util::find_dir_and_filename;
use common_query::Output;
use common_recordbatch::error::DataTypesSnafu;
use datafusion::parquet::arrow::ParquetRecordBatchStreamBuilder;
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::vectors::{Helper, VectorRef};
use futures_util::TryStreamExt;
use regex::Regex;
use snafu::{ensure, ResultExt};
use table::engine::TableReference;
use table::requests::{CopyTableRequest, InsertRequest};
use tokio::io::BufReader;

use crate::error::{self, Result};
use crate::sql::SqlHandler;

impl SqlHandler {
    pub(crate) async fn copy_table_from(&self, req: CopyTableRequest) -> Result<Output> {
        let table_ref = TableReference {
            catalog: &req.catalog_name,
            schema: &req.schema_name,
            table: &req.table_name,
        };
        let table = self.get_table(&table_ref)?;

        let (_schema, _host, path) = parse_url(&req.location).context(error::ParseUrlSnafu)?;

        let object_store =
            build_backend(&req.location, req.connection).context(error::BuildBackendSnafu)?;

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

        let mut buf: Vec<RecordBatch> = Vec::new();

        for entry in entries.iter() {
            let path = entry.path();
            let reader = object_store
                .reader(path)
                .await
                .context(error::ReadObjectSnafu { path })?;

            let buf_reader = BufReader::new(reader.compat());

            let builder = ParquetRecordBatchStreamBuilder::new(buf_reader)
                .await
                .context(error::ReadParquetSnafu)?;

            ensure!(
                builder.schema() == table.schema().arrow_schema(),
                error::InvalidSchemaSnafu {
                    table_schema: table.schema().arrow_schema().to_string(),
                    file_schema: (*(builder.schema())).to_string()
                }
            );

            let stream = builder
                .build()
                .context(error::BuildParquetRecordBatchStreamSnafu)?;

            let chunk = stream
                .try_collect::<Vec<_>>()
                .await
                .context(error::ReadParquetSnafu)?;

            buf.extend(chunk.into_iter());
        }

        let fields = table
            .schema()
            .arrow_schema()
            .fields()
            .iter()
            .map(|f| f.name().to_string())
            .collect::<Vec<_>>();

        // Vec<Columns>
        let column_chunks = buf
            .into_iter()
            .map(|c| Helper::try_into_vectors(c.columns()).context(DataTypesSnafu))
            .collect::<Vec<_>>();

        let mut futs = Vec::with_capacity(column_chunks.len());

        for column_chunk in column_chunks.into_iter() {
            let column_chunk = column_chunk.context(error::ParseDataTypesSnafu)?;
            let columns_values = fields
                .iter()
                .cloned()
                .zip(column_chunk.into_iter())
                .collect::<HashMap<String, VectorRef>>();

            futs.push(table.insert(InsertRequest {
                catalog_name: req.catalog_name.to_string(),
                schema_name: req.schema_name.to_string(),
                table_name: req.table_name.to_string(),
                columns_values,
                //TODO: support multi-regions
                region_number: 0,
            }))
        }

        let result = futures::future::try_join_all(futs)
            .await
            .context(error::InsertSnafu {
                table_name: req.table_name.to_string(),
            })?;

        Ok(Output::AffectedRows(result.iter().sum()))
    }
}
