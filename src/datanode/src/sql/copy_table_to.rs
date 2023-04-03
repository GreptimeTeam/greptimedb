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

use common_datasource;
use common_datasource::object_store::{build_backend, parse_url};
use common_query::physical_plan::SessionContext;
use common_query::Output;
use snafu::ResultExt;
use storage::sst::SstInfo;
use storage::{ParquetWriter, Source};
use table::engine::TableReference;
use table::requests::CopyTableRequest;

use crate::error::{self, Result, WriteParquetSnafu};
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

        let (_schema, _host, path) = parse_url(&req.location).context(error::ParseUrlSnafu)?;
        let object_store =
            build_backend(&req.location, req.connection).context(error::BuildBackendSnafu)?;

        let writer = ParquetWriter::new(&path, Source::Stream(stream), object_store);

        let rows_copied = writer
            .write_sst(&storage::sst::WriteOptions::default())
            .await
            .context(WriteParquetSnafu)?
            .map(|SstInfo { num_rows, .. }| num_rows)
            .unwrap_or(0);

        Ok(Output::AffectedRows(rows_copied))
    }
}
