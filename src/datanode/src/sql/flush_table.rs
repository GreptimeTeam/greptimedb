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

use common_query::Output;
use snafu::ResultExt;
use table::engine::TableReference;
use table::requests::FlushTableRequest;

use crate::error::{self, Result};
use crate::sql::SqlHandler;

impl SqlHandler {
    pub(crate) async fn flush_table(&self, req: FlushTableRequest) -> Result<Output> {
        let table_ref = TableReference {
            catalog: &req.catalog_name,
            schema: &req.schema_name,
            table: &req.table_name,
        };

        let full_table_name = table_ref.to_string();

        let table = self.get_table(&table_ref)?;

        table.flush(req).await.context(error::FlushTableSnafu {
            table_name: full_table_name,
        })?;
        Ok(Output::AffectedRows(0))
    }
}
