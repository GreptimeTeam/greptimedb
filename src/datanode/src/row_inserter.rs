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

use api::helper;
use api::helper::ColumnDataTypeWrapper;
use api::v1::{ColumnSchema, RowInsertRequest, RowInsertRequests};
use catalog::CatalogManagerRef;
use common_query::Output;
use datatypes::data_type::{ConcreteDataType, DataType};
use futures_util::future;
use session::context::QueryContextRef;
use snafu::{ensure, OptionExt, ResultExt};
use table::requests::InsertRequest;

use crate::error::{
    CatalogSnafu, ColumnDataTypeSnafu, CreateVectorSnafu, EmptyDataSnafu, InsertSnafu,
    InvalidInsertRowLenSnafu, JoinTaskSnafu, Result, TableNotFoundSnafu,
};

pub struct RowInserter {
    catalog_manager: CatalogManagerRef,
}

impl RowInserter {
    pub fn new(catalog_manager: CatalogManagerRef) -> Self {
        Self { catalog_manager }
    }

    pub async fn handle_inserts(
        &self,
        requests: RowInsertRequests,
        ctx: QueryContextRef,
    ) -> Result<Output> {
        let insert_tasks = requests.inserts.into_iter().map(|insert| {
            let catalog_manager = self.catalog_manager.clone();
            let catalog_name = ctx.current_catalog().to_owned();
            let schema_name = ctx.current_schema().to_owned();

            let insert_task = async move {
                let table_name = insert.table_name.clone();
                let table = catalog_manager
                    .table(&catalog_name, &schema_name, &table_name)
                    .await
                    .context(CatalogSnafu)?
                    .with_context(|| TableNotFoundSnafu {
                        table_name: format!("{catalog_name}.{schema_name}.{table_name}"),
                    })?;
                let request =
                    Self::convert_to_table_insert_request(&catalog_name, &schema_name, insert)?;

                table.insert(request).await.with_context(|_| InsertSnafu {
                    table_name: format!("{catalog_name}.{schema_name}.{table_name}"),
                })
            };

            common_runtime::spawn_write(insert_task)
        });

        let results = future::try_join_all(insert_tasks)
            .await
            .context(JoinTaskSnafu)?;
        let affected_rows = results.into_iter().sum::<Result<usize>>()?;

        Ok(Output::AffectedRows(affected_rows))
    }

    pub fn convert_to_table_insert_request(
        catalog_name: &str,
        schema_name: &str,
        request: RowInsertRequest,
    ) -> Result<InsertRequest> {
        let table_name = request.table_name;
        let region_number = request.region_number;
        let rows = request.rows.context(EmptyDataSnafu {
            msg: format!("insert to table: {:?}", table_name),
        })?;
        let schema = rows.schema;
        let rows = rows.rows;
        let num_rows = rows.len();

        let mut columns_values = HashMap::with_capacity(schema.len());
        for column_schema in &schema {
            let column_name = column_schema.column_name.clone();
            let datatype: ConcreteDataType =
                ColumnDataTypeWrapper::try_new(column_schema.datatype.clone())
                    .context(ColumnDataTypeSnafu)?
                    .into();
            let mutable_vector = datatype.create_mutable_vector(num_rows);
            columns_values.insert(column_name, mutable_vector);
        }

        for row in rows {
            ensure!(
                row.values.len() == schema.len(),
                InvalidInsertRowLenSnafu {
                    table_name: format!("{catalog_name}.{schema_name}.{table_name}"),
                    expected: schema.len(),
                    actual: row.values.len(),
                }
            );

            for (ColumnSchema { column_name, .. }, value) in schema.iter().zip(row.values.iter()) {
                if let Some(mutable_vector) = columns_values.get_mut(column_name) {
                    mutable_vector
                        .try_push_value_ref(helper::pb_value_to_value_ref(value))
                        .context(CreateVectorSnafu)?;
                }
            }
        }

        let columns_values = columns_values
            .into_iter()
            .map(|(k, mut v)| (k, v.to_vector()))
            .collect();

        Ok(InsertRequest {
            catalog_name: catalog_name.to_string(),
            schema_name: schema_name.to_string(),
            table_name,
            columns_values,
            region_number,
        })
    }
}
