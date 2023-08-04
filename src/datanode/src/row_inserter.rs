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

use api::helper::ColumnDataTypeWrapper;
use api::v1::value::Value;
use api::v1::{RowInsertRequest, RowInsertRequests};
use catalog::CatalogManagerRef;
use common_query::Output;
use common_time::time::Time;
use common_time::Timestamp;
use datatypes::data_type::{ConcreteDataType, DataType};
use datatypes::value::Value as InternalValue;
use futures_util::future;
use session::context::QueryContextRef;
use snafu::{ensure, OptionExt, ResultExt};
use table::requests::InsertRequest;

use crate::error::{CatalogSnafu, ColumnDataTypeSnafu, CreateVectorSnafu, EmptyDataSnafu, InsertSnafu, InvalidInsertRowLenSnafu, JoinTaskSnafu, Result, TableNotFoundSnafu};

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
            let catalog_name = ctx.current_catalog();
            let schema_name = ctx.current_schema();

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

        let mut columns_values = HashMap::with_capacity(schema.len());
        for row in rows.rows {
            let row_values = row.values;

            ensure!(
                row_values.len() == schema.len(),
                InvalidInsertRowLenSnafu {
                    table_name: format!("{catalog_name}.{schema_name}.{table_name}"),
                    expected: schema.len(),
                    actual: row_values.len(),
                }
            );

            let row_count = row_values.len();
            for (i, value) in row_values.into_iter().enumerate() {
                let column_name = schema[i].column_name.clone();
                let datatype: ConcreteDataType = ColumnDataTypeWrapper::try_new(schema[i].datatype)
                    .context(ColumnDataTypeSnafu)?
                    .into();
                let column_values_builder = columns_values
                    .entry(column_name)
                    .or_insert_with(|| datatype.create_mutable_vector(row_count));

                if let Some(value) = value.value {
                    let value = Self::convert_value(value);
                    column_values_builder
                        .try_push_value_ref(value.as_value_ref())
                        .context(CreateVectorSnafu)?;
                } else {
                    column_values_builder.push_null();
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

    fn convert_value(value: Value) -> InternalValue {
        match value {
            Value::I8Value(v) => (v as i8).into(),
            Value::I16Value(v) => (v as i16).into(),
            Value::I32Value(v) => v.into(),
            Value::I64Value(v) => v.into(),
            Value::U8Value(v) => (v as u8).into(),
            Value::U16Value(v) => (v as u16).into(),
            Value::U32Value(v) => v.into(),
            Value::U64Value(v) => v.into(),
            Value::F32Value(v) => v.into(),
            Value::F64Value(v) => v.into(),
            Value::BoolValue(v) => v.into(),
            Value::BinaryValue(v) => v.into(),
            Value::StringValue(v) => v.into(),
            Value::DateValue(v) => InternalValue::Date(v.into()),
            Value::DatetimeValue(v) => InternalValue::DateTime(v.into()),
            Value::TsSecondValue(v) => InternalValue::Timestamp(Timestamp::new_second(v)),
            Value::TsMillisecondValue(v) => InternalValue::Timestamp(Timestamp::new_millisecond(v)),
            Value::TsMicrosecondValue(v) => InternalValue::Timestamp(Timestamp::new_microsecond(v)),
            Value::TsNanosecondValue(v) => InternalValue::Timestamp(Timestamp::new_nanosecond(v)),
            Value::TimeSecondValue(v) => InternalValue::Time(Time::new_second(v)),
            Value::TimeMillisecondValue(v) => InternalValue::Time(Time::new_millisecond(v)),
            Value::TimeMicrosecondValue(v) => InternalValue::Time(Time::new_microsecond(v)),
            Value::TimeNanosecondValue(v) => InternalValue::Time(Time::new_nanosecond(v)),
        }
    }
}
