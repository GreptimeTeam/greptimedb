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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use common_recordbatch::SendableRecordBatchStream;
use datafusion::execution::FunctionRegistry;
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::Cast;
use datafusion::prelude::SessionContext;
use datafusion_expr::expr::Expr;
use datatypes::data_type::DataType;
use datatypes::prelude::ConcreteDataType;
use datatypes::schema::constraint::{CURRENT_TIMESTAMP, CURRENT_TIMESTAMP_FN, NOW_FN};
use datatypes::schema::{ColumnDefaultConstraint, ColumnSchema, SchemaRef};
use lazy_static::lazy_static;
use snafu::ResultExt;
use store_api::data_source::DataSourceRef;
use store_api::storage::ScanRequest;

use crate::error::{Result, TablesRecordBatchSnafu};
use crate::metadata::{FilterPushDownType, TableInfoRef, TableType};

pub mod adapter;
mod metrics;
pub mod numbers;
pub mod scan;

lazy_static! {
    /// The [`Expr`] to call UDF function `now()`.
    static ref NOW_EXPR: Expr = {
        let ctx = SessionContext::new();

        let now_udf = ctx.udf("now").expect("now UDF not found");

        Expr::ScalarFunction(ScalarFunction {
            func: now_udf,
            args: vec![],
        })
    };
}

pub type TableRef = Arc<Table>;

/// Table handle.
pub struct Table {
    table_info: TableInfoRef,
    filter_pushdown: FilterPushDownType,
    data_source: DataSourceRef,
    /// Columns default [`Expr`]
    column_defaults: HashMap<String, Expr>,
}

impl Table {
    pub fn new(
        table_info: TableInfoRef,
        filter_pushdown: FilterPushDownType,
        data_source: DataSourceRef,
    ) -> Self {
        Self {
            column_defaults: collect_column_defaults(table_info.meta.schema.column_schemas()),
            table_info,
            filter_pushdown,
            data_source,
        }
    }

    /// Get column default [`Expr`], if available.
    pub fn get_column_default(&self, column: &str) -> Option<&Expr> {
        self.column_defaults.get(column)
    }

    pub fn data_source(&self) -> DataSourceRef {
        self.data_source.clone()
    }

    /// Get a reference to the schema for this table.
    pub fn schema(&self) -> SchemaRef {
        self.table_info.meta.schema.clone()
    }

    /// Get a reference to the table info.
    pub fn table_info(&self) -> TableInfoRef {
        self.table_info.clone()
    }

    /// Get the type of this table for metadata/catalog purposes.
    pub fn table_type(&self) -> TableType {
        self.table_info.table_type
    }

    pub async fn scan_to_stream(&self, request: ScanRequest) -> Result<SendableRecordBatchStream> {
        self.data_source
            .get_stream(request)
            .context(TablesRecordBatchSnafu)
    }

    /// Tests whether the table provider can make use of any or all filter expressions
    /// to optimise data retrieval.
    pub fn supports_filters_pushdown(&self, filters: &[&Expr]) -> Result<Vec<FilterPushDownType>> {
        Ok(vec![self.filter_pushdown; filters.len()])
    }

    /// Get primary key columns in the definition order.
    pub fn primary_key_columns(&self) -> impl Iterator<Item = ColumnSchema> + '_ {
        self.table_info
            .meta
            .primary_key_indices
            .iter()
            .map(|i| self.table_info.meta.schema.column_schemas()[*i].clone())
    }

    /// Get field columns in the definition order.
    pub fn field_columns(&self) -> impl Iterator<Item = ColumnSchema> + '_ {
        // `value_indices` in TableMeta is not reliable. Do a filter here.
        let primary_keys = self
            .table_info
            .meta
            .primary_key_indices
            .iter()
            .copied()
            .collect::<HashSet<_>>();

        self.table_info
            .meta
            .schema
            .column_schemas()
            .iter()
            .enumerate()
            .filter(move |(i, c)| !primary_keys.contains(i) && !c.is_time_index())
            .map(|(_, c)| c.clone())
    }
}

/// Collects column default [`Expr`] from column schemas.
fn collect_column_defaults(column_schemas: &[ColumnSchema]) -> HashMap<String, Expr> {
    column_schemas
        .iter()
        .filter_map(|column_schema| {
            default_constraint_to_expr(
                column_schema.default_constraint()?,
                &column_schema.data_type,
            )
            .map(|expr| (column_schema.name.to_string(), expr))
        })
        .collect()
}

/// Try to cast the [`ColumnDefaultConstraint`] to [`Expr`] by the target data type.
fn default_constraint_to_expr(
    default_constraint: &ColumnDefaultConstraint,
    target_type: &ConcreteDataType,
) -> Option<Expr> {
    match default_constraint {
        ColumnDefaultConstraint::Value(v) => v
            .try_to_scalar_value(target_type)
            .ok()
            .map(|x| Expr::Literal(x, None)),

        ColumnDefaultConstraint::Function(name)
            if matches!(
                name.as_str(),
                CURRENT_TIMESTAMP | CURRENT_TIMESTAMP_FN | NOW_FN
            ) =>
        {
            Some(Expr::Cast(Cast {
                expr: Box::new(NOW_EXPR.clone()),
                data_type: target_type.as_arrow_type(),
            }))
        }

        ColumnDefaultConstraint::Function(_) => None,
    }
}

#[cfg(test)]
mod tests {
    use datafusion_common::ScalarValue;
    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::ColumnDefaultConstraint;

    use super::*;

    #[test]
    fn test_collect_columns_defaults() {
        let column_schemas = vec![
            ColumnSchema::new("col1", ConcreteDataType::int32_datatype(), false),
            ColumnSchema::new("col2", ConcreteDataType::string_datatype(), true)
                .with_default_constraint(Some(ColumnDefaultConstraint::Value("test".into())))
                .unwrap(),
            ColumnSchema::new(
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            )
            .with_time_index(true)
            .with_default_constraint(Some(ColumnDefaultConstraint::Function(
                "current_timestamp".to_string(),
            )))
            .unwrap(),
        ];
        let column_defaults = collect_column_defaults(&column_schemas[..]);

        assert!(!column_defaults.contains_key("col1"));
        assert!(matches!(column_defaults.get("col2").unwrap(),
                         Expr::Literal(ScalarValue::Utf8(Some(s)), _) if s == "test"));
        assert!(matches!(
            column_defaults.get("ts").unwrap(),
            Expr::Cast(Cast {
                expr,
                data_type
            }) if **expr == *NOW_EXPR && *data_type == ConcreteDataType::timestamp_millisecond_datatype().as_arrow_type()
        ));
    }
}
