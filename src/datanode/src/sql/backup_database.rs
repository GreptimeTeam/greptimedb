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

use common_query::logical_plan::DfExpr;
use common_query::prelude::Expr;
use common_query::Output;
use common_time::range::TimestampRange;
use common_time::timestamp::TimeUnit;
use common_time::Timestamp;
use datafusion_common::{Column, ScalarValue};
use datafusion_expr::{and, binary_expr, Operator};
use snafu::{OptionExt, ResultExt};
use table::engine::TableReference;
use table::requests::BackupDatabaseRequest;

use crate::error;
use crate::error::{CatalogNotFoundSnafu, CatalogSnafu, SchemaNotFoundSnafu};
use crate::sql::SqlHandler;

impl SqlHandler {
    pub(crate) async fn backup_database(
        &self,
        req: BackupDatabaseRequest,
    ) -> error::Result<Output> {
        let schema = self
            .catalog_manager
            .catalog(&req.catalog_name)
            .context(CatalogSnafu)?
            .context(CatalogNotFoundSnafu {
                name: &req.catalog_name,
            })?
            .schema(&req.schema_name)
            .context(CatalogSnafu)?
            .context(SchemaNotFoundSnafu {
                name: &req.schema_name,
            })?;

        let table_names = schema.table_names().context(CatalogSnafu)?;
        for table_name in &table_names {
            let table_ref = TableReference {
                catalog: &req.catalog_name,
                schema: &req.schema_name,
                table: table_name,
            };
            let table = self.get_table(&table_ref)?;
            let table_schema = table.schema();
            // TODO(hl): remove this unwrap
            let ts_col = table_schema.timestamp_column().unwrap();
            let time_filter = build_time_range_filter(&ts_col.name, req.time_range.as_ref())
                .unwrap()
                .map(|e| vec![e])
                .unwrap_or_default();
            let table_scan = table.scan(None, &time_filter, None).await;
        }

        todo!()
    }
}

fn build_time_range_filter(
    ts_col_name: &str,
    time_range: Option<&TimestampRange>,
) -> error::Result<Option<Expr>> {
    let Some(time_range) = time_range else { return Ok(None) };
    if time_range.is_empty() {
        // TODO(hl): don't panic here
        panic!("Timestamp range cannot be empty");
    }

    let ts_col_expr = DfExpr::Column(Column {
        relation: None,
        name: ts_col_name.to_string(),
    });

    let df_expr = match (time_range.start(), time_range.end()) {
        (None, None) => None,
        (Some(start), None) => Some(binary_expr(
            ts_col_expr,
            Operator::GtEq,
            timestamp_to_df_expr(start),
        )),
        (None, Some(end)) => Some(binary_expr(
            ts_col_expr,
            Operator::Lt,
            timestamp_to_df_expr(end),
        )),

        (Some(start), Some(end)) => Some(and(
            binary_expr(
                ts_col_expr.clone(),
                Operator::GtEq,
                timestamp_to_df_expr(start),
            ),
            binary_expr(ts_col_expr, Operator::Lt, timestamp_to_df_expr(end)),
        )),
    };

    Ok(df_expr.map(Expr::from))
}

fn timestamp_to_df_expr(timestamp: &Timestamp) -> DfExpr {
    let scalar_value = match timestamp.unit() {
        TimeUnit::Second => ScalarValue::TimestampSecond(Some(timestamp.value()), None),
        TimeUnit::Millisecond => ScalarValue::TimestampMillisecond(Some(timestamp.value()), None),
        TimeUnit::Microsecond => ScalarValue::TimestampMicrosecond(Some(timestamp.value()), None),
        TimeUnit::Nanosecond => ScalarValue::TimestampNanosecond(Some(timestamp.value()), None),
    };
    DfExpr::Literal(scalar_value)
}
