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

use api::v1::alter_expr::Kind;
use api::v1::ddl_request::Expr;
use api::v1::greptime_request::Request;
use api::v1::{AlterExpr, ColumnSchema, DdlRequest, Row, RowInsertRequest, RowInsertRequests};
use catalog::CatalogManagerRef;
use common_grpc_expr::{extract_new_columns, ColumnExpr};
use common_query::Output;
use common_telemetry::info;
use datatypes::schema::Schema;
use servers::query_handler::grpc::GrpcQueryHandlerRef;
use session::context::QueryContextRef;
use snafu::{ensure, OptionExt, ResultExt};
use table::TableRef;

use crate::error::{
    CatalogSnafu, EmptyDataSnafu, Error, FindNewColumnsOnInsertionSnafu, InvalidInsertRequestSnafu,
    Result,
};
use crate::expr_factory::CreateExprFactoryRef;

pub struct RowInserter {
    engine_name: String,
    catalog_manager: CatalogManagerRef,
    create_expr_factory: CreateExprFactoryRef,
    grpc_query_handler: GrpcQueryHandlerRef<Error>,
}

impl RowInserter {
    pub fn new(
        engine_name: String,
        catalog_manager: CatalogManagerRef,
        create_expr_factory: CreateExprFactoryRef,
        grpc_query_handler: GrpcQueryHandlerRef<Error>,
    ) -> Self {
        Self {
            engine_name,
            catalog_manager,
            create_expr_factory,
            grpc_query_handler,
        }
    }

    pub async fn handle_inserts(
        &self,
        requests: RowInsertRequests,
        ctx: QueryContextRef,
    ) -> Result<Output> {
        self.create_or_alter_tables_on_demand(&requests, ctx.clone())
            .await?;
        let query = Request::RowInserts(requests);
        self.grpc_query_handler.do_query(query, ctx).await
    }

    // check if tables already exist:
    // - if table does not exist, create table by inferred CreateExpr
    // - if table exist, check if schema matches. If any new column found, alter table by inferred `AlterExpr`
    async fn create_or_alter_tables_on_demand(
        &self,
        requests: &RowInsertRequests,
        ctx: QueryContextRef,
    ) -> Result<()> {
        let catalog_name = ctx.current_catalog();
        let schema_name = ctx.current_schema();

        let mut not_exist_tables = Vec::new();
        let mut tables = Vec::new();
        for req in &requests.inserts {
            let table_name = &req.table_name;
            // TODO(jeremy): get tables in batch?
            let table = self
                .catalog_manager
                .table(&catalog_name, &schema_name, table_name)
                .await
                .context(CatalogSnafu)?;
            match table {
                Some(table) => tables.push((table, req)),
                None => not_exist_tables.push((table_name.clone(), req)),
            }
        }

        if !not_exist_tables.is_empty() {
            self.create_tables(&catalog_name, &schema_name, not_exist_tables, ctx.clone())
                .await?;
        }

        if !tables.is_empty() {
            self.alter_tables_on_demand(&catalog_name, &schema_name, tables, ctx.clone())
                .await?;
        }

        Ok(())
    }

    async fn create_tables(
        &self,
        catalog_name: &str,
        schema_name: &str,
        tables: Vec<(String, &RowInsertRequest)>,
        ctx: QueryContextRef,
    ) -> Result<()> {
        let mut create_table_exprs = Vec::with_capacity(tables.len());
        for (table_name, req) in tables {
            let (rows_schema, _) = Self::extract_schema_and_rows(req)?;
            let column_exprs = ColumnExpr::from_column_schemas(rows_schema);
            let create_table_expr = self.create_expr_factory.create_table_expr(
                catalog_name,
                schema_name,
                &table_name,
                column_exprs,
                &self.engine_name,
            )?;
            create_table_exprs.push(create_table_expr);
        }

        // TODO(jeremy): create tables in batch
        for create_table_expr in create_table_exprs {
            let req = Request::Ddl(DdlRequest {
                expr: Some(Expr::CreateTable(create_table_expr)),
            });
            self.grpc_query_handler.do_query(req, ctx.clone()).await?;
        }

        Ok(())
    }

    async fn alter_tables_on_demand(
        &self,
        catalog_name: &str,
        schema_name: &str,
        tables: Vec<(TableRef, &RowInsertRequest)>,
        ctx: QueryContextRef,
    ) -> Result<()> {
        let mut alter_table_exprs = Vec::new();
        for (table, req) in &tables {
            let (rows_schema, _) = Self::validate_insert_request(&table.schema(), req)?;
            let column_exprs = ColumnExpr::from_column_schemas(rows_schema);
            let add_columns = extract_new_columns(&table.schema(), column_exprs)
                .context(FindNewColumnsOnInsertionSnafu)?;
            let Some(add_columns) = add_columns else {
                continue;
            };
            let table_name = table.table_info().name.clone();

            info!(
                "Adding new columns: {:?} to table: {}.{}.{}",
                add_columns, catalog_name, schema_name, table_name
            );

            alter_table_exprs.push(AlterExpr {
                catalog_name: catalog_name.to_string(),
                schema_name: schema_name.to_string(),
                table_name,
                kind: Some(Kind::AddColumns(add_columns)),
                ..Default::default()
            });
        }

        // TODO(jeremy): alter tables in batch
        for alter_table_expr in alter_table_exprs {
            let req = Request::Ddl(DdlRequest {
                expr: Some(Expr::Alter(alter_table_expr)),
            });
            self.grpc_query_handler.do_query(req, ctx.clone()).await?;
        }

        Ok(())
    }

    fn validate_insert_request<'a>(
        schema: &Schema,
        req: &'a RowInsertRequest,
    ) -> Result<(&'a [ColumnSchema], &'a [Row])> {
        let (rows_schema, rows) = Self::extract_schema_and_rows(req)?;
        let mut not_nulls = schema
            .column_schemas()
            .iter()
            .filter(|column_schema| {
                !column_schema.is_nullable() && column_schema.default_constraint().is_none()
            })
            .map(|column_schema| (&column_schema.name, rows.len()))
            .collect::<HashMap<_, _>>();

        for row in rows {
            ensure!(
                row.values.len() == rows_schema.len(),
                InvalidInsertRequestSnafu {
                    reason: format!(
                        "insert to table: {:?}, expected column count: {}, got: {}",
                        &req.table_name,
                        rows_schema.len(),
                        row.values.len()
                    )
                }
            );

            for (i, value) in row.values.iter().enumerate() {
                let column_name = &rows_schema[i].column_name;
                let Some(not_null) = not_nulls.get_mut(column_name) else {
                    continue;
                };
                let _ = value
                    .value_data
                    .as_ref()
                    .context(InvalidInsertRequestSnafu {
                        reason: format!(
                            "insert to table: {:?}, column: {:?}, expected not null",
                            &req.table_name, column_name,
                        ),
                    })?;
                *not_null -= 1;
            }
        }

        for (column_name, count) in not_nulls {
            ensure!(
                count == 0,
                InvalidInsertRequestSnafu {
                    reason: format!(
                        "insert to table: {:?}, column: {:?}, expected not null",
                        &req.table_name, column_name,
                    ),
                }
            );
        }

        Ok((rows_schema, rows))
    }

    fn extract_schema_and_rows(req: &RowInsertRequest) -> Result<(&[ColumnSchema], &[Row])> {
        let rows = req.rows.as_ref().context(EmptyDataSnafu {
            msg: format!("insert to table: {:?}", &req.table_name),
        })?;
        let schema = &rows.schema;
        let rows = &rows.rows;

        ensure!(
            !rows.is_empty(),
            EmptyDataSnafu {
                msg: format!("insert to table: {:?}", &req.table_name),
            }
        );

        Ok((schema, rows))
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;

    use api::v1::value::ValueData;
    use api::v1::{
        ColumnDataType as RpcColumnDataType, ColumnSchema as RpcColumnSchema, Row, Rows,
        SemanticType, Value as RpcValue,
    };
    use datatypes::prelude::*;
    use datatypes::schema::{ColumnDefaultConstraint, ColumnSchema};

    use super::*;

    fn gen_column_schema(
        name: &str,
        is_nullable: bool,
        default_constraint: Option<ColumnDefaultConstraint>,
    ) -> ColumnSchema {
        ColumnSchema::new(name, ConcreteDataType::int32_datatype(), is_nullable)
            .with_default_constraint(default_constraint)
            .unwrap()
    }

    fn gen_rpc_column_schema(name: &str, semantic_type: SemanticType) -> RpcColumnSchema {
        RpcColumnSchema {
            column_name: name.to_string(),
            datatype: RpcColumnDataType::Int32 as i32,
            semantic_type: semantic_type as i32,
        }
    }

    fn create_req(schema: Vec<RpcColumnSchema>, rows: Vec<Row>) -> RowInsertRequest {
        RowInsertRequest {
            table_name: "test_table".to_string(),
            rows: Some(Rows { schema, rows }),
            region_number: 0,
        }
    }

    #[test]
    fn test_validate_insert_request() {
        let schema = Schema::new(vec![
            gen_column_schema("col_0", true, None),
            gen_column_schema("col_1", false, None),
            gen_column_schema(
                "col_2",
                false,
                Some(ColumnDefaultConstraint::Value(Value::Int32(100))),
            ),
        ]);

        let data_schema = vec![
            gen_rpc_column_schema("col_0", SemanticType::Tag),
            gen_rpc_column_schema("col_1", SemanticType::Field),
            gen_rpc_column_schema("col_2", SemanticType::Field),
        ];

        // case 1
        let rows = vec![];
        let req = create_req(data_schema.clone(), rows);
        let res = RowInserter::validate_insert_request(&schema, &req);
        assert_matches!(res, Err(Error::EmptyData { .. }));

        // case 2
        let rows = vec![Row {
            values: vec![
                RpcValue {
                    value_data: Some(ValueData::I32Value(1)),
                },
                RpcValue { value_data: None },
                RpcValue {
                    value_data: Some(ValueData::I32Value(1)),
                },
            ],
        }];
        let req = create_req(data_schema.clone(), rows);
        let res = RowInserter::validate_insert_request(&schema, &req);
        assert_matches!(res, Err(Error::InvalidInsertRequest { .. }));

        // case 3
        let rows = vec![Row {
            values: vec![
                RpcValue {
                    value_data: Some(ValueData::I32Value(1)),
                },
                RpcValue {
                    value_data: Some(ValueData::I32Value(1)),
                },
            ],
        }];
        let req = create_req(data_schema.clone(), rows);
        let res = RowInserter::validate_insert_request(&schema, &req);
        assert_matches!(res, Err(Error::InvalidInsertRequest { .. }));

        // case 4
        let rows = vec![Row {
            values: vec![
                RpcValue { value_data: None },
                RpcValue {
                    value_data: Some(ValueData::I32Value(1)),
                },
                RpcValue {
                    value_data: Some(ValueData::I32Value(1)),
                },
            ],
        }];
        let req = create_req(data_schema.clone(), rows);
        let res = RowInserter::validate_insert_request(&schema, &req);
        assert_matches!(res, Ok(_));

        // case 5
        let rows = vec![Row {
            values: vec![
                RpcValue { value_data: None },
                RpcValue {
                    value_data: Some(ValueData::I32Value(1)),
                },
                RpcValue { value_data: None },
            ],
        }];
        let req = create_req(data_schema.clone(), rows);
        let res = RowInserter::validate_insert_request(&schema, &req);
        assert_matches!(res, Ok(_));

        // case 6
        let rows = vec![
            Row {
                values: vec![
                    RpcValue { value_data: None },
                    RpcValue {
                        value_data: Some(ValueData::I32Value(1)),
                    },
                    RpcValue { value_data: None },
                ],
            },
            Row {
                values: vec![
                    RpcValue {
                        value_data: Some(ValueData::I32Value(1)),
                    },
                    RpcValue {
                        value_data: Some(ValueData::I32Value(1)),
                    },
                ],
            },
        ];
        let req = create_req(data_schema.clone(), rows);
        let res = RowInserter::validate_insert_request(&schema, &req);
        assert_matches!(res, Err(Error::InvalidInsertRequest { .. }));

        // case 7
        let rows = vec![
            Row {
                values: vec![
                    RpcValue { value_data: None },
                    RpcValue {
                        value_data: Some(ValueData::I32Value(1)),
                    },
                    RpcValue {
                        value_data: Some(ValueData::I32Value(1)),
                    },
                ],
            },
            Row {
                values: vec![
                    RpcValue { value_data: None },
                    RpcValue {
                        value_data: Some(ValueData::I32Value(1)),
                    },
                    RpcValue { value_data: None },
                ],
            },
        ];
        let req = create_req(data_schema, rows);
        let res = RowInserter::validate_insert_request(&schema, &req);
        assert_matches!(res, Ok(_));
    }
}
