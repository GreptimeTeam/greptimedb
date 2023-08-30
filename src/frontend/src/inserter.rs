use api::v1::alter_expr::Kind;
use api::v1::ddl_request::Expr as DdlExpr;
use api::v1::greptime_request::Request;
use api::v1::{
    AddColumns, AlterExpr, Column, DdlRequest, InsertRequest, InsertRequests, RowInsertRequests,
};
use catalog::CatalogManagerRef;
use common_catalog::consts::default_engine;
use common_query::Output;
use common_telemetry::{debug, info};
use datatypes::schema::Schema;
use servers::query_handler::grpc::GrpcQueryHandlerRef;
use session::context::QueryContextRef;
use snafu::prelude::*;
use table::engine::TableReference;

use crate::error::{self, CatalogSnafu, Error, InvalidInsertRequestSnafu, Result};
use crate::expr_factory::CreateExprFactory;

pub(crate) struct Inserter<'a> {
    catalog_manager: &'a CatalogManagerRef,
    create_expr_factory: &'a CreateExprFactory,
    grpc_query_handler: &'a GrpcQueryHandlerRef<Error>,
}

impl<'a> Inserter<'a> {
    pub fn new(
        catalog_manager: &'a CatalogManagerRef,
        create_expr_factory: &'a CreateExprFactory,
        grpc_query_handler: &'a GrpcQueryHandlerRef<Error>,
    ) -> Self {
        Self {
            catalog_manager,
            create_expr_factory,
            grpc_query_handler,
        }
    }

    pub async fn handle_inserts(
        &self,
        requests: InsertRequests,
        ctx: QueryContextRef,
    ) -> Result<Output> {
        for req in requests.inserts.iter() {
            self.create_or_alter_table_on_demand(ctx.clone(), req)
                .await?;
        }

        unimplemented!()
    }

    pub async fn handle_row_inserts(
        &self,
        requests: RowInsertRequests,
        ctx: QueryContextRef,
    ) -> Result<Output> {
        unimplemented!()
    }
}

impl<'a> Inserter<'a> {
    // check if table already exist:
    // - if table does not exist, create table by inferred CreateExpr
    // - if table exist, check if schema matches. If any new column found, alter table by inferred `AlterExpr`
    async fn create_or_alter_table_on_demand(
        &self,
        ctx: QueryContextRef,
        request: &InsertRequest,
    ) -> Result<()> {
        let catalog_name = &ctx.current_catalog().to_owned();
        let schema_name = &ctx.current_schema().to_owned();
        let table_name = &request.table_name;
        let columns = &request.columns;

        let table = self
            .catalog_manager
            .table(catalog_name, schema_name, table_name)
            .await
            .context(CatalogSnafu)?;
        match table {
            None => {
                info!(
                    "Table {}.{}.{} does not exist, try create table",
                    catalog_name, schema_name, table_name,
                );
                let _ = self
                    .create_table_by_columns(ctx, table_name, columns, default_engine())
                    .await?;
                info!(
                    "Successfully created table on insertion: {}.{}.{}",
                    catalog_name, schema_name, table_name
                );
            }
            Some(table) => {
                let schema = table.schema();

                validate_insert_request(schema.as_ref(), request)?;

                if let Some(add_columns) = common_grpc_expr::find_new_columns(&schema, columns)
                    .context(error::FindNewColumnsOnInsertionSnafu)?
                {
                    info!(
                        "Find new columns {:?} on insertion, try to alter table: {}.{}.{}",
                        add_columns, catalog_name, schema_name, table_name
                    );
                    let _ = self
                        .add_new_columns_to_table(ctx, table_name, add_columns)
                        .await?;
                    info!(
                        "Successfully altered table on insertion: {}.{}.{}",
                        catalog_name, schema_name, table_name
                    );
                }
            }
        };
        Ok(())
    }

    /// Infer create table expr from inserting data
    async fn create_table_by_columns(
        &self,
        ctx: QueryContextRef,
        table_name: &str,
        columns: &[Column],
        engine: &str,
    ) -> Result<Output> {
        let catalog_name = ctx.current_catalog();
        let schema_name = ctx.current_schema();

        // Create table automatically, build schema from data.
        let table_name = TableReference::full(catalog_name, schema_name, table_name);
        let create_expr =
            self.create_expr_factory
                .create_table_expr_by_columns(&table_name, columns, engine)?;

        info!(
            "Try to create table: {} automatically with request: {:?}",
            table_name, create_expr,
        );

        self.grpc_query_handler
            .do_query(
                Request::Ddl(DdlRequest {
                    expr: Some(DdlExpr::CreateTable(create_expr)),
                }),
                ctx,
            )
            .await
    }

    async fn add_new_columns_to_table(
        &self,
        ctx: QueryContextRef,
        table_name: &str,
        add_columns: AddColumns,
    ) -> Result<Output> {
        debug!(
            "Adding new columns: {:?} to table: {}",
            add_columns, table_name
        );
        let expr = AlterExpr {
            catalog_name: ctx.current_catalog().to_owned(),
            schema_name: ctx.current_schema().to_owned(),
            table_name: table_name.to_string(),
            kind: Some(Kind::AddColumns(add_columns)),
            ..Default::default()
        };

        self.grpc_query_handler
            .do_query(
                Request::Ddl(DdlRequest {
                    expr: Some(DdlExpr::Alter(expr)),
                }),
                ctx,
            )
            .await
    }
}

fn validate_insert_request(schema: &Schema, request: &InsertRequest) -> Result<()> {
    for column_schema in schema.column_schemas() {
        if column_schema.is_nullable() || column_schema.default_constraint().is_some() {
            continue;
        }
        let not_null = request
            .columns
            .iter()
            .find(|x| x.column_name == column_schema.name)
            .map(|column| column.null_mask.is_empty() || column.null_mask.iter().all(|x| *x == 0));
        ensure!(
            not_null == Some(true),
            InvalidInsertRequestSnafu {
                reason: format!(
                    "Expecting insert data to be presented on a not null or no default value column '{}'.",
                    &column_schema.name
                )
            }
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use api::v1::column::Values;
    use datatypes::prelude::{ConcreteDataType, Value};
    use datatypes::schema::{ColumnDefaultConstraint, ColumnSchema};

    use super::*;

    #[test]
    fn test_validate_insert_request() {
        let schema = Schema::new(vec![
            ColumnSchema::new("a", ConcreteDataType::int32_datatype(), true)
                .with_default_constraint(None)
                .unwrap(),
            ColumnSchema::new("b", ConcreteDataType::int32_datatype(), true)
                .with_default_constraint(Some(ColumnDefaultConstraint::Value(Value::Int32(100))))
                .unwrap(),
        ]);
        let request = InsertRequest {
            columns: vec![Column {
                column_name: "c".to_string(),
                values: Some(Values {
                    i32_values: vec![1],
                    ..Default::default()
                }),
                null_mask: vec![0],
                ..Default::default()
            }],
            ..Default::default()
        };
        // If nullable is true, it doesn't matter whether the insert request has the column.
        validate_insert_request(&schema, &request).unwrap();

        let schema = Schema::new(vec![
            ColumnSchema::new("a", ConcreteDataType::int32_datatype(), false)
                .with_default_constraint(None)
                .unwrap(),
            ColumnSchema::new("b", ConcreteDataType::int32_datatype(), false)
                .with_default_constraint(Some(ColumnDefaultConstraint::Value(Value::Int32(-100))))
                .unwrap(),
        ]);
        let request = InsertRequest {
            columns: vec![Column {
                column_name: "a".to_string(),
                values: Some(Values {
                    i32_values: vec![1],
                    ..Default::default()
                }),
                null_mask: vec![0],
                ..Default::default()
            }],
            ..Default::default()
        };
        // If nullable is false, but the column is defined with default value,
        // it also doesn't matter whether the insert request has the column.
        validate_insert_request(&schema, &request).unwrap();

        let request = InsertRequest {
            columns: vec![Column {
                column_name: "b".to_string(),
                values: Some(Values {
                    i32_values: vec![1],
                    ..Default::default()
                }),
                null_mask: vec![0],
                ..Default::default()
            }],
            ..Default::default()
        };
        // Neither of the above cases.
        assert!(validate_insert_request(&schema, &request).is_err());
    }
}
