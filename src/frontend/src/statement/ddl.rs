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
use std::sync::Arc;

use api::helper::ColumnDataTypeWrapper;
use api::v1::{column_def, AlterExpr, CreateTableExpr, TruncateTableExpr};
use catalog::CatalogManagerRef;
use chrono::DateTime;
use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
use common_catalog::format_full_table_name;
use common_meta::cache_invalidator::Context;
use common_meta::ddl::ExecutorContext;
use common_meta::ident::TableIdent;
use common_meta::key::schema_name::{SchemaNameKey, SchemaNameValue};
use common_meta::rpc::ddl::{DdlTask, SubmitDdlTaskRequest, SubmitDdlTaskResponse};
use common_meta::rpc::router::{Partition, Partition as MetaPartition};
use common_meta::table_name::TableName;
use common_query::Output;
use common_telemetry::info;
use datatypes::prelude::ConcreteDataType;
use datatypes::schema::RawSchema;
use partition::partition::{PartitionBound, PartitionDef};
use session::context::QueryContextRef;
use snafu::{OptionExt, ResultExt};
use sql::ast::Value as SqlValue;
use sql::statements::alter::AlterTable;
use sql::statements::create::{CreateExternalTable, CreateTable, Partitions};
use sql::statements::sql_value_to_value;
use table::metadata::{self, RawTableInfo, RawTableMeta, TableId, TableInfo, TableType};
use table::requests::{AlterTableRequest, TableOptions};
use table::TableRef;

use super::StatementExecutor;
use crate::error::{
    self, AlterExprToRequestSnafu, CatalogSnafu, ColumnDataTypeSnafu, ColumnNotFoundSnafu,
    DeserializePartitionSnafu, ParseSqlSnafu, Result, SchemaNotFoundSnafu,
    TableMetadataManagerSnafu, TableNotFoundSnafu, UnrecognizedTableOptionSnafu,
};
use crate::table::DistTable;
use crate::{expr_factory, MAX_VALUE};

impl StatementExecutor {
    pub fn catalog_manager(&self) -> CatalogManagerRef {
        self.catalog_manager.clone()
    }

    pub async fn create_table(&self, stmt: CreateTable, ctx: QueryContextRef) -> Result<TableRef> {
        let create_expr = &mut expr_factory::create_to_expr(&stmt, ctx)?;
        self.create_table_inner(create_expr, stmt.partitions).await
    }

    pub async fn create_external_table(
        &self,
        create_expr: CreateExternalTable,
        ctx: QueryContextRef,
    ) -> Result<TableRef> {
        let create_expr = &mut expr_factory::create_external_expr(create_expr, ctx).await?;
        self.create_table_inner(create_expr, None).await
    }

    pub async fn create_table_inner(
        &self,
        create_table: &mut CreateTableExpr,
        partitions: Option<Partitions>,
    ) -> Result<TableRef> {
        let _timer = common_telemetry::timer!(crate::metrics::DIST_CREATE_TABLE);
        let schema = self
            .table_metadata_manager
            .schema_manager()
            .get(SchemaNameKey::new(
                &create_table.catalog_name,
                &create_table.schema_name,
            ))
            .await
            .context(TableMetadataManagerSnafu)?;

        let Some(schema_opts) = schema else {
            return SchemaNotFoundSnafu {
                schema_info: &create_table.schema_name,
            }
            .fail();
        };

        let table_name = TableName::new(
            &create_table.catalog_name,
            &create_table.schema_name,
            &create_table.table_name,
        );

        let (partitions, partition_cols) = parse_partitions(create_table, partitions)?;

        let mut table_info = create_table_info(create_table, partition_cols, schema_opts)?;

        let resp = self
            .create_table_procedure(create_table, partitions, table_info.clone())
            .await?;

        let table_id = resp.table_id.context(error::UnexpectedSnafu {
            violated: "expected table_id",
        })?;
        info!("Successfully created distributed table '{table_name}' with table id {table_id}");

        table_info.ident.table_id = table_id;
        let engine = table_info.meta.engine.to_string();

        let table_info = Arc::new(table_info.try_into().context(error::CreateTableInfoSnafu)?);
        create_table.table_id = Some(api::v1::TableId { id: table_id });

        let table = DistTable::table(table_info);

        // Invalidates local cache ASAP.
        self.cache_invalidator
            .invalidate_table(
                &Context::default(),
                TableIdent {
                    catalog: table_name.catalog_name.to_string(),
                    schema: table_name.schema_name.to_string(),
                    table: table_name.table_name.to_string(),
                    table_id,
                    engine,
                },
            )
            .await
            .context(error::InvalidateTableCacheSnafu)?;

        Ok(table)
    }

    pub async fn drop_table(&self, table_name: TableName) -> Result<Output> {
        let table = self
            .catalog_manager
            .table(
                &table_name.catalog_name,
                &table_name.schema_name,
                &table_name.table_name,
            )
            .await
            .context(CatalogSnafu)?
            .with_context(|| TableNotFoundSnafu {
                table_name: table_name.to_string(),
            })?;
        let table_id = table.table_info().table_id();
        let engine = table.table_info().meta.engine.to_string();
        self.drop_table_procedure(&table_name, table_id).await?;

        // Invalidates local cache ASAP.
        self.cache_invalidator
            .invalidate_table(
                &Context::default(),
                TableIdent {
                    catalog: table_name.catalog_name.to_string(),
                    schema: table_name.schema_name.to_string(),
                    table: table_name.table_name.to_string(),
                    table_id,
                    engine,
                },
            )
            .await
            .context(error::InvalidateTableCacheSnafu)?;

        Ok(Output::AffectedRows(1))
    }

    pub async fn truncate_table(&self, table_name: TableName) -> Result<Output> {
        let table = self
            .catalog_manager
            .table(
                &table_name.catalog_name,
                &table_name.schema_name,
                &table_name.table_name,
            )
            .await
            .context(CatalogSnafu)?
            .with_context(|| TableNotFoundSnafu {
                table_name: table_name.to_string(),
            })?;
        let table_id = table.table_info().ident.table_id;

        let expr = TruncateTableExpr {
            catalog_name: table_name.catalog_name.clone(),
            schema_name: table_name.schema_name.clone(),
            table_name: table_name.table_name.clone(),
            table_id: Some(api::v1::TableId { id: table_id }),
        };
        self.truncate_table_procedure(&expr).await?;

        Ok(Output::AffectedRows(0))
    }

    fn verify_alter(
        &self,
        table_id: TableId,
        table_info: Arc<TableInfo>,
        expr: AlterExpr,
    ) -> Result<()> {
        let request: table::requests::AlterTableRequest =
            common_grpc_expr::alter_expr_to_request(table_id, expr)
                .context(AlterExprToRequestSnafu)?;

        let AlterTableRequest { table_name, .. } = &request;

        let _ = table_info
            .meta
            .builder_with_alter_kind(table_name, &request.alter_kind)
            .context(error::TableSnafu)?
            .build()
            .context(error::BuildTableMetaSnafu { table_name })?;

        Ok(())
    }

    pub async fn alter_table(
        &self,
        alter_table: AlterTable,
        query_ctx: QueryContextRef,
    ) -> Result<Output> {
        let expr = expr_factory::to_alter_expr(alter_table, query_ctx)?;
        self.alter_table_inner(expr).await
    }

    pub async fn alter_table_inner(&self, expr: AlterExpr) -> Result<Output> {
        let catalog_name = if expr.catalog_name.is_empty() {
            DEFAULT_CATALOG_NAME
        } else {
            expr.catalog_name.as_str()
        };

        let schema_name = if expr.schema_name.is_empty() {
            DEFAULT_SCHEMA_NAME
        } else {
            expr.schema_name.as_str()
        };

        let table_name = expr.table_name.as_str();

        let table = self
            .catalog_manager
            .table(catalog_name, schema_name, table_name)
            .await
            .context(CatalogSnafu)?
            .with_context(|| TableNotFoundSnafu {
                table_name: format_full_table_name(catalog_name, schema_name, table_name),
            })?;

        let table_id = table.table_info().ident.table_id;
        let engine = table.table_info().meta.engine.to_string();
        self.verify_alter(table_id, table.table_info(), expr.clone())?;

        let req = SubmitDdlTaskRequest {
            task: DdlTask::new_alter_table(expr.clone()),
        };

        self.ddl_executor
            .submit_ddl_task(&ExecutorContext::default(), req)
            .await
            .context(error::ExecuteDdlSnafu)?;

        // Invalidates local cache ASAP.
        self.cache_invalidator
            .invalidate_table(
                &Context::default(),
                TableIdent {
                    catalog: catalog_name.to_string(),
                    schema: schema_name.to_string(),
                    table: table_name.to_string(),
                    table_id,
                    engine,
                },
            )
            .await
            .context(error::InvalidateTableCacheSnafu)?;

        Ok(Output::AffectedRows(0))
    }

    async fn create_table_procedure(
        &self,
        create_table: &CreateTableExpr,
        partitions: Vec<Partition>,
        table_info: RawTableInfo,
    ) -> Result<SubmitDdlTaskResponse> {
        let partitions = partitions.into_iter().map(Into::into).collect();

        let request = SubmitDdlTaskRequest {
            task: DdlTask::new_create_table(create_table.clone(), partitions, table_info),
        };

        self.ddl_executor
            .submit_ddl_task(&ExecutorContext::default(), request)
            .await
            .context(error::ExecuteDdlSnafu)
    }

    async fn drop_table_procedure(
        &self,
        table_name: &TableName,
        table_id: TableId,
    ) -> Result<SubmitDdlTaskResponse> {
        let request = SubmitDdlTaskRequest {
            task: DdlTask::new_drop_table(
                table_name.catalog_name.to_string(),
                table_name.schema_name.to_string(),
                table_name.table_name.to_string(),
                table_id,
            ),
        };

        self.ddl_executor
            .submit_ddl_task(&ExecutorContext::default(), request)
            .await
            .context(error::ExecuteDdlSnafu)
    }

    async fn truncate_table_procedure(
        &self,
        truncate_table: &TruncateTableExpr,
    ) -> Result<SubmitDdlTaskResponse> {
        let request = SubmitDdlTaskRequest {
            task: DdlTask::new_truncate_table(truncate_table.clone()),
        };

        self.ddl_executor
            .submit_ddl_task(&ExecutorContext::default(), request)
            .await
            .context(error::ExecuteDdlSnafu)
    }

    pub async fn create_database(
        &self,
        catalog: &str,
        database: &str,
        create_if_not_exists: bool,
    ) -> Result<Output> {
        // TODO(weny): considers executing it in the procedures.
        let schema_key = SchemaNameKey::new(catalog, database);
        let exists = self
            .table_metadata_manager
            .schema_manager()
            .exists(schema_key)
            .await
            .context(TableMetadataManagerSnafu)?;

        if exists {
            return if create_if_not_exists {
                Ok(Output::AffectedRows(1))
            } else {
                error::SchemaExistsSnafu { name: database }.fail()
            };
        }

        self.table_metadata_manager
            .schema_manager()
            .create(schema_key, None, false)
            .await
            .context(TableMetadataManagerSnafu)?;

        Ok(Output::AffectedRows(1))
    }
}

fn parse_partitions(
    create_table: &CreateTableExpr,
    partitions: Option<Partitions>,
) -> Result<(Vec<MetaPartition>, Vec<String>)> {
    // If partitions are not defined by user, use the timestamp column (which has to be existed) as
    // the partition column, and create only one partition.
    let partition_columns = find_partition_columns(&partitions)?;
    let partition_entries = find_partition_entries(create_table, &partitions, &partition_columns)?;

    Ok((
        partition_entries
            .into_iter()
            .map(|x| MetaPartition::try_from(PartitionDef::new(partition_columns.clone(), x)))
            .collect::<std::result::Result<_, _>>()
            .context(DeserializePartitionSnafu)?,
        partition_columns,
    ))
}

fn create_table_info(
    create_table: &CreateTableExpr,
    partition_columns: Vec<String>,
    schema_opts: SchemaNameValue,
) -> Result<RawTableInfo> {
    let mut column_schemas = Vec::with_capacity(create_table.column_defs.len());
    let mut column_name_to_index_map = HashMap::new();

    for (idx, column) in create_table.column_defs.iter().enumerate() {
        let schema =
            column_def::try_as_column_schema(column).context(error::InvalidColumnDefSnafu {
                column: &column.name,
            })?;
        let schema = schema.with_time_index(column.name == create_table.time_index);

        column_schemas.push(schema);
        let _ = column_name_to_index_map.insert(column.name.clone(), idx);
    }

    let timestamp_index = column_name_to_index_map
        .get(&create_table.time_index)
        .cloned();

    let raw_schema = RawSchema {
        column_schemas: column_schemas.clone(),
        timestamp_index,
        version: 0,
    };

    let primary_key_indices = create_table
        .primary_keys
        .iter()
        .map(|name| {
            column_name_to_index_map
                .get(name)
                .cloned()
                .context(ColumnNotFoundSnafu { msg: name })
        })
        .collect::<Result<Vec<_>>>()?;

    let partition_key_indices = partition_columns
        .into_iter()
        .map(|col_name| {
            column_name_to_index_map
                .get(&col_name)
                .cloned()
                .context(ColumnNotFoundSnafu { msg: col_name })
        })
        .collect::<Result<Vec<_>>>()?;

    let table_options = TableOptions::try_from(&create_table.table_options)
        .context(UnrecognizedTableOptionSnafu)?;
    let table_options = merge_options(table_options, schema_opts);

    let meta = RawTableMeta {
        schema: raw_schema,
        primary_key_indices,
        value_indices: vec![],
        engine: create_table.engine.clone(),
        next_column_id: column_schemas.len() as u32,
        region_numbers: vec![],
        engine_options: HashMap::new(),
        options: table_options,
        created_on: DateTime::default(),
        partition_key_indices,
    };

    let desc = if create_table.desc.is_empty() {
        None
    } else {
        Some(create_table.desc.clone())
    };

    let table_info = RawTableInfo {
        ident: metadata::TableIdent {
            // The table id of distributed table is assigned by Meta, set "0" here as a placeholder.
            table_id: 0,
            version: 0,
        },
        name: create_table.table_name.clone(),
        desc,
        catalog_name: create_table.catalog_name.clone(),
        schema_name: create_table.schema_name.clone(),
        meta,
        table_type: TableType::Base,
    };
    Ok(table_info)
}

fn find_partition_columns(partitions: &Option<Partitions>) -> Result<Vec<String>> {
    let columns = if let Some(partitions) = partitions {
        partitions
            .column_list
            .iter()
            .map(|x| x.value.clone())
            .collect::<Vec<_>>()
    } else {
        vec![]
    };
    Ok(columns)
}

fn find_partition_entries(
    create_table: &CreateTableExpr,
    partitions: &Option<Partitions>,
    partition_columns: &[String],
) -> Result<Vec<Vec<PartitionBound>>> {
    let entries = if let Some(partitions) = partitions {
        let column_defs = partition_columns
            .iter()
            .map(|pc| {
                create_table
                    .column_defs
                    .iter()
                    .find(|c| &c.name == pc)
                    // unwrap is safe here because we have checked that partition columns are defined
                    .unwrap()
            })
            .collect::<Vec<_>>();
        let mut column_name_and_type = Vec::with_capacity(column_defs.len());
        for column in column_defs {
            let column_name = &column.name;
            let data_type = ConcreteDataType::from(
                ColumnDataTypeWrapper::try_new(column.data_type).context(ColumnDataTypeSnafu)?,
            );
            column_name_and_type.push((column_name, data_type));
        }

        let mut entries = Vec::with_capacity(partitions.entries.len());
        for e in partitions.entries.iter() {
            let mut values = Vec::with_capacity(e.value_list.len());
            for (i, v) in e.value_list.iter().enumerate() {
                // indexing is safe here because we have checked that "value_list" and "column_list" are matched in size
                let (column_name, data_type) = &column_name_and_type[i];
                let v = match v {
                    SqlValue::Number(n, _) if n == MAX_VALUE => PartitionBound::MaxValue,
                    _ => PartitionBound::Value(
                        sql_value_to_value(column_name, data_type, v).context(ParseSqlSnafu)?,
                    ),
                };
                values.push(v);
            }
            entries.push(values);
        }
        entries
    } else {
        vec![vec![PartitionBound::MaxValue]]
    };
    Ok(entries)
}

fn merge_options(mut table_opts: TableOptions, schema_opts: SchemaNameValue) -> TableOptions {
    table_opts.ttl = table_opts.ttl.or(schema_opts.ttl);
    table_opts
}

#[cfg(test)]
mod test {
    use session::context::QueryContext;
    use sql::dialect::GreptimeDbDialect;
    use sql::parser::ParserContext;
    use sql::statements::statement::Statement;

    use super::*;
    use crate::expr_factory;

    #[tokio::test]
    async fn test_parse_partitions() {
        common_telemetry::init_default_ut_logging();
        let cases = [
            (
                r"
CREATE TABLE rcx ( a INT, b STRING, c TIMESTAMP, TIME INDEX (c) )
PARTITION BY RANGE COLUMNS (b) (
  PARTITION r0 VALUES LESS THAN ('hz'),
  PARTITION r1 VALUES LESS THAN ('sh'),
  PARTITION r2 VALUES LESS THAN (MAXVALUE),
)
ENGINE=mito",
                r#"[{"column_list":["b"],"value_list":["{\"Value\":{\"String\":\"hz\"}}"]},{"column_list":["b"],"value_list":["{\"Value\":{\"String\":\"sh\"}}"]},{"column_list":["b"],"value_list":["\"MaxValue\""]}]"#,
            ),
            (
                r"
CREATE TABLE rcx ( a INT, b STRING, c TIMESTAMP, TIME INDEX (c) )
PARTITION BY RANGE COLUMNS (b, a) (
  PARTITION r0 VALUES LESS THAN ('hz', 10),
  PARTITION r1 VALUES LESS THAN ('sh', 20),
  PARTITION r2 VALUES LESS THAN (MAXVALUE, MAXVALUE),
)
ENGINE=mito",
                r#"[{"column_list":["b","a"],"value_list":["{\"Value\":{\"String\":\"hz\"}}","{\"Value\":{\"Int32\":10}}"]},{"column_list":["b","a"],"value_list":["{\"Value\":{\"String\":\"sh\"}}","{\"Value\":{\"Int32\":20}}"]},{"column_list":["b","a"],"value_list":["\"MaxValue\"","\"MaxValue\""]}]"#,
            ),
        ];
        for (sql, expected) in cases {
            let result = ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}).unwrap();
            match &result[0] {
                Statement::CreateTable(c) => {
                    let expr = expr_factory::create_to_expr(c, QueryContext::arc()).unwrap();
                    let (partitions, _) = parse_partitions(&expr, c.partitions.clone()).unwrap();
                    let json = serde_json::to_string(&partitions).unwrap();
                    assert_eq!(json, expected);
                }
                _ => unreachable!(),
            }
        }
    }
}
