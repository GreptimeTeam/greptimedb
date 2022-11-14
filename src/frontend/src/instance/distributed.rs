use std::collections::HashMap;
use std::sync::Arc;

use chrono::DateTime;
use client::admin::{admin_result_to_output, Admin};
use client::Select;
use common_catalog::{TableGlobalKey, TableGlobalValue};
use common_query::Output;
use common_telemetry::debug;
use datatypes::schema::RawSchema;
use meta_client::client::MetaClient;
use meta_client::rpc::{
    CreateRequest as MetaCreateRequest, Partition as MetaPartition, RouteResponse, TableName,
    TableRoute,
};
use query::{QueryEngineFactory, QueryEngineRef};
use snafu::{ensure, OptionExt, ResultExt};
use sql::statements::create::CreateTable;
use sql::statements::{
    column_def_to_schema, sql_data_type_to_concrete_data_type, sql_value_to_value,
    table_idents_to_full_name,
};
use sqlparser::ast::ColumnDef;
use sqlparser::ast::Value as SqlValue;
use table::metadata::RawTableMeta;

use crate::catalog::FrontendCatalogManager;
use crate::datanode::DatanodeClients;
use crate::error::{self, Result};
use crate::instance::{find_primary_keys, find_time_index, Instance};
use crate::partitioning::{PartitionBound, PartitionDef};

#[derive(Clone)]
pub(crate) struct DistInstance {
    meta_client: Arc<MetaClient>,
    catalog_manager: Arc<FrontendCatalogManager>,
    datanode_clients: Arc<DatanodeClients>,
    query_engine: QueryEngineRef,
}

impl DistInstance {
    pub(crate) fn new(
        meta_client: Arc<MetaClient>,
        catalog_manager: Arc<FrontendCatalogManager>,
        datanode_clients: Arc<DatanodeClients>,
    ) -> Self {
        let query_engine = QueryEngineFactory::new(catalog_manager.clone()).query_engine();
        Self {
            meta_client,
            catalog_manager,
            datanode_clients,
            query_engine,
        }
    }

    pub(crate) async fn create_table(&self, create_table: &CreateTable) -> Result<Output> {
        let response = self.create_table_in_meta(create_table).await?;

        let table_routes = response.table_routes;
        ensure!(
            table_routes.len() == 1,
            error::FindTableRoutesSnafu {
                table_name: create_table.name.to_string()
            }
        );
        let table_route = table_routes.first().unwrap();

        let region_routes = &table_route.region_routes;
        ensure!(
            !region_routes.is_empty(),
            error::FindRegionRoutesSnafu {
                table_name: create_table.name.to_string()
            }
        );

        self.put_table_global_meta(create_table, table_route)
            .await?;

        for datanode in table_route.find_leaders() {
            let client = self.datanode_clients.get_client(&datanode).await;
            let client = Admin::new("greptime", client);

            let regions = table_route.find_leader_regions(&datanode);
            let create_expr = Instance::create_to_expr(
                Some(table_route.table.id as u32),
                regions.clone(),
                create_table,
            )?;
            debug!(
                "Creating table {:?} on Datanode {:?} with regions {:?}",
                create_table, datanode, regions,
            );

            client
                .create(create_expr)
                .await
                .and_then(admin_result_to_output)
                .context(error::InvalidAdminResultSnafu)?;
        }

        Ok(Output::AffectedRows(region_routes.len()))
    }

    pub(crate) async fn handle_select(&self, select: Select) -> Result<Output> {
        let Select::Sql(sql) = select;
        let plan = self
            .query_engine
            .sql_to_plan(&sql)
            .with_context(|_| error::ExecuteSqlSnafu { sql: sql.clone() })?;
        self.query_engine
            .execute(&plan)
            .await
            .context(error::ExecuteSqlSnafu { sql })
    }

    async fn create_table_in_meta(&self, create_table: &CreateTable) -> Result<RouteResponse> {
        let (catalog, schema, table) =
            table_idents_to_full_name(&create_table.name).context(error::ParseSqlSnafu)?;
        let table_name = TableName::new(catalog, schema, table);

        let partitions = parse_partitions(create_table)?;

        let request = MetaCreateRequest {
            table_name,
            partitions,
        };
        self.meta_client
            .create_route(request)
            .await
            .context(error::RequestMetaSnafu)
    }

    // TODO(LFC): Maybe move this to FrontendCatalogManager's "register_table" method?
    async fn put_table_global_meta(
        &self,
        create_table: &CreateTable,
        table_route: &TableRoute,
    ) -> Result<()> {
        let table_name = &table_route.table.table_name;
        let key = TableGlobalKey {
            catalog_name: table_name.catalog_name.clone(),
            schema_name: table_name.schema_name.clone(),
            table_name: table_name.table_name.clone(),
        };

        let value = create_table_global_value(create_table, table_route)?
            .as_bytes()
            .context(error::CatalogEntrySerdeSnafu)?;

        self.catalog_manager
            .backend()
            .set(key.to_string().as_bytes(), &value)
            .await
            .context(error::CatalogSnafu)
    }
}

fn create_table_global_value(
    create_table: &CreateTable,
    table_route: &TableRoute,
) -> Result<TableGlobalValue> {
    let table_name = &table_route.table.table_name;

    let region_routes = &table_route.region_routes;
    let node_id = region_routes[0]
        .leader_peer
        .as_ref()
        .context(error::FindLeaderPeerSnafu {
            region: region_routes[0].region.id,
            table_name: table_name.to_string(),
        })?
        .id;

    let mut column_schemas = Vec::with_capacity(create_table.columns.len());
    let time_index = find_time_index(&create_table.constraints)?;
    for column in create_table.columns.iter() {
        column_schemas.push(
            column_def_to_schema(column, column.name.value == time_index)
                .context(error::ParseSqlSnafu)?,
        );
    }
    let timestamp_index = column_schemas.iter().enumerate().find_map(|(i, c)| {
        if c.name == time_index {
            Some(i)
        } else {
            None
        }
    });
    let raw_schema = RawSchema {
        column_schemas: column_schemas.clone(),
        timestamp_index,
        version: 0,
    };

    let primary_key_indices = find_primary_keys(&create_table.constraints)?
        .iter()
        .map(|k| {
            column_schemas
                .iter()
                .enumerate()
                .find_map(|(i, c)| if &c.name == k { Some(i) } else { None })
                .unwrap() // unwrap is safe because primary key's column name must have been defined
        })
        .collect::<Vec<usize>>();

    let meta = RawTableMeta {
        schema: raw_schema,
        primary_key_indices,
        value_indices: vec![],
        engine: create_table.engine.clone(),
        next_column_id: column_schemas.len() as u32,
        region_numbers: vec![],
        engine_options: HashMap::new(),
        options: HashMap::new(),
        created_on: DateTime::default(),
    };

    Ok(TableGlobalValue {
        id: table_route.table.id as u32,
        node_id,
        regions_id_map: HashMap::new(),
        meta,
    })
}

fn parse_partitions(create_table: &CreateTable) -> Result<Vec<MetaPartition>> {
    // If partitions are not defined by user, use the timestamp column (which has to be existed) as
    // the partition column, and create only one partition.
    let partition_columns = find_partition_columns(create_table)?;
    let partition_entries = find_partition_entries(create_table, &partition_columns)?;

    partition_entries
        .into_iter()
        .map(|x| PartitionDef::new(partition_columns.clone(), x).try_into())
        .collect::<Result<Vec<MetaPartition>>>()
}

fn find_partition_entries(
    create_table: &CreateTable,
    partition_columns: &[String],
) -> Result<Vec<Vec<PartitionBound>>> {
    let entries = if let Some(partitions) = &create_table.partitions {
        let column_defs = partition_columns
            .iter()
            .map(|pc| {
                create_table
                    .columns
                    .iter()
                    .find(|c| &c.name.value == pc)
                    // unwrap is safe here because we have checked that partition columns are defined
                    .unwrap()
            })
            .collect::<Vec<&ColumnDef>>();
        let mut column_name_and_type = Vec::with_capacity(column_defs.len());
        for column in column_defs {
            let column_name = &column.name.value;
            let data_type = sql_data_type_to_concrete_data_type(&column.data_type)
                .context(error::ParseSqlSnafu)?;
            column_name_and_type.push((column_name, data_type));
        }

        let mut entries = Vec::with_capacity(partitions.entries.len());
        for e in partitions.entries.iter() {
            let mut values = Vec::with_capacity(e.value_list.len());
            for (i, v) in e.value_list.iter().enumerate() {
                // indexing is safe here because we have checked that "value_list" and "column_list" are matched in size
                let (column_name, data_type) = &column_name_and_type[i];
                let v = match v {
                    SqlValue::Number(n, _) if n == "MAXVALUE" => PartitionBound::MaxValue,
                    _ => PartitionBound::Value(
                        sql_value_to_value(column_name, data_type, v)
                            .context(error::ParseSqlSnafu)?,
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

fn find_partition_columns(create_table: &CreateTable) -> Result<Vec<String>> {
    let columns = if let Some(partitions) = &create_table.partitions {
        partitions
            .column_list
            .iter()
            .map(|x| x.value.clone())
            .collect::<Vec<String>>()
    } else {
        vec![find_time_index(&create_table.constraints)?]
    };
    Ok(columns)
}

#[cfg(test)]
mod test {

    use sql::parser::ParserContext;
    use sql::statements::statement::Statement;
    use sqlparser::dialect::GenericDialect;

    use super::*;

    #[test]
    fn test_parse_partitions() {
        let cases = [
            (
                r"
CREATE TABLE rcx ( a INT, b STRING, c INT )
PARTITION BY RANGE COLUMNS (b) (
  PARTITION r0 VALUES LESS THAN ('hz'),
  PARTITION r1 VALUES LESS THAN ('sh'),
  PARTITION r2 VALUES LESS THAN (MAXVALUE),
)
ENGINE=mito",
                r#"[{"column_list":"b","value_list":"{\"Value\":{\"String\":\"hz\"}}"},{"column_list":"b","value_list":"{\"Value\":{\"String\":\"sh\"}}"},{"column_list":"b","value_list":"\"MaxValue\""}]"#,
            ),
            (
                r"
CREATE TABLE rcx ( a INT, b STRING, c INT )
PARTITION BY RANGE COLUMNS (b, a) (
  PARTITION r0 VALUES LESS THAN ('hz', 10),
  PARTITION r1 VALUES LESS THAN ('sh', 20),
  PARTITION r2 VALUES LESS THAN (MAXVALUE, MAXVALUE),
)
ENGINE=mito",
                r#"[{"column_list":"b,a","value_list":"{\"Value\":{\"String\":\"hz\"}},{\"Value\":{\"Int32\":10}}"},{"column_list":"b,a","value_list":"{\"Value\":{\"String\":\"sh\"}},{\"Value\":{\"Int32\":20}}"},{"column_list":"b,a","value_list":"\"MaxValue\",\"MaxValue\""}]"#,
            ),
        ];
        for (sql, expected) in cases {
            let result = ParserContext::create_with_dialect(sql, &GenericDialect {}).unwrap();
            match &result[0] {
                Statement::CreateTable(c) => {
                    let partitions = parse_partitions(c).unwrap();
                    let json = serde_json::to_string(&partitions).unwrap();
                    assert_eq!(json, expected);
                }
                _ => unreachable!(),
            }
        }
    }
}
