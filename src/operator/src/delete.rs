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

use std::collections::HashSet;
use std::sync::Arc;
use std::{iter, mem};

use api::v1::region::{DeleteRequests as RegionDeleteRequests, RegionRequestHeader};
use api::v1::{DeleteRequests, RowDeleteRequests};
use catalog::CatalogManagerRef;
use common_meta::node_manager::{AffectedRows, NodeManagerRef};
use common_query::Output;
use common_telemetry::tracing_context::TracingContext;
use partition::manager::PartitionRuleManagerRef;
use session::context::QueryContextRef;
use snafu::{OptionExt, ResultExt, ensure};
use table::TableRef;
use table::requests::DeleteRequest as TableDeleteRequest;
use tokio_util::sync::CancellationToken;

use crate::error::{
    CatalogSnafu, InvalidDeleteRequestSnafu, JoinTaskSnafu, MissingTimeIndexColumnSnafu,
    RequestDeletesSnafu, Result, TableNotFoundSnafu,
};
use crate::region_req_factory::RegionRequestFactory;
use crate::req_convert::common::preprocess_row_delete_requests;
use crate::req_convert::delete::{ColumnToRow, RowToRegion, TableToRegion};
use crate::write_dispatcher::{WriteDispatcher, WriteDispatcherRef};

pub struct Deleter {
    catalog_manager: CatalogManagerRef,
    partition_manager: PartitionRuleManagerRef,
    write_dispatcher: WriteDispatcherRef,
}

pub type DeleterRef = Arc<Deleter>;

impl Deleter {
    /// Creates a row deleter with the default write dispatcher.
    pub fn new(
        catalog_manager: CatalogManagerRef,
        partition_manager: PartitionRuleManagerRef,
        node_manager: NodeManagerRef,
    ) -> Self {
        Self {
            write_dispatcher: Arc::new(WriteDispatcher::new(
                node_manager,
                partition_manager.clone(),
            )),
            catalog_manager,
            partition_manager,
        }
    }

    /// Installs the dispatcher shared with the other write paths.
    pub fn with_write_dispatcher(mut self, dispatcher: WriteDispatcherRef) -> Self {
        self.write_dispatcher = dispatcher;
        self
    }

    pub async fn handle_column_deletes(
        &self,
        requests: DeleteRequests,
        ctx: QueryContextRef,
    ) -> Result<Output> {
        let row_deletes = ColumnToRow::convert(requests)?;
        self.handle_row_deletes(row_deletes, ctx).await
    }

    pub async fn handle_row_deletes(
        &self,
        mut requests: RowDeleteRequests,
        ctx: QueryContextRef,
    ) -> Result<Output> {
        preprocess_row_delete_requests(&mut requests.deletes)?;
        // remove empty requests
        requests.deletes.retain(|req| {
            req.rows
                .as_ref()
                .map(|r| !r.rows.is_empty())
                .unwrap_or_default()
        });
        validate_column_count_match(&requests)?;

        let requests = self.trim_columns(requests, &ctx).await?;
        let deletes = RowToRegion::new(
            self.catalog_manager.as_ref(),
            self.partition_manager.as_ref(),
            &ctx,
        )
        .convert(requests)
        .await?;

        let affected_rows = self.do_request(deletes, &ctx).await?;

        Ok(Output::new_with_affected_rows(affected_rows))
    }

    pub async fn handle_table_delete(
        &self,
        request: TableDeleteRequest,
        ctx: QueryContextRef,
    ) -> Result<AffectedRows> {
        let catalog = request.catalog_name.as_str();
        let schema = request.schema_name.as_str();
        let table = request.table_name.as_str();
        let table = self.get_table(catalog, schema, table).await?;
        let table_info = table.table_info();

        let deletes = TableToRegion::new(&table_info, &self.partition_manager)
            .convert(request)
            .await?;

        let affected_rows = self.do_request(deletes, &ctx).await?;
        Ok(affected_rows as _)
    }
}

impl Deleter {
    async fn do_request(
        &self,
        requests: RegionDeleteRequests,
        ctx: &QueryContextRef,
    ) -> Result<AffectedRows> {
        let request_factory = RegionRequestFactory::new(RegionRequestHeader {
            tracing_context: TracingContext::from_current_span().to_w3c(),
            dbname: ctx.get_db_string(),
            ..Default::default()
        });

        let dispatcher = self.write_dispatcher.clone();
        let request = request_factory.build_delete(requests);
        let writes = dispatcher.prepare(vec![request]).await?;
        // Dropping the caller stops retries without abandoning in-flight writes.
        let cancellation = CancellationToken::new();
        let _guard = cancellation.clone().drop_guard();
        let affected_rows = common_runtime::spawn_global(async move {
            dispatcher.execute(writes, None, Some(&cancellation)).await
        })
        .await
        .context(JoinTaskSnafu)?
        .context(RequestDeletesSnafu)?
        .affected_rows;
        crate::metrics::DIST_DELETE_ROW_COUNT
            .with_label_values(&[ctx.get_db_string().as_str()])
            .inc_by(affected_rows as u64);
        Ok(affected_rows)
    }

    async fn trim_columns(
        &self,
        mut requests: RowDeleteRequests,
        ctx: &QueryContextRef,
    ) -> Result<RowDeleteRequests> {
        for req in &mut requests.deletes {
            let catalog = ctx.current_catalog();
            let schema = ctx.current_schema();
            let table = self.get_table(catalog, &schema, &req.table_name).await?;
            let key_column_names = self.key_column_names(&table)?;

            let rows = req.rows.as_mut().unwrap();
            let all_key = rows
                .schema
                .iter()
                .all(|column| key_column_names.contains(&column.column_name));
            if all_key {
                continue;
            }

            for row in &mut rows.rows {
                let source_values = mem::take(&mut row.values);
                row.values = rows
                    .schema
                    .iter()
                    .zip(source_values)
                    .filter_map(|(column, v)| {
                        key_column_names.contains(&column.column_name).then_some(v)
                    })
                    .collect();
            }
            rows.schema
                .retain(|column| key_column_names.contains(&column.column_name));
        }
        Ok(requests)
    }

    fn key_column_names(&self, table: &TableRef) -> Result<HashSet<String>> {
        let time_index = table
            .schema()
            .timestamp_column()
            .with_context(|| table::error::MissingTimeIndexColumnSnafu {
                table_name: table.table_info().name.clone(),
            })
            .context(MissingTimeIndexColumnSnafu)?
            .name
            .clone();

        let table_info = table.table_info();
        let key_column_names = table_info
            .meta
            .row_key_column_names()
            .chain(table_info.meta.partition_column_names())
            .cloned()
            .chain(iter::once(time_index))
            .collect();

        Ok(key_column_names)
    }

    async fn get_table(&self, catalog: &str, schema: &str, table: &str) -> Result<TableRef> {
        self.catalog_manager
            .table(catalog, schema, table, None)
            .await
            .context(CatalogSnafu)?
            .with_context(|| TableNotFoundSnafu {
                table_name: common_catalog::format_full_table_name(catalog, schema, table),
            })
    }
}

fn validate_column_count_match(requests: &RowDeleteRequests) -> Result<()> {
    for request in &requests.deletes {
        let rows = request.rows.as_ref().unwrap();
        let column_count = rows.schema.len();
        rows.rows.iter().try_for_each(|r| {
            ensure!(
                r.values.len() == column_count,
                InvalidDeleteRequestSnafu {
                    reason: format!(
                        "column count mismatch, columns: {}, values: {}",
                        column_count,
                        r.values.len()
                    )
                }
            );
            Ok(())
        })?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use api::v1::helper::{field_column_schema, tag_column_schema, time_index_column_schema};
    use api::v1::value::ValueData;
    use api::v1::{ColumnDataType, Row, RowDeleteRequest, RowDeleteRequests, Rows, Value};
    use catalog::RegisterTableRequest;
    use catalog::memory::MemoryCatalogManager;
    use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
    use common_meta::ddl::test_util::datanode_handler::NaiveDatanodeHandler;
    use common_meta::test_util::MockDatanodeManager;
    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::{ColumnSchema, SchemaBuilder};
    use session::context::QueryContext;
    use table::Table;
    use table::dist_table::DummyDataSource;
    use table::metadata::{FilterPushDownType, TableInfoBuilder, TableMetaBuilder};

    use super::*;
    use crate::test_util::{create_partition_rule_manager, prepare_mocked_backend};

    #[tokio::test]
    async fn test_trim_columns_retains_non_key_partition_column_and_aligns_rows() {
        let schema = SchemaBuilder::try_from(vec![
            ColumnSchema::new("k1", ConcreteDataType::string_datatype(), true),
            ColumnSchema::new("k2", ConcreteDataType::int32_datatype(), true),
            ColumnSchema::new(
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            )
            .with_time_index(true),
            ColumnSchema::new("part_val", ConcreteDataType::float64_datatype(), true),
            ColumnSchema::new("extra_field", ConcreteDataType::int64_datatype(), true),
        ])
        .unwrap()
        .build()
        .unwrap();

        let meta = TableMetaBuilder::empty()
            .schema(Arc::new(schema))
            .primary_key_indices(vec![0, 1])
            .value_indices(vec![3, 4])
            .partition_key_indices(vec![3])
            .engine("mito")
            .next_column_id(5)
            .build()
            .unwrap();
        let table_info = TableInfoBuilder::default()
            .table_id(1)
            .table_version(1)
            .name("val_part_table")
            .meta(meta)
            .build()
            .unwrap();

        let catalog = MemoryCatalogManager::with_default_setup();
        let table = Arc::new(Table::new(
            Arc::new(table_info),
            FilterPushDownType::Unsupported,
            Arc::new(DummyDataSource),
        ));
        catalog
            .register_table_sync(RegisterTableRequest {
                catalog: DEFAULT_CATALOG_NAME.to_string(),
                schema: DEFAULT_SCHEMA_NAME.to_string(),
                table_name: "val_part_table".to_string(),
                table_id: 1,
                table,
            })
            .unwrap();

        let backend = prepare_mocked_backend().await;
        let partition_manager = create_partition_rule_manager(backend).await;
        let deleter = Deleter::new(
            catalog,
            partition_manager,
            Arc::new(MockDatanodeManager::new(NaiveDatanodeHandler)),
        );

        // Interleave an unrelated field between keys, non-key partition column, and timestamp
        // to verify both column filtering and positional value alignment across multiple rows.
        let requests = RowDeleteRequests {
            deletes: vec![RowDeleteRequest {
                table_name: "val_part_table".to_string(),
                rows: Some(Rows {
                    schema: vec![
                        tag_column_schema("k1", ColumnDataType::String),
                        field_column_schema("extra_field", ColumnDataType::Int64),
                        field_column_schema("part_val", ColumnDataType::Float64),
                        time_index_column_schema("ts", ColumnDataType::TimestampMillisecond),
                        tag_column_schema("k2", ColumnDataType::Int32),
                    ],
                    rows: vec![
                        Row {
                            values: vec![
                                Value {
                                    value_data: Some(ValueData::StringValue("job_a".to_string())),
                                },
                                Value {
                                    value_data: Some(ValueData::I64Value(999)),
                                },
                                Value {
                                    value_data: Some(ValueData::F64Value(-1.5)),
                                },
                                Value {
                                    value_data: Some(ValueData::TimestampMillisecondValue(60_000)),
                                },
                                Value {
                                    value_data: Some(ValueData::I32Value(10)),
                                },
                            ],
                        },
                        Row {
                            values: vec![
                                Value {
                                    value_data: Some(ValueData::StringValue("job_b".to_string())),
                                },
                                Value {
                                    value_data: Some(ValueData::I64Value(1234)),
                                },
                                Value {
                                    value_data: Some(ValueData::F64Value(2.5)),
                                },
                                Value {
                                    value_data: Some(ValueData::TimestampMillisecondValue(61_000)),
                                },
                                Value {
                                    value_data: Some(ValueData::I32Value(20)),
                                },
                            ],
                        },
                    ],
                }),
            }],
        };

        let trimmed = deleter
            .trim_columns(requests, &QueryContext::arc())
            .await
            .unwrap();
        validate_column_count_match(&trimmed).unwrap();

        let trimmed_rows = trimmed.deletes[0].rows.as_ref().unwrap();
        let col_names: Vec<&str> = trimmed_rows
            .schema
            .iter()
            .map(|c| c.column_name.as_str())
            .collect();
        assert_eq!(col_names, vec!["k1", "part_val", "ts", "k2"]);

        assert_eq!(
            trimmed_rows.rows[0].values,
            vec![
                Value {
                    value_data: Some(ValueData::StringValue("job_a".to_string())),
                },
                Value {
                    value_data: Some(ValueData::F64Value(-1.5)),
                },
                Value {
                    value_data: Some(ValueData::TimestampMillisecondValue(60_000)),
                },
                Value {
                    value_data: Some(ValueData::I32Value(10)),
                },
            ]
        );
        assert_eq!(
            trimmed_rows.rows[1].values,
            vec![
                Value {
                    value_data: Some(ValueData::StringValue("job_b".to_string())),
                },
                Value {
                    value_data: Some(ValueData::F64Value(2.5)),
                },
                Value {
                    value_data: Some(ValueData::TimestampMillisecondValue(61_000)),
                },
                Value {
                    value_data: Some(ValueData::I32Value(20)),
                },
            ]
        );
    }
}
