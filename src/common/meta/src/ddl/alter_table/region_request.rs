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

use api::v1::alter_table_expr::Kind;
use api::v1::region::{
    AddColumn, AddColumns, DropColumn, DropColumns, RegionColumnDef, alter_request,
};
use snafu::OptionExt;
use table::metadata::RawTableInfo;

use crate::ddl::alter_table::AlterTableProcedure;
use crate::error::{self, InvalidProtoMsgSnafu, Result};

impl AlterTableProcedure {
    /// Makes alter kind proto that all regions can reuse.
    /// Region alter request always add columns if not exist.
    pub(crate) fn make_region_alter_kind(&self) -> Result<Option<alter_request::Kind>> {
        // Safety: Checked in `AlterTableProcedure::new`.
        let alter_kind = self.data.task.alter_table.kind.as_ref().unwrap();
        // Safety: checked
        let table_info = self.data.table_info().unwrap();
        let kind = create_proto_alter_kind(table_info, alter_kind)?;

        Ok(kind)
    }
}

/// Creates region proto alter kind from `table_info` and `alter_kind`.
///
/// It always adds column if not exists and drops column if exists.
/// It skips the column if it already exists in the table.
fn create_proto_alter_kind(
    table_info: &RawTableInfo,
    alter_kind: &Kind,
) -> Result<Option<alter_request::Kind>> {
    match alter_kind {
        Kind::AddColumns(x) => {
            // Construct a set of existing columns in the table.
            let existing_columns: HashSet<_> = table_info
                .meta
                .schema
                .column_schemas
                .iter()
                .map(|col| &col.name)
                .collect();
            let mut next_column_id = table_info.meta.next_column_id;

            let mut add_columns = Vec::with_capacity(x.add_columns.len());
            for add_column in &x.add_columns {
                let column_def = add_column
                    .column_def
                    .as_ref()
                    .context(InvalidProtoMsgSnafu {
                        err_msg: "'column_def' is absent",
                    })?;

                // Skips existing columns.
                if existing_columns.contains(&column_def.name) {
                    continue;
                }

                let column_id = next_column_id;
                next_column_id += 1;
                let column_def = RegionColumnDef {
                    column_def: Some(column_def.clone()),
                    column_id,
                };

                add_columns.push(AddColumn {
                    column_def: Some(column_def),
                    location: add_column.location.clone(),
                });
            }

            Ok(Some(alter_request::Kind::AddColumns(AddColumns {
                add_columns,
            })))
        }
        Kind::ModifyColumnTypes(x) => Ok(Some(alter_request::Kind::ModifyColumnTypes(x.clone()))),
        Kind::DropColumns(x) => {
            let drop_columns = x
                .drop_columns
                .iter()
                .map(|x| DropColumn {
                    name: x.name.clone(),
                })
                .collect::<Vec<_>>();

            Ok(Some(alter_request::Kind::DropColumns(DropColumns {
                drop_columns,
            })))
        }
        Kind::RenameTable(_) => Ok(None),
        Kind::SetTableOptions(v) => Ok(Some(alter_request::Kind::SetTableOptions(v.clone()))),
        Kind::UnsetTableOptions(v) => Ok(Some(alter_request::Kind::UnsetTableOptions(v.clone()))),
        Kind::SetIndex(v) => Ok(Some(alter_request::Kind::SetIndex(v.clone()))),
        Kind::UnsetIndex(v) => Ok(Some(alter_request::Kind::UnsetIndex(v.clone()))),
        Kind::SetIndexes(v) => Ok(Some(alter_request::Kind::SetIndexes(v.clone()))),
        Kind::UnsetIndexes(v) => Ok(Some(alter_request::Kind::UnsetIndexes(v.clone()))),
        Kind::DropDefaults(v) => Ok(Some(alter_request::Kind::DropDefaults(v.clone()))),
        Kind::SetDefaults(v) => Ok(Some(alter_request::Kind::SetDefaults(v.clone()))),
        Kind::Repartition(_) => error::UnexpectedSnafu {
            err_msg: "Repartition operation should be handled through DdlManager and not converted to AlterTableRequest",
        }
        .fail()?,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use api::v1::add_column_location::LocationType;
    use api::v1::alter_table_expr::Kind;
    use api::v1::region::RegionColumnDef;
    use api::v1::region::region_request::Body;
    use api::v1::{
        AddColumn, AddColumnLocation, AddColumns, AlterTableExpr, ColumnDataType,
        ColumnDef as PbColumnDef, ModifyColumnType, ModifyColumnTypes, SemanticType, region,
    };
    use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
    use store_api::storage::{RegionId, TableId};

    use crate::ddl::DdlContext;
    use crate::ddl::alter_table::AlterTableProcedure;
    use crate::ddl::alter_table::executor::make_alter_region_request;
    use crate::ddl::test_util::columns::TestColumnDefBuilder;
    use crate::ddl::test_util::create_table::{
        TestCreateTableExprBuilder, build_raw_table_info_from_expr,
    };
    use crate::key::table_route::TableRouteValue;
    use crate::peer::Peer;
    use crate::rpc::ddl::AlterTableTask;
    use crate::rpc::router::{Region, RegionRoute};
    use crate::test_util::{MockDatanodeManager, new_ddl_context};

    /// Prepares a region with schema `[ts: Timestamp, host: Tag, cpu: Field]`.
    async fn prepare_ddl_context() -> (DdlContext, TableId, RegionId, String) {
        let datanode_manager = Arc::new(MockDatanodeManager::new(()));
        let ddl_context = new_ddl_context(datanode_manager);
        let table_id = 1024;
        let region_id = RegionId::new(table_id, 1);
        let table_name = "foo";

        let create_table = TestCreateTableExprBuilder::default()
            .column_defs([
                TestColumnDefBuilder::default()
                    .name("ts")
                    .data_type(ColumnDataType::TimestampMillisecond)
                    .semantic_type(SemanticType::Timestamp)
                    .build()
                    .unwrap()
                    .into(),
                TestColumnDefBuilder::default()
                    .name("host")
                    .data_type(ColumnDataType::String)
                    .semantic_type(SemanticType::Tag)
                    .build()
                    .unwrap()
                    .into(),
                TestColumnDefBuilder::default()
                    .name("cpu")
                    .data_type(ColumnDataType::Float64)
                    .semantic_type(SemanticType::Field)
                    .is_nullable(true)
                    .build()
                    .unwrap()
                    .into(),
            ])
            .table_id(table_id)
            .time_index("ts")
            .primary_keys(["host".into()])
            .table_name(table_name)
            .build()
            .unwrap()
            .into();
        let table_info = build_raw_table_info_from_expr(&create_table);

        // Puts a value to table name key.
        ddl_context
            .table_metadata_manager
            .create_table_metadata(
                table_info,
                TableRouteValue::physical(vec![RegionRoute {
                    region: Region::new_test(region_id),
                    leader_peer: Some(Peer::empty(1)),
                    follower_peers: vec![],
                    leader_state: None,
                    leader_down_since: None,
                }]),
                HashMap::new(),
            )
            .await
            .unwrap();
        (ddl_context, table_id, region_id, table_name.to_string())
    }

    #[tokio::test]
    async fn test_make_alter_region_request() {
        let (ddl_context, table_id, region_id, table_name) = prepare_ddl_context().await;

        let task = AlterTableTask {
            alter_table: AlterTableExpr {
                catalog_name: DEFAULT_CATALOG_NAME.to_string(),
                schema_name: DEFAULT_SCHEMA_NAME.to_string(),
                table_name,
                kind: Some(Kind::AddColumns(AddColumns {
                    add_columns: vec![AddColumn {
                        column_def: Some(PbColumnDef {
                            name: "my_tag3".to_string(),
                            data_type: ColumnDataType::String as i32,
                            is_nullable: true,
                            default_constraint: Vec::new(),
                            semantic_type: SemanticType::Tag as i32,
                            comment: String::new(),
                            ..Default::default()
                        }),
                        location: Some(AddColumnLocation {
                            location_type: LocationType::After as i32,
                            after_column_name: "host".to_string(),
                        }),
                        add_if_not_exists: false,
                    }],
                })),
            },
        };

        let mut procedure = AlterTableProcedure::new(table_id, task, ddl_context).unwrap();
        procedure.on_prepare().await.unwrap();
        let alter_kind = procedure.make_region_alter_kind().unwrap();
        let Some(Body::Alter(alter_region_request)) =
            make_alter_region_request(region_id, alter_kind).body
        else {
            unreachable!()
        };
        assert_eq!(alter_region_request.region_id, region_id.as_u64());
        assert_eq!(alter_region_request.schema_version, 0);
        assert_eq!(
            alter_region_request.kind,
            Some(region::alter_request::Kind::AddColumns(
                region::AddColumns {
                    add_columns: vec![region::AddColumn {
                        column_def: Some(RegionColumnDef {
                            column_def: Some(PbColumnDef {
                                name: "my_tag3".to_string(),
                                data_type: ColumnDataType::String as i32,
                                is_nullable: true,
                                default_constraint: Vec::new(),
                                semantic_type: SemanticType::Tag as i32,
                                comment: String::new(),
                                ..Default::default()
                            }),
                            column_id: 3,
                        }),
                        location: Some(AddColumnLocation {
                            location_type: LocationType::After as i32,
                            after_column_name: "host".to_string(),
                        }),
                    }]
                }
            ))
        );
    }

    #[tokio::test]
    async fn test_make_alter_column_type_region_request() {
        let (ddl_context, table_id, region_id, table_name) = prepare_ddl_context().await;

        let task = AlterTableTask {
            alter_table: AlterTableExpr {
                catalog_name: DEFAULT_CATALOG_NAME.to_string(),
                schema_name: DEFAULT_SCHEMA_NAME.to_string(),
                table_name,
                kind: Some(Kind::ModifyColumnTypes(ModifyColumnTypes {
                    modify_column_types: vec![ModifyColumnType {
                        column_name: "cpu".to_string(),
                        target_type: ColumnDataType::String as i32,
                        target_type_extension: None,
                    }],
                })),
            },
        };

        let mut procedure = AlterTableProcedure::new(table_id, task, ddl_context).unwrap();
        procedure.on_prepare().await.unwrap();
        let alter_kind = procedure.make_region_alter_kind().unwrap();
        let Some(Body::Alter(alter_region_request)) =
            make_alter_region_request(region_id, alter_kind).body
        else {
            unreachable!()
        };
        assert_eq!(alter_region_request.region_id, region_id.as_u64());
        assert_eq!(alter_region_request.schema_version, 0);
        assert_eq!(
            alter_region_request.kind,
            Some(region::alter_request::Kind::ModifyColumnTypes(
                ModifyColumnTypes {
                    modify_column_types: vec![ModifyColumnType {
                        column_name: "cpu".to_string(),
                        target_type: ColumnDataType::String as i32,
                        target_type_extension: None,
                    }]
                }
            ))
        );
    }
}
