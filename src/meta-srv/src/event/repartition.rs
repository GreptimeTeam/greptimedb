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

use std::any::Any;
use std::collections::HashMap;
use std::time::Duration;

use api::v1::value::ValueData;
use api::v1::{ColumnSchema, Row};
use common_event_recorder::Event;
use common_event_recorder::error::{Result, SerializeEventSnafu};
use common_event_recorder::event_table::{
    CATALOG_NAME_COLUMN, PARENT_PROCEDURE_ID_COLUMN, REPARTITION_GROUP_ID_COLUMN,
    SCHEMA_NAME_COLUMN, SOURCE_PARTITION_EXPR_COLUMN, SOURCE_REGION_ID_COLUMN,
    SOURCE_REGION_NUMBER_COLUMN, TABLE_ID_COLUMN, TABLE_NAME_COLUMN, TARGET_PARTITION_EXPR_COLUMN,
    TARGET_REGION_ID_COLUMN, TARGET_REGION_NUMBER_COLUMN, column_schemas, nullable_string,
    nullable_value,
};
use serde::Serialize;
use snafu::ResultExt;
use store_api::storage::{RegionId, TableId};

use crate::procedure::repartition::PersistentContext as RepartitionPersistentContext;
use crate::procedure::repartition::group::PersistentContext as GroupPersistentContext;
use crate::procedure::repartition::plan::{SourceRegionDescriptor, TargetRegionDescriptor};
use crate::procedure::repartition::repartition_start::RepartitionStart;

pub const REPARTITION_EVENT_TYPE: &str = "repartition";
pub const REPARTITION_GROUP_EVENT_TYPE: &str = "repartition_group";

const PAYLOAD_VERSION: u8 = 1;

#[derive(Debug, Serialize)]
struct RepartitionSubmittedPayload {
    version: u8,
    source: RepartitionSourcePayload,
    target_partition_exprs: Vec<String>,
    target_partition_columns: Option<Vec<String>>,
    #[serde(with = "humantime_serde")]
    timeout: Duration,
}

#[derive(Debug, Serialize)]
struct RepartitionSourcePayload {
    #[serde(rename = "type")]
    source_type: &'static str,
    partition_exprs: Vec<String>,
}

#[derive(Debug)]
pub(crate) struct RepartitionEvent {
    catalog_name: Option<String>,
    schema_name: Option<String>,
    table_name: Option<String>,
    table_id: Option<TableId>,
    payload: Option<RepartitionSubmittedPayload>,
}

impl RepartitionEvent {
    pub(crate) fn submitted(
        persistent_ctx: &RepartitionPersistentContext,
        start: &RepartitionStart,
    ) -> Self {
        let intent = start.submitted_intent();
        Self {
            catalog_name: Some(persistent_ctx.catalog_name.clone()),
            schema_name: Some(persistent_ctx.schema_name.clone()),
            table_name: Some(persistent_ctx.table_name.clone()),
            table_id: Some(persistent_ctx.table_id),
            payload: Some(RepartitionSubmittedPayload {
                version: PAYLOAD_VERSION,
                source: RepartitionSourcePayload {
                    source_type: intent.source_type(),
                    partition_exprs: intent
                        .source_partition_exprs()
                        .iter()
                        .map(ToString::to_string)
                        .collect(),
                },
                target_partition_exprs: intent
                    .target_partition_exprs()
                    .iter()
                    .map(ToString::to_string)
                    .collect(),
                target_partition_columns: intent.target_partition_columns().map(ToOwned::to_owned),
                timeout: persistent_ctx.timeout,
            }),
        }
    }

    pub(crate) fn lifecycle() -> Self {
        Self {
            catalog_name: None,
            schema_name: None,
            table_name: None,
            table_id: None,
            payload: None,
        }
    }

    fn schema() -> Vec<ColumnSchema> {
        column_schemas([
            &CATALOG_NAME_COLUMN,
            &SCHEMA_NAME_COLUMN,
            &TABLE_NAME_COLUMN,
            &TABLE_ID_COLUMN,
        ])
    }
}

impl Event for RepartitionEvent {
    fn event_type(&self) -> &str {
        REPARTITION_EVENT_TYPE
    }

    fn json_payload(&self) -> Result<serde_json::Value> {
        self.payload
            .as_ref()
            .map(serde_json::to_value)
            .transpose()
            .context(SerializeEventSnafu)
            .map(|payload| payload.unwrap_or(serde_json::Value::Null))
    }

    fn extra_schema(&self) -> Vec<ColumnSchema> {
        Self::schema()
    }

    fn extra_rows(&self) -> Result<Vec<Row>> {
        Ok(vec![Row {
            values: vec![
                nullable_string(self.catalog_name.as_deref()),
                nullable_string(self.schema_name.as_deref()),
                nullable_string(self.table_name.as_deref()),
                nullable_value(self.table_id.map(ValueData::U32Value)),
            ],
        }])
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(Debug, Serialize)]
struct RepartitionGroupSubmittedPayload {
    version: u8,
    sync_region: bool,
    allocated_region_ids: Vec<u64>,
    pending_deallocate_region_ids: Vec<u64>,
    #[serde(with = "humantime_serde")]
    timeout: Duration,
}

#[derive(Debug)]
struct RepartitionTopologyRow {
    source_region_id: Option<RegionId>,
    source_partition_expr: Option<String>,
    target_region_id: Option<RegionId>,
    target_partition_expr: Option<String>,
}

#[derive(Debug)]
pub(crate) struct RepartitionGroupEvent {
    catalog_name: Option<String>,
    schema_name: Option<String>,
    table_name: Option<String>,
    table_id: Option<TableId>,
    parent_procedure_id: Option<String>,
    group_id: Option<String>,
    topology: Vec<RepartitionTopologyRow>,
    payload: Option<RepartitionGroupSubmittedPayload>,
}

impl RepartitionGroupEvent {
    pub(crate) fn submitted(persistent_ctx: &GroupPersistentContext) -> Self {
        Self {
            catalog_name: Some(persistent_ctx.catalog_name.clone()),
            schema_name: Some(persistent_ctx.schema_name.clone()),
            table_name: persistent_ctx.table_name.clone(),
            table_id: Some(persistent_ctx.table_id),
            parent_procedure_id: persistent_ctx.parent_procedure_id.map(|id| id.to_string()),
            group_id: Some(persistent_ctx.group_id.to_string()),
            topology: topology_rows(
                &persistent_ctx.sources,
                &persistent_ctx.targets,
                &persistent_ctx.region_mapping,
            ),
            payload: Some(RepartitionGroupSubmittedPayload {
                version: PAYLOAD_VERSION,
                sync_region: persistent_ctx.sync_region,
                allocated_region_ids: persistent_ctx
                    .allocated_region_ids
                    .iter()
                    .map(|region_id| region_id.as_u64())
                    .collect(),
                pending_deallocate_region_ids: persistent_ctx
                    .pending_deallocate_region_ids
                    .iter()
                    .map(|region_id| region_id.as_u64())
                    .collect(),
                timeout: persistent_ctx.timeout,
            }),
        }
    }

    pub(crate) fn lifecycle() -> Self {
        Self {
            catalog_name: None,
            schema_name: None,
            table_name: None,
            table_id: None,
            parent_procedure_id: None,
            group_id: None,
            topology: vec![RepartitionTopologyRow {
                source_region_id: None,
                source_partition_expr: None,
                target_region_id: None,
                target_partition_expr: None,
            }],
            payload: None,
        }
    }

    fn schema() -> Vec<ColumnSchema> {
        let mut schema = RepartitionEvent::schema();
        schema.extend(column_schemas([
            &PARENT_PROCEDURE_ID_COLUMN,
            &REPARTITION_GROUP_ID_COLUMN,
            &SOURCE_REGION_ID_COLUMN,
            &SOURCE_REGION_NUMBER_COLUMN,
            &SOURCE_PARTITION_EXPR_COLUMN,
            &TARGET_REGION_ID_COLUMN,
            &TARGET_REGION_NUMBER_COLUMN,
            &TARGET_PARTITION_EXPR_COLUMN,
        ]));
        schema
    }
}

impl Event for RepartitionGroupEvent {
    fn event_type(&self) -> &str {
        REPARTITION_GROUP_EVENT_TYPE
    }

    fn json_payload(&self) -> Result<serde_json::Value> {
        self.payload
            .as_ref()
            .map(serde_json::to_value)
            .transpose()
            .context(SerializeEventSnafu)
            .map(|payload| payload.unwrap_or(serde_json::Value::Null))
    }

    fn extra_schema(&self) -> Vec<ColumnSchema> {
        Self::schema()
    }

    fn extra_rows(&self) -> Result<Vec<Row>> {
        Ok(self
            .topology
            .iter()
            .map(|topology| Row {
                values: vec![
                    nullable_string(self.catalog_name.as_deref()),
                    nullable_string(self.schema_name.as_deref()),
                    nullable_string(self.table_name.as_deref()),
                    nullable_value(self.table_id.map(ValueData::U32Value)),
                    nullable_string(self.parent_procedure_id.as_deref()),
                    nullable_string(self.group_id.as_deref()),
                    nullable_value(
                        topology
                            .source_region_id
                            .map(|id| ValueData::U64Value(id.as_u64())),
                    ),
                    nullable_value(
                        topology
                            .source_region_id
                            .map(|id| ValueData::U32Value(id.region_number())),
                    ),
                    nullable_string(topology.source_partition_expr.as_deref()),
                    nullable_value(
                        topology
                            .target_region_id
                            .map(|id| ValueData::U64Value(id.as_u64())),
                    ),
                    nullable_value(
                        topology
                            .target_region_id
                            .map(|id| ValueData::U32Value(id.region_number())),
                    ),
                    nullable_string(topology.target_partition_expr.as_deref()),
                ],
            })
            .collect())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

fn topology_rows(
    sources: &[SourceRegionDescriptor],
    targets: &[TargetRegionDescriptor],
    region_mapping: &HashMap<RegionId, Vec<RegionId>>,
) -> Vec<RepartitionTopologyRow> {
    let targets = targets
        .iter()
        .map(|target| (target.region_id, target))
        .collect::<HashMap<_, _>>();
    let mut rows = Vec::new();
    for source in sources {
        let source_region_id = source.region_id();
        let source_partition_expr = source.partition_expr().map(ToString::to_string);
        if let Some(target_ids) = region_mapping.get(&source_region_id) {
            for target_region_id in target_ids {
                let target = targets.get(target_region_id);
                rows.push(RepartitionTopologyRow {
                    source_region_id: Some(source_region_id),
                    source_partition_expr: source_partition_expr.clone(),
                    target_region_id: Some(*target_region_id),
                    target_partition_expr: target.map(|target| target.partition_expr.to_string()),
                });
            }
        }
    }
    if rows.is_empty() {
        rows.push(RepartitionTopologyRow {
            source_region_id: None,
            source_partition_expr: None,
            target_region_id: None,
            target_partition_expr: None,
        });
    }
    rows
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::time::Duration;

    use common_event_recorder::Event;
    use datatypes::value::Value;
    use partition::expr::col;
    use table::table_name::TableName;

    use super::*;
    use crate::procedure::repartition::repartition_start::RepartitionFrom;

    fn expr(start: i64, end: i64) -> partition::expr::PartitionExpr {
        col("host")
            .gt_eq(Value::Int64(start))
            .and(col("host").lt(Value::Int64(end)))
    }

    fn parent_persistent_ctx() -> RepartitionPersistentContext {
        RepartitionPersistentContext::new(
            TableName::new("greptime", "public", "repartition_events"),
            1024,
            Some(Duration::from_secs(30)),
        )
    }

    #[test]
    fn test_parent_submitted_payload_preserves_repartition_semantics() {
        let unpartitioned = RepartitionEvent::submitted(
            &parent_persistent_ctx(),
            &RepartitionStart::new(
                RepartitionFrom::Unpartitioned {
                    partition_columns: vec!["host".to_string()],
                },
                vec![expr(0, 100)],
            ),
        );
        let partitioned = RepartitionEvent::submitted(
            &parent_persistent_ctx(),
            &RepartitionStart::new(
                RepartitionFrom::Partitioned {
                    exprs: vec![expr(0, 100)],
                    target_partition_columns: Some(vec!["host".to_string()]),
                },
                vec![expr(0, 50), expr(50, 100)],
            ),
        );

        let unpartitioned_payload = unpartitioned.json_payload().unwrap();
        let partitioned_payload = partitioned.json_payload().unwrap();
        assert_eq!(unpartitioned_payload["source"]["type"], "unpartitioned");
        assert_eq!(
            unpartitioned_payload["source"]["partition_exprs"],
            serde_json::json!([])
        );
        assert_eq!(
            unpartitioned_payload["target_partition_columns"],
            serde_json::json!(["host"])
        );
        assert_eq!(partitioned_payload["source"]["type"], "partitioned");
        assert_eq!(
            partitioned_payload["source"]["partition_exprs"],
            serde_json::json!([expr(0, 100).to_string()])
        );
        assert_eq!(
            partitioned_payload["target_partition_columns"],
            serde_json::json!(["host"])
        );
    }

    #[test]
    fn test_topology_rows_expand_one_source_to_multiple_targets() {
        let table_id = 1024;
        let source = RegionId::new(table_id, 1);
        let left = RegionId::new(table_id, 2);
        let right = RegionId::new(table_id, 3);
        let source_expr = expr(0, 100);
        let targets = vec![
            TargetRegionDescriptor {
                region_id: left,
                partition_expr: expr(0, 50),
            },
            TargetRegionDescriptor {
                region_id: right,
                partition_expr: expr(50, 100),
            },
        ];
        let rows = topology_rows(
            &[SourceRegionDescriptor::partitioned(
                source,
                source_expr.clone(),
            )],
            &targets,
            &HashMap::from([(source, vec![left, right])]),
        );

        assert_eq!(rows.len(), 2);
        assert!(rows.iter().all(|row| row.source_region_id == Some(source)));
        assert_eq!(rows[0].source_partition_expr, Some(source_expr.to_string()));
        assert_eq!(rows[0].target_region_id, Some(left));
        assert_eq!(rows[1].target_region_id, Some(right));
    }

    #[test]
    fn test_topology_rows_expand_multiple_sources_to_one_target() {
        let table_id = 1024;
        let left = RegionId::new(table_id, 1);
        let right = RegionId::new(table_id, 2);
        let merged = RegionId::new(table_id, 3);
        let rows = topology_rows(
            &[
                SourceRegionDescriptor::partitioned(left, expr(0, 50)),
                SourceRegionDescriptor::partitioned(right, expr(50, 100)),
            ],
            &[TargetRegionDescriptor {
                region_id: merged,
                partition_expr: expr(0, 100),
            }],
            &HashMap::from([(left, vec![merged]), (right, vec![merged])]),
        );

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].source_region_id, Some(left));
        assert_eq!(rows[1].source_region_id, Some(right));
        assert!(rows.iter().all(|row| row.target_region_id == Some(merged)));
    }

    #[test]
    fn test_topology_rows_keep_default_source_expr_null() {
        let table_id = 1024;
        let source = RegionId::new(table_id, 0);
        let target = RegionId::new(table_id, 1);
        let rows = topology_rows(
            &[SourceRegionDescriptor::Default { region_id: source }],
            &[TargetRegionDescriptor {
                region_id: target,
                partition_expr: expr(0, 100),
            }],
            &HashMap::from([(source, vec![target])]),
        );

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].source_partition_expr, None);
        assert_eq!(rows[0].source_region_id.unwrap().region_number(), 0);
        assert_eq!(rows[0].target_region_id.unwrap().region_number(), 1);
        assert_eq!(
            rows[0].target_partition_expr,
            Some(expr(0, 100).to_string())
        );
    }

    #[test]
    fn test_topology_rows_fall_back_for_empty_mapping() {
        let rows = topology_rows(&[], &[], &HashMap::new());

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].source_region_id, None);
        assert_eq!(rows[0].target_region_id, None);
    }

    #[test]
    fn test_lifecycle_events_are_lightweight() {
        let events: Vec<Box<dyn Event>> = vec![
            Box::new(RepartitionEvent::lifecycle()),
            Box::new(RepartitionGroupEvent::lifecycle()),
        ];

        for event in events {
            assert_eq!(event.json_payload().unwrap(), serde_json::Value::Null);
            assert!(
                event.extra_rows().unwrap()[0]
                    .values
                    .iter()
                    .all(|value| value.value_data.is_none())
            );
        }
    }
}
