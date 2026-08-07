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

pub(crate) const REPARTITION_EVENT_TYPE: &str = "repartition";
pub(crate) const REPARTITION_GROUP_EVENT_TYPE: &str = "repartition_group";

const PAYLOAD_VERSION: u8 = 2;

#[derive(Debug, Serialize)]
struct RepartitionSubmittedPayload {
    version: u8,
    source_type: &'static str,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    source_partition_exprs: Vec<String>,
    target_partition_exprs: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    target_partition_columns: Option<Vec<String>>,
    #[serde(with = "humantime_serde")]
    timeout: Duration,
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
                source_type: intent.source_type(),
                source_partition_exprs: intent
                    .source_partition_exprs()
                    .iter()
                    .map(|expr| expr.to_string())
                    .collect(),
                target_partition_exprs: intent
                    .target_partition_exprs()
                    .iter()
                    .map(|expr| expr.to_string())
                    .collect(),
                target_partition_columns: intent.target_partition_columns().map(ToOwned::to_owned),
                timeout: persistent_ctx.timeout,
            }),
        }
    }

    pub(crate) fn lifecycle(persistent_ctx: &RepartitionPersistentContext) -> Self {
        Self {
            catalog_name: Some(persistent_ctx.catalog_name.clone()),
            schema_name: Some(persistent_ctx.schema_name.clone()),
            table_name: Some(persistent_ctx.table_name.clone()),
            table_id: Some(persistent_ctx.table_id),
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
struct RepartitionTopology {
    sources: Vec<RepartitionTopologySource>,
    target_partition_exprs: HashMap<RegionId, String>,
    region_mapping: HashMap<RegionId, Vec<RegionId>>,
}

#[derive(Debug)]
struct RepartitionTopologySource {
    region_id: RegionId,
    partition_expr: Option<String>,
}

#[derive(Debug)]
struct RepartitionTopologyRow<'a> {
    source_region_id: RegionId,
    source_partition_expr: Option<&'a str>,
    target_region_id: RegionId,
    target_partition_expr: Option<&'a str>,
}

impl RepartitionTopology {
    fn new(
        sources: &[SourceRegionDescriptor],
        targets: &[TargetRegionDescriptor],
        region_mapping: &HashMap<RegionId, Vec<RegionId>>,
    ) -> Self {
        Self {
            sources: sources
                .iter()
                .map(|source| RepartitionTopologySource {
                    region_id: source.region_id(),
                    partition_expr: source.partition_expr().map(|expr| expr.to_string()),
                })
                .collect(),
            target_partition_exprs: targets
                .iter()
                .map(|target| (target.region_id, target.partition_expr.to_string()))
                .collect(),
            region_mapping: region_mapping.clone(),
        }
    }

    fn rows(&self) -> impl Iterator<Item = RepartitionTopologyRow<'_>> {
        self.sources.iter().flat_map(|source| {
            self.region_mapping
                .get(&source.region_id)
                .into_iter()
                .flatten()
                .map(|target_region_id| RepartitionTopologyRow {
                    source_region_id: source.region_id,
                    source_partition_expr: source.partition_expr.as_deref(),
                    target_region_id: *target_region_id,
                    target_partition_expr: self
                        .target_partition_exprs
                        .get(target_region_id)
                        .map(String::as_str),
                })
        })
    }
}

#[derive(Debug)]
pub(crate) struct RepartitionGroupEvent {
    catalog_name: Option<String>,
    schema_name: Option<String>,
    table_name: Option<String>,
    table_id: Option<TableId>,
    parent_procedure_id: Option<String>,
    group_id: Option<String>,
    topology: Option<RepartitionTopology>,
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
            topology: Some(RepartitionTopology::new(
                &persistent_ctx.sources,
                &persistent_ctx.targets,
                &persistent_ctx.region_mapping,
            )),
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

    pub(crate) fn lifecycle(persistent_ctx: &GroupPersistentContext) -> Self {
        Self {
            catalog_name: Some(persistent_ctx.catalog_name.clone()),
            schema_name: Some(persistent_ctx.schema_name.clone()),
            table_name: persistent_ctx.table_name.clone(),
            table_id: Some(persistent_ctx.table_id),
            parent_procedure_id: persistent_ctx.parent_procedure_id.map(|id| id.to_string()),
            group_id: Some(persistent_ctx.group_id.to_string()),
            topology: None,
            payload: None,
        }
    }

    fn extra_row(&self, topology: Option<RepartitionTopologyRow<'_>>) -> Row {
        let (source_region_id, source_partition_expr, target_region_id, target_partition_expr) =
            match topology {
                Some(topology) => (
                    Some(topology.source_region_id),
                    topology.source_partition_expr,
                    Some(topology.target_region_id),
                    topology.target_partition_expr,
                ),
                None => (None, None, None, None),
            };

        Row {
            values: vec![
                nullable_string(self.catalog_name.as_deref()),
                nullable_string(self.schema_name.as_deref()),
                nullable_string(self.table_name.as_deref()),
                nullable_value(self.table_id.map(ValueData::U32Value)),
                nullable_string(self.parent_procedure_id.as_deref()),
                nullable_string(self.group_id.as_deref()),
                nullable_value(source_region_id.map(|id| ValueData::U64Value(id.as_u64()))),
                nullable_value(source_region_id.map(|id| ValueData::U32Value(id.region_number()))),
                nullable_string(source_partition_expr),
                nullable_value(target_region_id.map(|id| ValueData::U64Value(id.as_u64()))),
                nullable_value(target_region_id.map(|id| ValueData::U32Value(id.region_number()))),
                nullable_string(target_partition_expr),
            ],
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
        match &self.topology {
            Some(topology) => Ok(topology
                .rows()
                .map(|row| self.extra_row(Some(row)))
                .collect()),
            None => Ok(vec![self.extra_row(None)]),
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::time::Duration;

    use api::v1::value::ValueData;
    use api::v1::{ColumnSchema, Row};
    use common_event_recorder::Event;
    use common_event_recorder::event_table::{
        PROCEDURE_ERROR_COLUMN, PROCEDURE_ID_COLUMN, PROCEDURE_STATE_COLUMN,
        PROCEDURE_TRIGGER_COLUMN, jsonb_value,
    };
    use common_event_recorder::testing::assert_event_contract;
    use common_procedure::{EventTrigger, ProcedureEvent, ProcedureId, ProcedureState};
    use table::table_name::TableName;
    use uuid::Uuid;

    use super::*;
    use crate::procedure::repartition::repartition_start::RepartitionFrom;
    use crate::procedure::repartition::test_util::{new_persistent_context, range_expr};

    fn expr(start: i64, end: i64) -> partition::expr::PartitionExpr {
        range_expr("host", start, end)
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

        assert_event_contract(
            &unpartitioned,
            REPARTITION_EVENT_TYPE,
            &parent_schema(),
            &[Row {
                values: vec![
                    ValueData::StringValue("greptime".to_string()).into(),
                    ValueData::StringValue("public".to_string()).into(),
                    ValueData::StringValue("repartition_events".to_string()).into(),
                    ValueData::U32Value(1024).into(),
                ],
            }],
        );

        let unpartitioned_payload = unpartitioned.json_payload().unwrap();
        let partitioned_payload = partitioned.json_payload().unwrap();
        assert_eq!(unpartitioned_payload["version"], PAYLOAD_VERSION);
        assert_eq!(unpartitioned_payload["source_type"], "unpartitioned");
        assert!(
            unpartitioned_payload
                .get("source_partition_exprs")
                .is_none()
        );
        assert_eq!(
            unpartitioned_payload["target_partition_columns"],
            serde_json::json!(["host"])
        );
        assert_eq!(partitioned_payload["source_type"], "partitioned");
        assert_eq!(
            partitioned_payload["source_partition_exprs"],
            serde_json::json!([expr(0, 100).to_string()])
        );
        assert_eq!(
            partitioned_payload["target_partition_columns"],
            serde_json::json!(["host"])
        );

        let merge = RepartitionEvent::submitted(
            &parent_persistent_ctx(),
            &RepartitionStart::new(
                RepartitionFrom::Partitioned {
                    exprs: vec![expr(0, 50), expr(50, 100)],
                    target_partition_columns: None,
                },
                vec![expr(0, 100)],
            ),
        );
        assert!(
            merge
                .json_payload()
                .unwrap()
                .get("target_partition_columns")
                .is_none()
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
        let topology = RepartitionTopology::new(
            &[SourceRegionDescriptor::partitioned(
                source,
                source_expr.clone(),
            )],
            &targets,
            &HashMap::from([(source, vec![left, right])]),
        );
        let rows = topology.rows().collect::<Vec<_>>();
        let source_expr = source_expr.to_string();

        assert_eq!(rows.len(), 2);
        assert!(rows.iter().all(|row| row.source_region_id == source));
        assert_eq!(rows[0].source_partition_expr, Some(source_expr.as_str()));
        assert_eq!(rows[0].target_region_id, left);
        assert_eq!(rows[1].target_region_id, right);
    }

    #[test]
    fn test_group_submitted_event_contract() {
        let table_id = 1024;
        let source = RegionId::new(table_id, 1);
        let left = RegionId::new(table_id, 2);
        let right = RegionId::new(table_id, 3);
        let source_expr = expr(0, 100);
        let left_expr = expr(0, 50);
        let right_expr = expr(50, 100);
        let parent_procedure_id =
            ProcedureId::parse_str("00000000-0000-0000-0000-000000000001").unwrap();
        let group_id = Uuid::parse_str("00000000-0000-0000-0000-000000000002").unwrap();
        let mut persistent_ctx = new_persistent_context(
            table_id,
            vec![SourceRegionDescriptor::partitioned(
                source,
                source_expr.clone(),
            )],
            vec![
                TargetRegionDescriptor {
                    region_id: left,
                    partition_expr: left_expr.clone(),
                },
                TargetRegionDescriptor {
                    region_id: right,
                    partition_expr: right_expr.clone(),
                },
            ],
        );
        persistent_ctx.parent_procedure_id = Some(parent_procedure_id);
        persistent_ctx.group_id = group_id;
        persistent_ctx.region_mapping = HashMap::from([(source, vec![left, right])]);

        let event = RepartitionGroupEvent::submitted(&persistent_ctx);

        assert_event_contract(
            &event,
            REPARTITION_GROUP_EVENT_TYPE,
            &group_schema(),
            &[
                group_row(
                    parent_procedure_id,
                    group_id,
                    source,
                    &source_expr.to_string(),
                    left,
                    &left_expr.to_string(),
                ),
                group_row(
                    parent_procedure_id,
                    group_id,
                    source,
                    &source_expr.to_string(),
                    right,
                    &right_expr.to_string(),
                ),
            ],
        );
    }

    #[test]
    fn test_topology_rows_expand_multiple_sources_to_one_target() {
        let table_id = 1024;
        let left = RegionId::new(table_id, 1);
        let right = RegionId::new(table_id, 2);
        let merged = RegionId::new(table_id, 3);
        let topology = RepartitionTopology::new(
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
        let rows = topology.rows().collect::<Vec<_>>();

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].source_region_id, left);
        assert_eq!(rows[1].source_region_id, right);
        assert!(rows.iter().all(|row| row.target_region_id == merged));
    }

    #[test]
    fn test_topology_rows_keep_default_source_expr_null() {
        let table_id = 1024;
        let source = RegionId::new(table_id, 0);
        let target = RegionId::new(table_id, 1);
        let topology = RepartitionTopology::new(
            &[SourceRegionDescriptor::Default { region_id: source }],
            &[TargetRegionDescriptor {
                region_id: target,
                partition_expr: expr(0, 100),
            }],
            &HashMap::from([(source, vec![target])]),
        );
        let rows = topology.rows().collect::<Vec<_>>();
        let target_partition_expr = expr(0, 100).to_string();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].source_partition_expr, None);
        assert_eq!(rows[0].source_region_id.region_number(), 0);
        assert_eq!(rows[0].target_region_id.region_number(), 1);
        assert_eq!(
            rows[0].target_partition_expr,
            Some(target_partition_expr.as_str())
        );
    }

    #[test]
    fn test_topology_rows_skip_empty_mapping() {
        let topology = RepartitionTopology::new(&[], &[], &HashMap::new());

        assert!(topology.rows().next().is_none());
    }

    #[test]
    fn test_lifecycle_events_preserve_locators_and_null_payloads() {
        let parent_ctx = parent_persistent_ctx();
        let parent = RepartitionEvent::lifecycle(&parent_ctx);
        assert_event_contract(
            &parent,
            REPARTITION_EVENT_TYPE,
            &parent_schema(),
            &[Row {
                values: vec![
                    ValueData::StringValue("greptime".to_string()).into(),
                    ValueData::StringValue("public".to_string()).into(),
                    ValueData::StringValue("repartition_events".to_string()).into(),
                    ValueData::U32Value(1024).into(),
                ],
            }],
        );
        assert_eq!(parent.json_payload().unwrap(), serde_json::Value::Null);

        let group_ctx = new_persistent_context(1024, vec![], vec![]);
        let group = RepartitionGroupEvent::lifecycle(&group_ctx);
        assert_event_contract(
            &group,
            REPARTITION_GROUP_EVENT_TYPE,
            &group_schema(),
            &[group.extra_row(None)],
        );
        assert_eq!(group.json_payload().unwrap(), serde_json::Value::Null);
    }

    #[test]
    fn test_repartition_event_preserves_procedure_envelope_contract() {
        let procedure_id = ProcedureId::parse_str("00000000-0000-0000-0000-000000000001").unwrap();
        let event = ProcedureEvent::new(
            procedure_id,
            Box::new(RepartitionEvent::submitted(
                &parent_persistent_ctx(),
                &RepartitionStart::new(
                    RepartitionFrom::Unpartitioned {
                        partition_columns: vec!["host".to_string()],
                    },
                    vec![expr(0, 100)],
                ),
            )),
            ProcedureState::Running,
            EventTrigger::Submitted,
        );
        let mut schema = procedure_schema();
        schema.extend(parent_schema());

        assert_event_contract(
            &event,
            REPARTITION_EVENT_TYPE,
            &schema,
            &[Row {
                values: vec![
                    ValueData::StringValue(procedure_id.to_string()).into(),
                    ValueData::StringValue("Running".to_string()).into(),
                    ValueData::StringValue(String::new()).into(),
                    jsonb_value(&serde_json::json!({"type": "Submitted"})),
                    ValueData::StringValue("greptime".to_string()).into(),
                    ValueData::StringValue("public".to_string()).into(),
                    ValueData::StringValue("repartition_events".to_string()).into(),
                    ValueData::U32Value(1024).into(),
                ],
            }],
        );
    }

    fn parent_schema() -> Vec<ColumnSchema> {
        column_schemas([
            &CATALOG_NAME_COLUMN,
            &SCHEMA_NAME_COLUMN,
            &TABLE_NAME_COLUMN,
            &TABLE_ID_COLUMN,
        ])
    }

    fn group_schema() -> Vec<ColumnSchema> {
        let mut schema = parent_schema();
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

    fn procedure_schema() -> Vec<ColumnSchema> {
        column_schemas([
            &PROCEDURE_ID_COLUMN,
            &PROCEDURE_STATE_COLUMN,
            &PROCEDURE_ERROR_COLUMN,
            &PROCEDURE_TRIGGER_COLUMN,
        ])
    }

    fn group_row(
        parent_procedure_id: ProcedureId,
        group_id: Uuid,
        source_region_id: RegionId,
        source_partition_expr: &str,
        target_region_id: RegionId,
        target_partition_expr: &str,
    ) -> Row {
        Row {
            values: vec![
                ValueData::StringValue("test_catalog".to_string()).into(),
                ValueData::StringValue("test_schema".to_string()).into(),
                ValueData::StringValue("test_table".to_string()).into(),
                ValueData::U32Value(source_region_id.table_id()).into(),
                ValueData::StringValue(parent_procedure_id.to_string()).into(),
                ValueData::StringValue(group_id.to_string()).into(),
                ValueData::U64Value(source_region_id.as_u64()).into(),
                ValueData::U32Value(source_region_id.region_number()).into(),
                ValueData::StringValue(source_partition_expr.to_string()).into(),
                ValueData::U64Value(target_region_id.as_u64()).into(),
                ValueData::U32Value(target_region_id.region_number()).into(),
                ValueData::StringValue(target_partition_expr.to_string()).into(),
            ],
        }
    }
}
