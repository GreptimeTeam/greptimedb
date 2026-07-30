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
use std::collections::BTreeSet;
use std::time::Duration;

use api::v1::value::ValueData;
use api::v1::{ColumnSchema, Row, Value};
use common_event_recorder::Event;
use common_event_recorder::error::{Result, SerializeEventSnafu};
use common_event_recorder::event_table::{
    GC_REPORT_COLUMN, REGION_ID_COLUMN, REGION_NUMBER_COLUMN, TABLE_ID_COLUMN, column_schemas,
    nullable_json,
};
use serde::Serialize;
use snafu::ResultExt;
use store_api::storage::{GcReport, IndexVersion, RegionId};

/// Procedure event type for batch garbage collection.
pub(crate) const BATCH_GC_EVENT_TYPE: &str = "batch_gc";

const PAYLOAD_VERSION: u8 = 1;

#[derive(Debug, Serialize)]
struct BatchGcPayload {
    version: u8,
    regions: Vec<RegionId>,
    full_file_listing: bool,
    #[serde(with = "humantime_serde")]
    timeout: Duration,
}

#[derive(Debug, Serialize)]
struct DeletedIndexPayload {
    file_id: String,
    index_version: IndexVersion,
}

#[derive(Debug, Serialize)]
struct BatchGcRegionReport {
    #[serde(skip_serializing_if = "Vec::is_empty")]
    deleted_files: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    deleted_indexes: Vec<DeletedIndexPayload>,
    need_retry: bool,
}

#[derive(Debug)]
struct BatchGcRegionRow {
    region_id: RegionId,
    report: BatchGcRegionReport,
}

#[derive(Debug)]
pub(crate) struct BatchGcEvent {
    payload: Option<BatchGcPayload>,
    // None emits a procedure-level lifecycle row with null Region dimensions.
    regions: Option<Vec<BatchGcRegionRow>>,
}

impl BatchGcEvent {
    pub(crate) fn with_config(
        regions: &[RegionId],
        full_file_listing: bool,
        timeout: Duration,
    ) -> Self {
        Self {
            payload: Some(BatchGcPayload {
                version: PAYLOAD_VERSION,
                regions: regions.to_vec(),
                full_file_listing,
                timeout,
            }),
            regions: None,
        }
    }

    /// Returns None when the GC report contains no deleted files, indexes, or retries.
    pub(crate) fn succeeded(report: &GcReport) -> Option<Self> {
        let mut region_ids = report
            .deleted_files
            .keys()
            .copied()
            .collect::<BTreeSet<_>>();
        region_ids.extend(report.deleted_indexes.keys().copied());
        region_ids.extend(report.need_retry_regions.iter().copied());

        let regions: Vec<_> = region_ids
            .into_iter()
            .filter_map(|region_id| {
                region_report(region_id, report)
                    .map(|report| BatchGcRegionRow { region_id, report })
            })
            .collect();

        (!regions.is_empty()).then_some(Self {
            payload: None,
            regions: Some(regions),
        })
    }
}

impl Event for BatchGcEvent {
    fn event_type(&self) -> &str {
        BATCH_GC_EVENT_TYPE
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
        schema()
    }

    fn extra_rows(&self) -> Result<Vec<Row>> {
        let Some(regions) = &self.regions else {
            return Ok(vec![null_row()]);
        };

        regions
            .iter()
            .map(|region| {
                let report = serde_json::to_value(&region.report).context(SerializeEventSnafu)?;
                Ok(Row {
                    values: vec![
                        ValueData::U64Value(region.region_id.as_u64()).into(),
                        ValueData::U32Value(region.region_id.table_id()).into(),
                        ValueData::U32Value(region.region_id.region_number()).into(),
                        nullable_json(Some(&report)),
                    ],
                })
            })
            .collect()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

fn schema() -> Vec<ColumnSchema> {
    column_schemas([
        &REGION_ID_COLUMN,
        &TABLE_ID_COLUMN,
        &REGION_NUMBER_COLUMN,
        &GC_REPORT_COLUMN,
    ])
}

fn null_row() -> Row {
    Row {
        values: (0..schema().len())
            .map(|_| Value { value_data: None })
            .collect(),
    }
}

fn region_report(region_id: RegionId, report: &GcReport) -> Option<BatchGcRegionReport> {
    let deleted_files = report
        .deleted_files
        .get(&region_id)
        .into_iter()
        .flatten()
        .map(ToString::to_string)
        .collect::<Vec<_>>();

    let deleted_indexes = report
        .deleted_indexes
        .get(&region_id)
        .into_iter()
        .flatten()
        .map(|(file_id, index_version)| DeletedIndexPayload {
            file_id: file_id.to_string(),
            index_version: *index_version,
        })
        .collect::<Vec<_>>();

    let need_retry = report.need_retry_regions.contains(&region_id);
    if deleted_files.is_empty() && deleted_indexes.is_empty() && !need_retry {
        return None;
    }

    Some(BatchGcRegionReport {
        deleted_files,
        deleted_indexes,
        need_retry,
    })
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::sync::Arc;

    use api::v1::ColumnSchema;
    use common_event_recorder::EventTypeFilter;
    use common_event_recorder::event_table::{
        PROCEDURE_ERROR_COLUMN, PROCEDURE_ID_COLUMN, PROCEDURE_STATE_COLUMN,
        PROCEDURE_TRIGGER_COLUMN,
    };
    use common_event_recorder::testing::assert_event_contract;
    use common_meta::key::TableMetadataManager;
    use common_meta::kv_backend::memory::MemoryKvBackend;
    use common_meta::sequence::SequenceBuilder;
    use common_procedure::{
        EventContext, EventTrigger, Procedure, ProcedureEvent, ProcedureId, ProcedureState,
        RetryPhase,
    };
    use store_api::storage::FileId;

    use super::*;
    use crate::gc::BatchGcProcedure;
    use crate::procedure::test_util::MailboxContext;

    #[test]
    fn test_batch_gc_lifecycle_event_contract() {
        let first = RegionId::new(1024, 1);
        let second = RegionId::new(1024, 2);
        let event =
            BatchGcEvent::with_config(&[second, first, second], true, Duration::from_secs(10));

        assert_event_contract(&event, BATCH_GC_EVENT_TYPE, &schema(), &[null_row()]);
        assert_eq!(
            event.json_payload().unwrap(),
            serde_json::json!({
                "version": 1,
                "regions": [second.as_u64(), first.as_u64(), second.as_u64()],
                "full_file_listing": true,
                "timeout": "10s",
            })
        );
    }

    #[test]
    fn test_batch_gc_succeeded_event_contract() {
        let first = RegionId::new(1024, 1);
        let second = RegionId::new(1024, 2);
        let third = RegionId::new(1024, 3);
        let first_file = FileId::parse_str("00000000-0000-0000-0000-000000000001").unwrap();
        let second_file = FileId::parse_str("00000000-0000-0000-0000-000000000002").unwrap();
        let index_file = FileId::parse_str("00000000-0000-0000-0000-000000000003").unwrap();
        let report = GcReport {
            deleted_files: HashMap::from([(first, vec![second_file, first_file])]),
            deleted_indexes: HashMap::from([(second, vec![(index_file, 42)])]),
            processed_regions: HashSet::from([first, second]),
            need_retry_regions: HashSet::from([third]),
        };

        let event = BatchGcEvent::succeeded(&report).unwrap();

        assert_event_contract(
            &event,
            BATCH_GC_EVENT_TYPE,
            &schema(),
            &[
                region_row(
                    first,
                    Some(serde_json::json!({
                        "deleted_files": [
                            "00000000-0000-0000-0000-000000000002",
                            "00000000-0000-0000-0000-000000000001",
                        ],
                        "need_retry": false,
                    })),
                ),
                region_row(
                    second,
                    Some(serde_json::json!({
                        "deleted_indexes": [{
                            "file_id": "00000000-0000-0000-0000-000000000003",
                            "index_version": 42,
                        }],
                        "need_retry": false,
                    })),
                ),
                region_row(
                    third,
                    Some(serde_json::json!({
                        "need_retry": true,
                    })),
                ),
            ],
        );
        assert_eq!(event.json_payload().unwrap(), serde_json::Value::Null);
    }

    #[test]
    fn test_batch_gc_succeeded_event_skips_noop_regions() {
        let noop = RegionId::new(1024, 1);
        let retry = RegionId::new(1024, 2);
        let event = BatchGcEvent::succeeded(&GcReport {
            processed_regions: HashSet::from([noop]),
            need_retry_regions: HashSet::from([retry]),
            ..Default::default()
        })
        .unwrap();

        assert_event_contract(
            &event,
            BATCH_GC_EVENT_TYPE,
            &schema(),
            &[region_row(
                retry,
                Some(serde_json::json!({
                    "need_retry": true,
                })),
            )],
        );
    }

    #[test]
    fn test_batch_gc_event_filter() {
        let procedure = batch_gc_procedure();
        let running = ProcedureState::Running;
        let event_context = |trigger, lifecycle_state, event_type_filter| EventContext {
            procedure_id: ProcedureId::random(),
            lifecycle_state,
            trigger,
            event_type_filter: Arc::new(event_type_filter),
        };

        assert!(
            procedure
                .event(&event_context(
                    EventTrigger::Submitted,
                    &running,
                    EventTypeFilter::All,
                ))
                .is_none()
        );

        let report = GcReport {
            processed_regions: HashSet::from([RegionId::new(1024, 1)]),
            ..Default::default()
        };
        let done = ProcedureState::Done {
            output: Some(Arc::new(report)),
        };
        assert!(
            procedure
                .event(&event_context(
                    EventTrigger::Succeeded,
                    &done,
                    EventTypeFilter::All,
                ))
                .is_none()
        );

        assert!(
            procedure
                .event(&event_context(
                    EventTrigger::Recovered,
                    &running,
                    EventTypeFilter::All,
                ))
                .is_none()
        );

        let retrying = procedure
            .event(&event_context(
                EventTrigger::Retrying {
                    phase: RetryPhase::Execute,
                    attempt: 1,
                },
                &running,
                EventTypeFilter::All,
            ))
            .unwrap();
        assert_eq!(
            retrying.json_payload().unwrap(),
            serde_json::json!({
                "version": 1,
                "regions": [RegionId::new(1024, 1).as_u64()],
                "full_file_listing": true,
                "timeout": "10s",
            })
        );
        assert_eq!(retrying.extra_rows().unwrap(), vec![null_row()]);

        assert!(
            procedure
                .event(&event_context(
                    EventTrigger::Recovered,
                    &running,
                    EventTypeFilter::Only(HashSet::from(["another_event".to_string()])),
                ))
                .is_none()
        );
        assert!(
            procedure
                .event(&event_context(
                    EventTrigger::Recovered,
                    &running,
                    EventTypeFilter::Only(HashSet::new()),
                ))
                .is_none()
        );

        let missing = ProcedureState::Done { output: None };
        assert!(
            procedure
                .event(&event_context(
                    EventTrigger::Succeeded,
                    &missing,
                    EventTypeFilter::All,
                ))
                .is_none()
        );
        let wrong = ProcedureState::Done {
            output: Some(Arc::new("not a GC report".to_string())),
        };
        assert!(
            procedure
                .event(&event_context(
                    EventTrigger::Succeeded,
                    &wrong,
                    EventTypeFilter::All,
                ))
                .is_none()
        );
    }

    #[test]
    fn test_batch_gc_procedure_event_contract() {
        let procedure_id = ProcedureId::parse_str("00000000-0000-0000-0000-000000000001").unwrap();
        let region_id = RegionId::new(1024, 1);
        let file_id = FileId::parse_str("00000000-0000-0000-0000-000000000001").unwrap();
        let internal = BatchGcEvent::succeeded(&GcReport {
            deleted_files: HashMap::from([(region_id, vec![file_id])]),
            ..Default::default()
        })
        .unwrap();
        let event = ProcedureEvent::new(
            procedure_id,
            Box::new(internal),
            ProcedureState::Done { output: None },
            EventTrigger::Succeeded,
        );
        let mut event_schema = procedure_schema();
        event_schema.extend(schema());
        let mut values = vec![
            ValueData::StringValue(procedure_id.to_string()).into(),
            ValueData::StringValue("Done".to_string()).into(),
            ValueData::StringValue(String::new()).into(),
            ValueData::StringValue("Succeeded".to_string()).into(),
        ];
        values.extend(
            region_row(
                region_id,
                Some(serde_json::json!({
                    "deleted_files": ["00000000-0000-0000-0000-000000000001"],
                    "need_retry": false,
                })),
            )
            .values,
        );

        assert_event_contract(
            &event,
            BATCH_GC_EVENT_TYPE,
            &event_schema,
            &[Row { values }],
        );
    }

    fn procedure_schema() -> Vec<ColumnSchema> {
        column_schemas([
            &PROCEDURE_ID_COLUMN,
            &PROCEDURE_STATE_COLUMN,
            &PROCEDURE_ERROR_COLUMN,
            &PROCEDURE_TRIGGER_COLUMN,
        ])
    }

    fn region_row(region_id: RegionId, report: Option<serde_json::Value>) -> Row {
        Row {
            values: vec![
                ValueData::U64Value(region_id.as_u64()).into(),
                ValueData::U32Value(region_id.table_id()).into(),
                ValueData::U32Value(region_id.region_number()).into(),
                nullable_json(report.as_ref()),
            ],
        }
    }

    fn batch_gc_procedure() -> BatchGcProcedure {
        let kv_backend = Arc::new(MemoryKvBackend::new());
        let table_metadata_manager = Arc::new(TableMetadataManager::new(kv_backend.clone()));
        let mailbox_sequence = SequenceBuilder::new("test_batch_gc_event", kv_backend).build();
        let mailbox = MailboxContext::new(mailbox_sequence);
        BatchGcProcedure::new(
            mailbox.mailbox().clone(),
            table_metadata_manager,
            "localhost".to_string(),
            vec![RegionId::new(1024, 1)],
            true,
            Duration::from_secs(10),
            HashMap::new(),
        )
    }
}
