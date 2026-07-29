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
use api::v1::{ColumnSchema, Row};
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
struct BatchGcSubmittedPayload {
    version: u8,
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
    processed: bool,
    need_retry: bool,
}

#[derive(Debug)]
struct BatchGcRegionRow {
    region_id: RegionId,
    report: Option<BatchGcRegionReport>,
}

#[derive(Debug)]
pub(crate) struct BatchGcEvent {
    payload: Option<BatchGcSubmittedPayload>,
    regions: Vec<BatchGcRegionRow>,
}

impl BatchGcEvent {
    pub(crate) fn submitted(
        regions: &[RegionId],
        full_file_listing: bool,
        timeout: Duration,
    ) -> Self {
        Self {
            payload: Some(BatchGcSubmittedPayload {
                version: PAYLOAD_VERSION,
                full_file_listing,
                timeout,
            }),
            regions: lifecycle_rows(regions),
        }
    }

    pub(crate) fn succeeded(regions: &[RegionId], report: &GcReport) -> Self {
        let mut region_ids = regions.iter().copied().collect::<BTreeSet<_>>();
        region_ids.extend(report.deleted_files.keys().copied());
        region_ids.extend(report.deleted_indexes.keys().copied());
        region_ids.extend(report.processed_regions.iter().copied());
        region_ids.extend(report.need_retry_regions.iter().copied());

        let regions = region_ids
            .into_iter()
            .map(|region_id| BatchGcRegionRow {
                region_id,
                report: region_report(region_id, report),
            })
            .collect();

        Self {
            payload: None,
            regions,
        }
    }

    pub(crate) fn lifecycle(regions: &[RegionId]) -> Self {
        Self {
            payload: None,
            regions: lifecycle_rows(regions),
        }
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
        column_schemas([
            &REGION_ID_COLUMN,
            &TABLE_ID_COLUMN,
            &REGION_NUMBER_COLUMN,
            &GC_REPORT_COLUMN,
        ])
    }

    fn extra_rows(&self) -> Result<Vec<Row>> {
        self.regions
            .iter()
            .map(|region| {
                let report = region
                    .report
                    .as_ref()
                    .map(serde_json::to_value)
                    .transpose()
                    .context(SerializeEventSnafu)?;
                Ok(Row {
                    values: vec![
                        ValueData::U64Value(region.region_id.as_u64()).into(),
                        ValueData::U32Value(region.region_id.table_id()).into(),
                        ValueData::U32Value(region.region_id.region_number()).into(),
                        nullable_json(report.as_ref()),
                    ],
                })
            })
            .collect()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

fn lifecycle_rows(regions: &[RegionId]) -> Vec<BatchGcRegionRow> {
    regions
        .iter()
        .copied()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .map(|region_id| BatchGcRegionRow {
            region_id,
            report: None,
        })
        .collect()
}

fn region_report(region_id: RegionId, report: &GcReport) -> Option<BatchGcRegionReport> {
    let mut deleted_files = report
        .deleted_files
        .get(&region_id)
        .into_iter()
        .flatten()
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    deleted_files.sort_unstable();

    let mut deleted_indexes = report
        .deleted_indexes
        .get(&region_id)
        .into_iter()
        .flatten()
        .map(|(file_id, index_version)| DeletedIndexPayload {
            file_id: file_id.to_string(),
            index_version: *index_version,
        })
        .collect::<Vec<_>>();
    deleted_indexes.sort_unstable_by(|left, right| {
        (&left.file_id, left.index_version).cmp(&(&right.file_id, right.index_version))
    });

    let need_retry = report.need_retry_regions.contains(&region_id);
    if deleted_files.is_empty() && deleted_indexes.is_empty() && !need_retry {
        return None;
    }

    Some(BatchGcRegionReport {
        deleted_files,
        deleted_indexes,
        processed: report.processed_regions.contains(&region_id),
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
    };
    use store_api::storage::FileId;

    use super::*;
    use crate::gc::BatchGcProcedure;
    use crate::procedure::test_util::MailboxContext;

    #[test]
    fn test_batch_gc_submitted_event_contract() {
        let first = RegionId::new(1024, 1);
        let second = RegionId::new(1024, 2);
        let event =
            BatchGcEvent::submitted(&[second, first, second], true, Duration::from_secs(10));

        assert_event_contract(
            &event,
            BATCH_GC_EVENT_TYPE,
            &batch_gc_schema(),
            &[region_row(first, None), region_row(second, None)],
        );
        assert_eq!(
            event.json_payload().unwrap(),
            serde_json::json!({
                "version": 1,
                "full_file_listing": true,
                "timeout": "10s",
            })
        );
    }

    #[test]
    fn test_batch_gc_succeeded_event_contract() {
        let first = RegionId::new(1024, 1);
        let second = RegionId::new(1024, 2);
        let first_file = FileId::parse_str("00000000-0000-0000-0000-000000000001").unwrap();
        let second_file = FileId::parse_str("00000000-0000-0000-0000-000000000002").unwrap();
        let index_file = FileId::parse_str("00000000-0000-0000-0000-000000000003").unwrap();
        let report = GcReport {
            deleted_files: HashMap::from([(first, vec![second_file, first_file])]),
            deleted_indexes: HashMap::from([(second, vec![(index_file, 42)])]),
            processed_regions: HashSet::from([first]),
            need_retry_regions: HashSet::from([second]),
        };

        let event = BatchGcEvent::succeeded(&[second], &report);

        assert_event_contract(
            &event,
            BATCH_GC_EVENT_TYPE,
            &batch_gc_schema(),
            &[
                region_row(
                    first,
                    Some(serde_json::json!({
                        "deleted_files": [
                            "00000000-0000-0000-0000-000000000001",
                            "00000000-0000-0000-0000-000000000002",
                        ],
                        "processed": true,
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
                        "processed": false,
                        "need_retry": true,
                    })),
                ),
            ],
        );
        assert_eq!(event.json_payload().unwrap(), serde_json::Value::Null);
    }

    #[test]
    fn test_batch_gc_succeeded_event_omits_noop_report() {
        let noop = RegionId::new(1024, 1);
        let retry = RegionId::new(1024, 2);
        let event = BatchGcEvent::succeeded(
            &[noop, retry],
            &GcReport {
                processed_regions: HashSet::from([noop]),
                need_retry_regions: HashSet::from([retry]),
                ..Default::default()
            },
        );

        assert_event_contract(
            &event,
            BATCH_GC_EVENT_TYPE,
            &batch_gc_schema(),
            &[
                region_row(noop, None),
                region_row(
                    retry,
                    Some(serde_json::json!({
                        "processed": false,
                        "need_retry": true,
                    })),
                ),
            ],
        );
    }

    #[test]
    fn test_batch_gc_lifecycle_event_contract() {
        let first = RegionId::new(1024, 1);
        let second = RegionId::new(1024, 2);
        let event = BatchGcEvent::lifecycle(&[second, first, second]);

        assert_event_contract(
            &event,
            BATCH_GC_EVENT_TYPE,
            &batch_gc_schema(),
            &[region_row(first, None), region_row(second, None)],
        );
        assert_eq!(event.json_payload().unwrap(), serde_json::Value::Null);
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

        let submitted = procedure
            .event(&event_context(
                EventTrigger::Submitted,
                &running,
                EventTypeFilter::All,
            ))
            .unwrap();
        assert_eq!(submitted.event_type(), BATCH_GC_EVENT_TYPE);
        assert_ne!(submitted.json_payload().unwrap(), serde_json::Value::Null);

        let allowed = procedure
            .event(&event_context(
                EventTrigger::Submitted,
                &running,
                EventTypeFilter::Only(HashSet::from([BATCH_GC_EVENT_TYPE.to_string()])),
            ))
            .unwrap();
        assert_eq!(allowed.event_type(), BATCH_GC_EVENT_TYPE);

        let report = GcReport {
            processed_regions: HashSet::from([RegionId::new(1024, 1)]),
            ..Default::default()
        };
        let done = ProcedureState::Done {
            output: Some(Arc::new(report)),
        };
        let succeeded = procedure
            .event(&event_context(
                EventTrigger::Succeeded,
                &done,
                EventTypeFilter::All,
            ))
            .unwrap();
        assert_eq!(succeeded.json_payload().unwrap(), serde_json::Value::Null);
        assert!(
            succeeded.extra_rows().unwrap()[0].values[3]
                .value_data
                .is_none()
        );

        let recovered = procedure
            .event(&event_context(
                EventTrigger::Recovered,
                &running,
                EventTypeFilter::All,
            ))
            .unwrap();
        assert_eq!(recovered.json_payload().unwrap(), serde_json::Value::Null);
        assert!(
            recovered.extra_rows().unwrap()[0].values[3]
                .value_data
                .is_none()
        );

        assert!(
            procedure
                .event(&event_context(
                    EventTrigger::Submitted,
                    &running,
                    EventTypeFilter::Only(HashSet::from(["another_event".to_string()])),
                ))
                .is_none()
        );
        assert!(
            procedure
                .event(&event_context(
                    EventTrigger::Submitted,
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
        let internal = BatchGcEvent::succeeded(
            &[region_id],
            &GcReport {
                processed_regions: HashSet::from([region_id]),
                ..Default::default()
            },
        );
        let event = ProcedureEvent::new(
            procedure_id,
            Box::new(internal),
            ProcedureState::Done { output: None },
            EventTrigger::Succeeded,
        );
        let mut schema = procedure_schema();
        schema.extend(batch_gc_schema());
        let mut values = vec![
            ValueData::StringValue(procedure_id.to_string()).into(),
            ValueData::StringValue("Done".to_string()).into(),
            ValueData::StringValue(String::new()).into(),
            ValueData::StringValue("Succeeded".to_string()).into(),
        ];
        values.extend(region_row(region_id, None).values);

        assert_event_contract(&event, BATCH_GC_EVENT_TYPE, &schema, &[Row { values }]);
    }

    fn batch_gc_schema() -> Vec<ColumnSchema> {
        column_schemas([
            &REGION_ID_COLUMN,
            &TABLE_ID_COLUMN,
            &REGION_NUMBER_COLUMN,
            &GC_REPORT_COLUMN,
        ])
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
