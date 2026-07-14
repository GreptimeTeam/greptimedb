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
use std::sync::Arc;
use std::time::Duration;

use api::v1::value::ValueData;
use api::v1::{ColumnDataType, ColumnSchema, Row, SemanticType};
use common_event_recorder::error::{Result, SerializeEventSnafu};
use common_event_recorder::{Event, Eventable};
use parking_lot::RwLock;
use serde::Serialize;
use snafu::ResultExt;
use store_api::storage::{GcReport, RegionId};

pub(crate) const BATCH_GC_EVENT_TYPE: &str = "batch_gc";

const GC_FULL_FILE_LISTING_COLUMN_NAME: &str = "gc_full_file_listing";
const GC_REQUESTED_REGION_COUNT_COLUMN_NAME: &str = "gc_requested_region_count";
const GC_REPORT_AVAILABLE_COLUMN_NAME: &str = "gc_report_available";
const GC_PROCESSED_REGION_COUNT_COLUMN_NAME: &str = "gc_processed_region_count";
const GC_NEED_RETRY_REGION_COUNT_COLUMN_NAME: &str = "gc_need_retry_region_count";
const GC_DELETED_FILE_COUNT_COLUMN_NAME: &str = "gc_deleted_file_count";
const GC_DELETED_INDEX_COUNT_COLUMN_NAME: &str = "gc_deleted_index_count";
/// Maximum number of deleted objects included for each category in an event payload.
const DELETED_OBJECT_SAMPLE_LIMIT_PER_CATEGORY: usize = 100;

#[derive(Clone, Debug, Serialize)]
struct DeletedFileSample {
    region_id: u64,
    file_id: String,
}

#[derive(Clone, Debug, Serialize)]
struct DeletedIndexSample {
    region_id: u64,
    file_id: String,
    index_version: u64,
}

#[derive(Clone, Debug)]
struct CompactGcResult {
    processed_regions: Vec<u64>,
    need_retry_regions: Vec<u64>,
    deleted_file_count: u64,
    deleted_index_count: u64,
    deleted_files_sample: Vec<DeletedFileSample>,
    deleted_files_sample_truncated: bool,
    deleted_indexes_sample: Vec<DeletedIndexSample>,
    deleted_indexes_sample_truncated: bool,
}

impl From<&GcReport> for CompactGcResult {
    fn from(report: &GcReport) -> Self {
        let mut processed_regions = report
            .processed_regions
            .iter()
            .map(|region| region.as_u64())
            .collect::<Vec<_>>();
        processed_regions.sort_unstable();

        let mut need_retry_regions = report
            .need_retry_regions
            .iter()
            .map(|region| region.as_u64())
            .collect::<Vec<_>>();
        need_retry_regions.sort_unstable();

        let mut deleted_files_sample = report
            .deleted_files
            .iter()
            .flat_map(|(region_id, files)| {
                files.iter().map(move |file_id| DeletedFileSample {
                    region_id: region_id.as_u64(),
                    file_id: file_id.to_string(),
                })
            })
            .collect::<Vec<_>>();
        deleted_files_sample.sort_unstable_by(|left, right| {
            left.region_id
                .cmp(&right.region_id)
                .then_with(|| left.file_id.cmp(&right.file_id))
        });
        let deleted_files_sample_truncated =
            deleted_files_sample.len() > DELETED_OBJECT_SAMPLE_LIMIT_PER_CATEGORY;
        deleted_files_sample.truncate(DELETED_OBJECT_SAMPLE_LIMIT_PER_CATEGORY);

        let mut deleted_indexes_sample = report
            .deleted_indexes
            .iter()
            .flat_map(|(region_id, indexes)| {
                indexes
                    .iter()
                    .map(move |(file_id, index_version)| DeletedIndexSample {
                        region_id: region_id.as_u64(),
                        file_id: file_id.to_string(),
                        index_version: *index_version,
                    })
            })
            .collect::<Vec<_>>();
        deleted_indexes_sample.sort_unstable_by(|left, right| {
            left.region_id
                .cmp(&right.region_id)
                .then_with(|| left.file_id.cmp(&right.file_id))
                .then_with(|| left.index_version.cmp(&right.index_version))
        });
        let deleted_indexes_sample_truncated =
            deleted_indexes_sample.len() > DELETED_OBJECT_SAMPLE_LIMIT_PER_CATEGORY;
        deleted_indexes_sample.truncate(DELETED_OBJECT_SAMPLE_LIMIT_PER_CATEGORY);

        Self {
            processed_regions,
            need_retry_regions,
            deleted_file_count: report
                .deleted_files
                .values()
                .map(|files| files.len() as u64)
                .sum(),
            deleted_index_count: report
                .deleted_indexes
                .values()
                .map(|indexes| indexes.len() as u64)
                .sum(),
            deleted_files_sample,
            deleted_files_sample_truncated,
            deleted_indexes_sample,
            deleted_indexes_sample_truncated,
        }
    }
}

/// Immutable batch GC inputs plus the compact result observed after GC completes.
#[derive(Debug)]
pub(crate) struct BatchGcEventContext {
    full_file_listing: bool,
    requested_regions: Vec<u64>,
    timeout: Duration,
    result: RwLock<Option<CompactGcResult>>,
}

impl BatchGcEventContext {
    pub(crate) fn new(
        full_file_listing: bool,
        regions: &[RegionId],
        timeout: Duration,
    ) -> Arc<Self> {
        let mut requested_regions = regions
            .iter()
            .map(|region| region.as_u64())
            .collect::<Vec<_>>();
        requested_regions.sort_unstable();

        Arc::new(Self {
            full_file_listing,
            requested_regions,
            timeout,
            result: RwLock::new(None),
        })
    }

    pub(crate) fn update(&self, report: &GcReport) {
        *self.result.write() = Some(report.into());
    }
}

impl Eventable for BatchGcEventContext {
    fn to_event(&self) -> Option<Box<dyn Event>> {
        // The recorder buffers events, so this must copy the mutable result now.
        Some(Box::new(BatchGcEvent {
            full_file_listing: self.full_file_listing,
            requested_regions: self.requested_regions.clone(),
            timeout: self.timeout,
            result: self.result.read().clone(),
        }))
    }
}

/// An immutable snapshot of one batch GC procedure lifecycle transition.
#[derive(Debug)]
pub(crate) struct BatchGcEvent {
    full_file_listing: bool,
    requested_regions: Vec<u64>,
    timeout: Duration,
    result: Option<CompactGcResult>,
}

#[derive(Serialize)]
struct Payload {
    #[serde(with = "humantime_serde")]
    timeout: Duration,
    requested_regions: Vec<u64>,
    processed_regions: Option<Vec<u64>>,
    need_retry_regions: Option<Vec<u64>>,
    deleted_files_sample: Option<Vec<DeletedFileSample>>,
    deleted_files_sample_truncated: Option<bool>,
    deleted_indexes_sample: Option<Vec<DeletedIndexSample>>,
    deleted_indexes_sample_truncated: Option<bool>,
}

impl Event for BatchGcEvent {
    fn event_type(&self) -> &str {
        BATCH_GC_EVENT_TYPE
    }

    fn extra_schema(&self) -> Vec<ColumnSchema> {
        [
            (GC_FULL_FILE_LISTING_COLUMN_NAME, ColumnDataType::Boolean),
            (
                GC_REQUESTED_REGION_COUNT_COLUMN_NAME,
                ColumnDataType::Uint64,
            ),
            (GC_REPORT_AVAILABLE_COLUMN_NAME, ColumnDataType::Boolean),
            (
                GC_PROCESSED_REGION_COUNT_COLUMN_NAME,
                ColumnDataType::Uint64,
            ),
            (
                GC_NEED_RETRY_REGION_COUNT_COLUMN_NAME,
                ColumnDataType::Uint64,
            ),
            (GC_DELETED_FILE_COUNT_COLUMN_NAME, ColumnDataType::Uint64),
            (GC_DELETED_INDEX_COUNT_COLUMN_NAME, ColumnDataType::Uint64),
        ]
        .into_iter()
        .map(|(column_name, datatype)| ColumnSchema {
            column_name: column_name.to_string(),
            datatype: datatype.into(),
            semantic_type: SemanticType::Field.into(),
            ..Default::default()
        })
        .collect()
    }

    fn extra_rows(&self) -> Result<Vec<Row>> {
        let (
            report_available,
            processed_count,
            need_retry_count,
            deleted_file_count,
            deleted_index_count,
        ) = self.result.as_ref().map_or((false, 0, 0, 0, 0), |result| {
            (
                true,
                result.processed_regions.len() as u64,
                result.need_retry_regions.len() as u64,
                result.deleted_file_count,
                result.deleted_index_count,
            )
        });

        Ok(vec![Row {
            values: vec![
                ValueData::BoolValue(self.full_file_listing).into(),
                ValueData::U64Value(self.requested_regions.len() as u64).into(),
                ValueData::BoolValue(report_available).into(),
                ValueData::U64Value(processed_count).into(),
                ValueData::U64Value(need_retry_count).into(),
                ValueData::U64Value(deleted_file_count).into(),
                ValueData::U64Value(deleted_index_count).into(),
            ],
        }])
    }

    fn json_payload(&self) -> Result<serde_json::Value> {
        let result = self.result.as_ref();
        serde_json::to_value(Payload {
            timeout: self.timeout,
            requested_regions: self.requested_regions.clone(),
            processed_regions: result.map(|result| result.processed_regions.clone()),
            need_retry_regions: result.map(|result| result.need_retry_regions.clone()),
            deleted_files_sample: result.map(|result| result.deleted_files_sample.clone()),
            deleted_files_sample_truncated: result
                .map(|result| result.deleted_files_sample_truncated),
            deleted_indexes_sample: result.map(|result| result.deleted_indexes_sample.clone()),
            deleted_indexes_sample_truncated: result
                .map(|result| result.deleted_indexes_sample_truncated),
        })
        .context(SerializeEventSnafu)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::time::Duration;

    use api::v1::value::ValueData;
    use common_event_recorder::Eventable;
    use store_api::storage::{FileId, GcReport, RegionId};

    use super::*;

    fn region(number: u32) -> RegionId {
        RegionId::new(1, number)
    }

    #[test]
    fn test_schema_is_stable_before_and_after_report() {
        let context =
            BatchGcEventContext::new(false, &[region(2), region(1)], Duration::from_secs(3));
        let before = context.to_event().unwrap();

        context.update(&GcReport::default());
        let after = context.to_event().unwrap();

        assert_eq!(before.extra_schema(), after.extra_schema());
        assert_eq!(before.extra_rows().unwrap().len(), 1);
        assert_eq!(after.extra_rows().unwrap().len(), 1);
    }

    #[test]
    fn test_event_snapshot_does_not_observe_later_report() {
        let context = BatchGcEventContext::new(false, &[region(1)], Duration::from_secs(3));
        let running = context.to_event().unwrap();

        let file_id = FileId::random();
        let report = GcReport {
            deleted_files: HashMap::from([(region(1), vec![file_id])]),
            processed_regions: HashSet::from([region(1)]),
            ..Default::default()
        };
        context.update(&report);

        assert_eq!(
            running.json_payload().unwrap()["processed_regions"],
            serde_json::Value::Null
        );
        for field in [
            "deleted_files_sample",
            "deleted_files_sample_truncated",
            "deleted_indexes_sample",
            "deleted_indexes_sample_truncated",
        ] {
            assert_eq!(
                running.json_payload().unwrap()[field],
                serde_json::Value::Null
            );
        }
        assert_eq!(
            running.extra_rows().unwrap()[0].values[2].value_data,
            Some(ValueData::BoolValue(false))
        );
        assert_eq!(
            context.to_event().unwrap().json_payload().unwrap()["processed_regions"],
            serde_json::json!([region(1).as_u64()])
        );
        assert_eq!(
            context.to_event().unwrap().json_payload().unwrap()["deleted_files_sample"],
            serde_json::json!([{"region_id": region(1).as_u64(), "file_id": file_id.to_string()}])
        );
    }

    #[test]
    fn test_compact_report_samples_are_sorted() {
        let file_1 = FileId::parse_str("00000000-0000-0000-0000-000000000001").unwrap();
        let file_2 = FileId::parse_str("00000000-0000-0000-0000-000000000002").unwrap();
        let file_3 = FileId::parse_str("00000000-0000-0000-0000-000000000003").unwrap();
        let index_1 = FileId::parse_str("00000000-0000-0000-0000-000000000004").unwrap();
        let index_2 = FileId::parse_str("00000000-0000-0000-0000-000000000005").unwrap();
        let report = GcReport {
            deleted_files: HashMap::from([
                (region(2), vec![file_3, file_1]),
                (region(1), vec![file_2]),
            ]),
            deleted_indexes: HashMap::from([
                (region(2), vec![(index_2, 1), (index_1, 3), (index_1, 2)]),
                (region(1), vec![(index_2, 4)]),
            ]),
            need_retry_regions: HashSet::from([region(3), region(1)]),
            processed_regions: HashSet::from([region(2), region(1)]),
        };
        let context =
            BatchGcEventContext::new(true, &[region(3), region(1)], Duration::from_secs(10));
        context.update(&report);
        let event = context.to_event().unwrap();

        let payload = event.json_payload().unwrap();
        assert_eq!(
            payload["requested_regions"],
            serde_json::json!([region(1).as_u64(), region(3).as_u64()])
        );
        assert_eq!(
            payload["processed_regions"],
            serde_json::json!([region(1).as_u64(), region(2).as_u64()])
        );
        assert_eq!(
            payload["need_retry_regions"],
            serde_json::json!([region(1).as_u64(), region(3).as_u64()])
        );
        let values = &event.extra_rows().unwrap()[0].values;
        assert_eq!(values[0].value_data, Some(ValueData::BoolValue(true)));
        assert_eq!(values[1].value_data, Some(ValueData::U64Value(2)));
        assert_eq!(values[2].value_data, Some(ValueData::BoolValue(true)));
        assert_eq!(values[3].value_data, Some(ValueData::U64Value(2)));
        assert_eq!(values[4].value_data, Some(ValueData::U64Value(2)));
        assert_eq!(values[5].value_data, Some(ValueData::U64Value(3)));
        assert_eq!(values[6].value_data, Some(ValueData::U64Value(4)));
        assert_eq!(
            payload["deleted_files_sample"],
            serde_json::json!([
                {"region_id": region(1).as_u64(), "file_id": file_2.to_string()},
                {"region_id": region(2).as_u64(), "file_id": file_1.to_string()},
                {"region_id": region(2).as_u64(), "file_id": file_3.to_string()},
            ])
        );
        assert_eq!(payload["deleted_files_sample_truncated"], false);
        assert_eq!(
            payload["deleted_indexes_sample"],
            serde_json::json!([
                {"region_id": region(1).as_u64(), "file_id": index_2.to_string(), "index_version": 4},
                {"region_id": region(2).as_u64(), "file_id": index_1.to_string(), "index_version": 2},
                {"region_id": region(2).as_u64(), "file_id": index_1.to_string(), "index_version": 3},
                {"region_id": region(2).as_u64(), "file_id": index_2.to_string(), "index_version": 1},
            ])
        );
        assert_eq!(payload["deleted_indexes_sample_truncated"], false);
    }

    #[test]
    fn test_compact_report_samples_are_bounded() {
        let object_count = DELETED_OBJECT_SAMPLE_LIMIT_PER_CATEGORY + 1;
        let report = GcReport {
            deleted_files: HashMap::from([(
                region(1),
                (0..object_count).map(|_| FileId::random()).collect(),
            )]),
            deleted_indexes: HashMap::from([(
                region(1),
                (0..object_count)
                    .map(|index_version| (FileId::random(), index_version as u64))
                    .collect(),
            )]),
            ..Default::default()
        };
        let context = BatchGcEventContext::new(false, &[region(1)], Duration::from_secs(3));
        context.update(&report);

        let payload = context.to_event().unwrap().json_payload().unwrap();
        assert_eq!(
            payload["deleted_files_sample"].as_array().unwrap().len(),
            DELETED_OBJECT_SAMPLE_LIMIT_PER_CATEGORY
        );
        assert_eq!(payload["deleted_files_sample_truncated"], true);
        assert_eq!(
            payload["deleted_indexes_sample"].as_array().unwrap().len(),
            DELETED_OBJECT_SAMPLE_LIMIT_PER_CATEGORY
        );
        assert_eq!(payload["deleted_indexes_sample_truncated"], true);
    }
}
