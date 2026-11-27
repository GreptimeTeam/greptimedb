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

//! Utils for testing the optimizer.

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use api::region::RegionResponse;
use api::v1::SemanticType;
use async_trait::async_trait;
use common_error::ext::{BoxedError, PlainError};
use common_error::status_code::StatusCode;
use common_recordbatch::{RecordBatch, RecordBatches, SendableRecordBatchStream};
use common_time::Timestamp;
use datafusion::physical_plan::display::{DisplayAs, DisplayFormatType};
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::{ExecutionPlan, PhysicalExpr};
use datafusion_physical_expr::expressions::{Column, IsNotNullExpr};
use datafusion_physical_expr::{LexOrdering, PhysicalSortExpr};
use datatypes::arrow::compute::SortOptions;
use datatypes::schema::{ColumnSchema, Schema, SchemaRef};
use store_api::metadata::{
    ColumnMetadata, RegionMetadata, RegionMetadataBuilder, RegionMetadataRef,
};
use store_api::metric_engine_consts::DATA_SCHEMA_TSID_COLUMN_NAME;
use store_api::region_engine::{
    PartitionRange, PrepareRequest, QueryScanContext, RegionEngine, RegionRole, RegionScanner,
    RegionScannerRef, RegionStatistic, RemapManifestsRequest, RemapManifestsResponse,
    ScannerProperties, SetRegionRoleStateResponse, SettableRegionRoleState, SinglePartitionScanner,
    StatisticsSplit, SyncRegionFromRequest, SyncRegionFromResponse,
};
use store_api::region_request::RegionRequest;
use store_api::storage::{ConcreteDataType, RegionId, ScanRequest, SequenceNumber};
use table::table::scan::RegionScanExec;

use crate::dummy_catalog::DummyTableProvider;

/// A mock region engine that can be used for testing the optimizer.
pub(crate) struct MetaRegionEngine {
    metadatas: HashMap<RegionId, RegionMetadataRef>,
    name: &'static str,
}

impl MetaRegionEngine {
    /// Creates a engine with the given metadata.
    pub(crate) fn with_metadata(metadata: RegionMetadataRef) -> Self {
        let mut metadatas = HashMap::new();
        metadatas.insert(metadata.region_id, metadata);

        Self {
            metadatas,
            name: "MetaRegionEngine",
        }
    }

    /// Sets the engine name exposed to scan-hint capability checks.
    pub(crate) fn with_engine_name(mut self, name: &'static str) -> Self {
        self.name = name;
        self
    }
}

#[async_trait]
impl RegionEngine for MetaRegionEngine {
    fn name(&self) -> &str {
        self.name
    }

    async fn handle_request(
        &self,
        _region_id: RegionId,
        _request: RegionRequest,
    ) -> Result<RegionResponse, BoxedError> {
        unimplemented!()
    }

    async fn handle_query(
        &self,
        _region_id: RegionId,
        _request: ScanRequest,
    ) -> Result<RegionScannerRef, BoxedError> {
        unimplemented!()
    }

    async fn get_metadata(&self, region_id: RegionId) -> Result<RegionMetadataRef, BoxedError> {
        self.metadatas.get(&region_id).cloned().ok_or_else(|| {
            BoxedError::new(PlainError::new(
                "Region not found".to_string(),
                StatusCode::RegionNotFound,
            ))
        })
    }

    fn region_statistic(&self, _region_id: RegionId) -> Option<RegionStatistic> {
        None
    }

    async fn get_committed_sequence(
        &self,
        _region_id: RegionId,
    ) -> Result<SequenceNumber, BoxedError> {
        Ok(SequenceNumber::default())
    }

    async fn stop(&self) -> Result<(), BoxedError> {
        Ok(())
    }

    fn set_region_role(&self, _region_id: RegionId, _role: RegionRole) -> Result<(), BoxedError> {
        unimplemented!()
    }

    fn set_region_role_with_lease(
        &self,
        _region_id: RegionId,
        _role: RegionRole,
        _sent_at: std::time::Instant,
        _deadline: std::time::Instant,
        _candidate_lease_id: Option<u64>,
    ) -> Result<bool, BoxedError> {
        unimplemented!()
    }

    async fn set_region_role_state_gracefully(
        &self,
        _region_id: RegionId,
        _region_role_state: SettableRegionRoleState,
    ) -> Result<SetRegionRoleStateResponse, BoxedError> {
        unimplemented!()
    }

    async fn sync_region(
        &self,
        _region_id: RegionId,
        _request: SyncRegionFromRequest,
    ) -> Result<SyncRegionFromResponse, BoxedError> {
        unimplemented!()
    }

    async fn remap_manifests(
        &self,
        _request: RemapManifestsRequest,
    ) -> Result<RemapManifestsResponse, BoxedError> {
        unimplemented!()
    }

    fn role(&self, _region_id: RegionId) -> Option<RegionRole> {
        None
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Mock a [DummyTableProvider] with a single region.
/// The schema is: `k0: string tag, ts: timestamp, v0: float64 field`
pub(crate) fn mock_table_provider(region_id: RegionId) -> DummyTableProvider {
    let metadata = Arc::new(mock_region_metadata(region_id));
    let engine = Arc::new(MetaRegionEngine::with_metadata(metadata.clone()));
    DummyTableProvider::new(region_id, engine, metadata)
}

/// Mock a [DummyTableProvider] with a single region and internal `__tsid` column.
///
/// The schema is: `__tsid: uint64 tag, k0: string tag, ts: timestamp, v0: float64 field`
pub(crate) fn mock_table_provider_with_tsid(region_id: RegionId) -> DummyTableProvider {
    let metadata = Arc::new(mock_region_metadata_with_tsid(region_id));
    let engine = Arc::new(MetaRegionEngine::with_metadata(metadata.clone()));
    DummyTableProvider::new(region_id, engine, metadata)
}

/// Returns a mock region metadata.
/// The schema is: `k0: string tag, ts: timestamp, v0: float64 field`
fn mock_region_metadata(region_id: RegionId) -> RegionMetadata {
    let mut builder = RegionMetadataBuilder::new(region_id);
    builder
        .push_column_metadata(ColumnMetadata {
            column_schema: ColumnSchema::new("k0", ConcreteDataType::string_datatype(), true),
            semantic_type: SemanticType::Tag,
            column_id: 1,
        })
        .push_column_metadata(ColumnMetadata {
            column_schema: ColumnSchema::new(
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            ),
            semantic_type: SemanticType::Timestamp,
            column_id: 2,
        })
        .push_column_metadata(ColumnMetadata {
            column_schema: ColumnSchema::new("v0", ConcreteDataType::float64_datatype(), false),
            semantic_type: SemanticType::Field,
            column_id: 3,
        })
        .primary_key(vec![1]);
    builder.build().unwrap()
}

/// Returns a mock region metadata with internal `__tsid` column.
fn mock_region_metadata_with_tsid(region_id: RegionId) -> RegionMetadata {
    let mut builder = RegionMetadataBuilder::new(region_id);
    builder
        .push_column_metadata(ColumnMetadata {
            column_schema: ColumnSchema::new(
                DATA_SCHEMA_TSID_COLUMN_NAME,
                ConcreteDataType::uint64_datatype(),
                false,
            ),
            semantic_type: SemanticType::Tag,
            column_id: 1,
        })
        .push_column_metadata(ColumnMetadata {
            column_schema: ColumnSchema::new("k0", ConcreteDataType::string_datatype(), true),
            semantic_type: SemanticType::Tag,
            column_id: 2,
        })
        .push_column_metadata(ColumnMetadata {
            column_schema: ColumnSchema::new(
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            ),
            semantic_type: SemanticType::Timestamp,
            column_id: 3,
        })
        .push_column_metadata(ColumnMetadata {
            column_schema: ColumnSchema::new("v0", ConcreteDataType::float64_datatype(), false),
            semantic_type: SemanticType::Field,
            column_id: 4,
        })
        .primary_key(vec![1, 2]);
    builder.build().unwrap()
}

fn log_region_metadata() -> RegionMetadataRef {
    let mut builder = RegionMetadataBuilder::new(RegionId::new(1, 1));
    builder
        .push_column_metadata(ColumnMetadata {
            column_schema: ColumnSchema::new("body", ConcreteDataType::string_datatype(), true),
            semantic_type: SemanticType::Field,
            column_id: 1,
        })
        .push_column_metadata(ColumnMetadata {
            column_schema: ColumnSchema::new(
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            ),
            semantic_type: SemanticType::Timestamp,
            column_id: 2,
        });
    Arc::new(builder.build().unwrap())
}

/// A region scan over an empty `(body STRING, ts TIMESTAMP TIME INDEX)` table split
/// into `partitions` partitions of `ranges_per_partition` ranges, each range reporting
/// one row.
pub(crate) fn log_region_scan(
    partitions: usize,
    ranges_per_partition: usize,
) -> Arc<dyn ExecutionPlan> {
    let metadata = log_region_metadata();
    let stream = RecordBatches::try_new(metadata.schema.clone(), vec![])
        .unwrap()
        .as_stream();
    let scanner = Box::new(SinglePartitionScanner::new(stream, false, metadata, None));
    let scan = RegionScanExec::new(scanner, ScanRequest::default(), None).unwrap();
    let ranges = (0..partitions)
        .map(|partition| {
            (0..ranges_per_partition)
                .map(|i| {
                    let id = partition * ranges_per_partition + i;
                    PartitionRange {
                        start: Timestamp::new_millisecond(id as i64 * 10),
                        end: Timestamp::new_millisecond(id as i64 * 10 + 10),
                        num_rows: 1,
                        identifier: id,
                    }
                })
                .collect()
        })
        .collect();
    Arc::new(scan.with_new_partitions(ranges, partitions).unwrap())
}

/// Filters the output of a [`log_region_scan`] by `body IS NOT NULL`.
pub(crate) fn body_is_not_null(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    let predicate = Arc::new(IsNotNullExpr::new(Arc::new(Column::new("body", 0))));
    Arc::new(FilterExec::try_new(predicate, input).unwrap())
}

pub(crate) fn sort_by(input: Arc<dyn ExecutionPlan>, key: Column, descending: bool) -> SortExec {
    let options = SortOptions {
        descending,
        nulls_first: true,
    };
    let ordering = LexOrdering::new(vec![PhysicalSortExpr::new(Arc::new(key), options)]).unwrap();
    SortExec::new(ordering, input)
}

/// A region scan over the same table as [`log_region_scan`] whose ranges are not yet
/// assigned to partitions, as `ParallelizeScan` sees it.
pub(crate) fn unassigned_log_region_scan(ranges: Vec<PartitionRange>) -> Arc<dyn ExecutionPlan> {
    let scanner = Box::new(RangesScanner {
        metadata: log_region_metadata(),
        properties: ScannerProperties::new(vec![ranges], false, 0),
        batches: None,
        statistics_split: None,
    });
    Arc::new(RegionScanExec::new(scanner, ScanRequest::default(), None).unwrap())
}

/// Builds a count scan whose original row count is unknown, but whose residual
/// has exact statistics. A scanner that can split append-mode rows may do this.
pub(crate) fn metadata_aggr_region_scan(
    exact_rows: usize,
    residual_rows: usize,
) -> Arc<dyn ExecutionPlan> {
    let schema = Arc::new(Schema::new(vec![]));
    let scanner = |rows, exact, statistics_split| RangesScanner {
        metadata: log_region_metadata(),
        properties: ScannerProperties::new(vec![vec![]], false, rows)
            .with_exact_num_rows(exact),
        batches: Some(
            RecordBatches::try_new(
                schema.clone(),
                vec![RecordBatch::new_with_count(schema.clone(), rows).unwrap()],
            )
            .unwrap(),
        ),
        statistics_split,
    };
    let residual = scanner(residual_rows, Some(residual_rows), None);
    let original = scanner(
        exact_rows + residual_rows,
        None,
        Some(StatisticsSplit {
            exact_rows,
            residual: Box::new(residual),
        }),
    );
    Arc::new(RegionScanExec::new(Box::new(original), ScanRequest::default(), None).unwrap())
}

#[derive(Debug)]
struct RangesScanner {
    metadata: RegionMetadataRef,
    properties: ScannerProperties,
    batches: Option<RecordBatches>,
    statistics_split: Option<StatisticsSplit>,
}

impl DisplayAs for RangesScanner {
    fn fmt_as(
        &self,
        _format: DisplayFormatType,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        write!(f, "RangesScanner")
    }
}

impl RegionScanner for RangesScanner {
    fn name(&self) -> &str {
        "RangesScanner"
    }

    fn properties(&self) -> &ScannerProperties {
        &self.properties
    }

    fn schema(&self) -> SchemaRef {
        self.batches
            .as_ref()
            .map_or_else(|| self.metadata.schema.clone(), RecordBatches::schema)
    }

    fn metadata(&self) -> RegionMetadataRef {
        self.metadata.clone()
    }

    fn prepare(&mut self, request: PrepareRequest) -> Result<(), BoxedError> {
        self.properties.prepare(request);
        Ok(())
    }

    fn scan_partition(
        &self,
        _ctx: &QueryScanContext,
        _metrics: &ExecutionPlanMetricsSet,
        _partition: usize,
    ) -> Result<SendableRecordBatchStream, BoxedError> {
        Ok(self.batches.as_ref().expect("planning only").as_stream())
    }

    fn take_statistics_split(&mut self) -> Option<StatisticsSplit> {
        self.statistics_split.take()
    }

    fn has_predicate_without_region(&self) -> bool {
        false
    }

    fn add_dyn_filter_to_predicate(
        &mut self,
        filter_exprs: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Vec<bool> {
        vec![false; filter_exprs.len()]
    }

    fn set_logical_region(&mut self, logical_region: bool) {
        self.properties.set_logical_region(logical_region);
    }

    fn set_query_load_region_id(&mut self, region_id: RegionId) {
        self.properties.set_query_load_region_id(region_id);
    }
}
