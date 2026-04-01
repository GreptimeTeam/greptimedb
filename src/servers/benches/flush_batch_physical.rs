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

use api::region::RegionResponse;
use api::v1::meta::Peer;
use api::v1::region::RegionRequest;
use arrow::array::{Float64Array, StringArray, TimestampMillisecondArray};
use arrow::datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema, TimeUnit};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use catalog::error::Result as CatalogResult;
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use datatypes::prelude::ConcreteDataType;
use datatypes::schema::{ColumnSchema as DtColumnSchema, Schema as DtSchema};
use partition::error::Result as PartitionResult;
use partition::partition::{PartitionRule, PartitionRuleRef, RegionMask};
use servers::error::{self, Result};
use servers::pending_rows_batcher::{
    PhysicalFlushCatalogProvider, PhysicalFlushNodeRequester, PhysicalFlushPartitionProvider,
    PhysicalTableMetadata, TableBatch, flush_batch_physical,
};
use store_api::storage::RegionId;
use table::test_util::table_info::test_table_info;
use tokio::runtime::Runtime;

// ---------------------------------------------------------------------------
// Mock implementations (memory-backed, no I/O)
// ---------------------------------------------------------------------------

struct BenchCatalogProvider {
    table: PhysicalTableMetadata,
}

#[async_trait]
impl PhysicalFlushCatalogProvider for BenchCatalogProvider {
    async fn physical_table(
        &self,
        _catalog: &str,
        _schema: &str,
        _table_name: &str,
        _query_ctx: &session::context::QueryContext,
    ) -> CatalogResult<Option<PhysicalTableMetadata>> {
        Ok(Some(self.table.clone()))
    }
}

struct BenchPartitionProvider;

struct SingleRegionPartitionRule;

impl PartitionRule for SingleRegionPartitionRule {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn partition_columns(&self) -> &[String] {
        &[]
    }

    fn find_region(
        &self,
        _values: &[datatypes::prelude::Value],
    ) -> PartitionResult<store_api::storage::RegionNumber> {
        Ok(1)
    }

    fn split_record_batch(
        &self,
        record_batch: &RecordBatch,
    ) -> PartitionResult<HashMap<store_api::storage::RegionNumber, RegionMask>> {
        let n = record_batch.num_rows();
        Ok(HashMap::from([(
            1,
            RegionMask::new(arrow::array::BooleanArray::from(vec![true; n]), n),
        )]))
    }
}

#[async_trait]
impl PhysicalFlushPartitionProvider for BenchPartitionProvider {
    async fn find_table_partition_rule(
        &self,
        _table_info: &table::metadata::TableInfo,
    ) -> PartitionResult<PartitionRuleRef> {
        Ok(Arc::new(SingleRegionPartitionRule))
    }

    async fn find_region_leader(&self, _region_id: RegionId) -> Result<Peer> {
        Ok(Peer {
            id: 1,
            addr: "bench-node".to_string(),
        })
    }
}

struct BenchNodeRequester;

#[async_trait]
impl PhysicalFlushNodeRequester for BenchNodeRequester {
    async fn handle(&self, _peer: &Peer, _request: RegionRequest) -> error::Result<RegionResponse> {
        Ok(RegionResponse::new(0))
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn make_physical_table_metadata(num_tags: usize) -> PhysicalTableMetadata {
    let mut columns = vec![
        DtColumnSchema::new("__primary_key", ConcreteDataType::binary_datatype(), false),
        DtColumnSchema::new(
            "greptime_timestamp",
            ConcreteDataType::timestamp_millisecond_datatype(),
            false,
        ),
        DtColumnSchema::new("greptime_value", ConcreteDataType::float64_datatype(), true),
    ];

    let mut name_to_ids = HashMap::new();
    let mut column_ids = vec![0u32, 1, 2];

    for i in 0..num_tags {
        let tag_name = format!("tag{}", i);
        let col_id = (i + 3) as u32;
        columns.push(DtColumnSchema::new(
            &tag_name,
            ConcreteDataType::string_datatype(),
            true,
        ));
        name_to_ids.insert(tag_name, col_id);
        column_ids.push(col_id);
    }

    let schema = Arc::new(DtSchema::try_new(columns).unwrap());
    let mut table_info = test_table_info(1, "phy", "public", "greptime", schema);
    table_info.meta.column_ids = column_ids;

    PhysicalTableMetadata {
        table_info: Arc::new(table_info),
        name_to_ids: Some(name_to_ids),
    }
}

fn make_tag_batch(tag_names: &[&str], num_rows: usize) -> RecordBatch {
    let mut fields = vec![
        Field::new(
            "greptime_timestamp",
            ArrowDataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("greptime_value", ArrowDataType::Float64, true),
    ];
    for tag in tag_names {
        fields.push(Field::new(*tag, ArrowDataType::Utf8, true));
    }

    let schema = Arc::new(ArrowSchema::new(fields));

    let ts: Vec<i64> = (0..num_rows as i64).collect();
    let vals: Vec<f64> = (0..num_rows).map(|i| i as f64).collect();

    let mut arrays: Vec<Arc<dyn arrow::array::Array>> = vec![
        Arc::new(TimestampMillisecondArray::from(ts)),
        Arc::new(Float64Array::from(vals)),
    ];

    for (tag_idx, _tag) in tag_names.iter().enumerate() {
        let values: Vec<String> = (0..num_rows)
            .map(|i| format!("val-{}-{}", tag_idx, i))
            .collect();
        arrays.push(Arc::new(StringArray::from(values)));
    }

    RecordBatch::try_new(schema, arrays).unwrap()
}

fn make_table_batches(
    num_logical_tables: usize,
    rows_per_table: usize,
    tag_names: &[&str],
) -> Vec<TableBatch> {
    (0..num_logical_tables)
        .map(|i| {
            let batch = make_tag_batch(tag_names, rows_per_table);
            let row_count = batch.num_rows();
            TableBatch {
                table_name: format!("logical_{}", i),
                table_id: (100 + i) as u32,
                batches: vec![batch],
                row_count,
            }
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Benchmarks
// ---------------------------------------------------------------------------

fn bench_flush_batch_physical(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let ctx = session::context::QueryContext::arc();

    let num_tags = 5;
    let tag_names: Vec<String> = (0..num_tags).map(|i| format!("tag{}", i)).collect();
    let tag_refs: Vec<&str> = tag_names.iter().map(|s| s.as_str()).collect();

    let catalog_provider = BenchCatalogProvider {
        table: make_physical_table_metadata(num_tags),
    };
    let partition_provider = BenchPartitionProvider;
    let node_requester = BenchNodeRequester;

    let mut group = c.benchmark_group("flush_batch_physical");

    // Vary the number of logical tables
    for num_tables in [1, 10, 50, 100] {
        let rows_per_table = 100;
        let table_batches = make_table_batches(num_tables, rows_per_table, &tag_refs);

        group.bench_with_input(
            BenchmarkId::new("tables", num_tables),
            &table_batches,
            |b, batches| {
                b.iter(|| {
                    rt.block_on(async {
                        flush_batch_physical(
                            batches,
                            "phy",
                            &ctx,
                            &partition_provider,
                            &node_requester,
                            &catalog_provider,
                        )
                        .await
                        .unwrap();
                    });
                });
            },
        );
    }

    // Vary the number of rows per table
    for rows_per_table in [10, 100, 1000, 5000] {
        let num_tables = 10;
        let table_batches = make_table_batches(num_tables, rows_per_table, &tag_refs);

        group.bench_with_input(
            BenchmarkId::new("rows_per_table", rows_per_table),
            &table_batches,
            |b, batches| {
                b.iter(|| {
                    rt.block_on(async {
                        flush_batch_physical(
                            batches,
                            "phy",
                            &ctx,
                            &partition_provider,
                            &node_requester,
                            &catalog_provider,
                        )
                        .await
                        .unwrap();
                    });
                });
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_flush_batch_physical);
criterion_main!(benches);
