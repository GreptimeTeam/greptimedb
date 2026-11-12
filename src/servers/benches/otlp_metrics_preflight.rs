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

#[path = "otlp_metrics/fixtures.rs"]
mod fixtures;

use std::collections::BTreeMap;
use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;

use api::helper::ColumnDataTypeWrapper;
use api::v1::{ColumnDataType, ColumnSchema, RowInsertRequests, SemanticType};
use catalog::RegisterTableRequest;
use catalog::memory::MemoryCatalogManager;
use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
use common_meta::cache::new_table_flownode_set_cache;
use common_meta::ddl::test_util::datanode_handler::NaiveDatanodeHandler;
use common_meta::kv_backend::memory::MemoryKvBackend;
use common_meta::test_util::MockDatanodeManager;
use common_query::prelude::greptime_value;
use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use datatypes::schema::{ColumnSchema as DataColumn, Schema};
use moka::future::Cache;
use operator::insert::Inserter;
use operator::test_util::create_partition_rule_manager;
use servers::otlp::metrics::{prepare_metrics, to_grpc_insert_requests};
use session::context::QueryContext;
use session::protocol_ctx::OtlpMetricCtx;
use store_api::metric_engine_consts::{LOGICAL_TABLE_METADATA_KEY, PHYSICAL_TABLE_METADATA_KEY};
use table::dist_table::DummyDataSource;
use table::metadata::FilterPushDownType;
use table::test_util::table_info::test_table_info;

fn register_table(catalog: &MemoryCatalogManager, id: u32, name: &str, columns: &[ColumnSchema]) {
    let schema = Schema::new(
        columns
            .iter()
            .map(|column| {
                let timestamp = column.semantic_type == SemanticType::Timestamp as i32;
                let required = name == "required_tag_1024" && column.column_name == "label_0";
                DataColumn::new(
                    &column.column_name,
                    ColumnDataTypeWrapper::try_new(
                        column.datatype,
                        column.datatype_extension.clone(),
                    )
                    .unwrap()
                    .into(),
                    !timestamp && !required,
                )
                .with_time_index(timestamp)
            })
            .collect(),
    );
    let mut info = test_table_info(
        id,
        name,
        DEFAULT_SCHEMA_NAME,
        DEFAULT_CATALOG_NAME,
        Arc::new(schema),
    );
    info.meta.primary_key_indices = columns
        .iter()
        .enumerate()
        .filter_map(|(i, c)| (c.semantic_type == SemanticType::Tag as i32).then_some(i))
        .collect();
    info.meta.value_indices = columns
        .iter()
        .enumerate()
        .filter_map(|(i, c)| (c.semantic_type == SemanticType::Field as i32).then_some(i))
        .collect();
    // Ordinary tables retain required tags; logical tables inherit nullable physical tags.
    if name != "required_tag_1024" {
        info.meta.engine = "metric".into();
        let (key, value) = if name == "physical" {
            (PHYSICAL_TABLE_METADATA_KEY, "true")
        } else {
            (LOGICAL_TABLE_METADATA_KEY, "physical")
        };
        info.meta
            .options
            .extra_options
            .insert(key.into(), value.into());
    }
    catalog
        .register_table_sync(RegisterTableRequest {
            catalog: DEFAULT_CATALOG_NAME.into(),
            schema: DEFAULT_SCHEMA_NAME.into(),
            table_name: name.into(),
            table_id: id,
            table: Arc::new(table::Table::new(
                Arc::new(info),
                FilterPushDownType::Unsupported,
                Arc::new(DummyDataSource),
            )),
        })
        .unwrap();
}

async fn inserter(requests: &RowInsertRequests) -> Inserter {
    let catalog = MemoryCatalogManager::with_default_setup();
    let mut physical = BTreeMap::new();
    physical.insert(
        greptime_value().to_string(),
        api::v1::helper::field_column_schema(greptime_value(), ColumnDataType::Float64),
    );
    for (index, request) in requests.inserts.iter().enumerate() {
        let columns = &request.rows.as_ref().unwrap().schema;
        register_table(&catalog, index as u32 + 1, &request.table_name, columns);
        physical.extend(
            columns
                .iter()
                .map(|column| (column.column_name.clone(), column.clone())),
        );
    }
    register_table(
        &catalog,
        0,
        "physical",
        &physical.into_values().collect::<Vec<_>>(),
    );
    let backend = Arc::new(MemoryKvBackend::default());
    Inserter::new(
        catalog,
        create_partition_rule_manager(backend.clone()).await,
        Arc::new(MockDatanodeManager::new(NaiveDatanodeHandler)),
        Arc::new(new_table_flownode_set_cache(
            String::new(),
            Cache::new(100),
            backend,
        )),
        true,
    )
}

fn bench_preflight(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let ctx = QueryContext::arc();
    let mut group = c.benchmark_group("otlp_preflight");
    for fixture in fixtures::fixtures() {
        let mut metric_ctx = OtlpMetricCtx {
            with_metric_engine: true,
            ..Default::default()
        };
        let converted = to_grpc_insert_requests(fixture.request.clone(), &mut metric_ctx).unwrap();
        assert_eq!(
            converted.outcome.accepted_data_points,
            fixture.points as i64 - fixture.rejected
        );
        let inserter = runtime.block_on(inserter(&converted.requests));
        let prepared = prepare_metrics(fixture.request, &mut metric_ctx).unwrap();
        group.throughput(Throughput::Elements(prepared.num_points() as u64));
        group.bench_function(fixture.name, |b| {
            let mut requests = prepared.requests.clone();
            let result = runtime
                .block_on(inserter.validate_and_normalize_otlp_metrics(
                    &mut requests,
                    &ctx,
                    true,
                    "physical",
                ))
                .unwrap();
            let rejections = prepared.point_rejections(result.rejections).unwrap();
            assert_eq!(rejections.len(), prepared.num_points());
            assert_eq!(
                rejections.iter().filter(|r| r.is_some()).count(),
                if fixture.name == "required_tag_1024" {
                    fixture.points.div_ceil(10) as usize
                } else {
                    0
                }
            );
            b.iter_batched_ref(
                || prepared.requests.clone(),
                |requests| {
                    black_box(
                        runtime
                            .block_on(inserter.validate_and_normalize_otlp_metrics(
                                requests, &ctx, true, "physical",
                            ))
                            .unwrap(),
                    )
                },
                BatchSize::LargeInput,
            );
        });
    }
    group.finish();
}

fn bench_ingestion(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let ctx = QueryContext::arc();
    let mut group = c.benchmark_group("otlp_ingestion");
    for fixture in fixtures::fixtures() {
        let metric_ctx = OtlpMetricCtx {
            with_metric_engine: true,
            ..Default::default()
        };
        let converted =
            to_grpc_insert_requests(fixture.request.clone(), &mut metric_ctx.clone()).unwrap();
        let inserter = runtime.block_on(inserter(&converted.requests));
        let convert = |request| {
            let mut prepared = prepare_metrics(request, &mut metric_ctx.clone()).unwrap();
            let validated = runtime
                .block_on(inserter.validate_and_normalize_otlp_metrics(
                    &mut prepared.requests,
                    &ctx,
                    true,
                    "physical",
                ))
                .unwrap();
            let rejections = prepared.point_rejections(validated.rejections).unwrap();
            prepared
                .finish(rejections, &validated.physical_tables)
                .unwrap()
        };
        let output = convert(fixture.request.clone());
        let rejected = if fixture.name == "required_tag_1024" {
            fixture.points.div_ceil(10) as i64
        } else {
            fixture.rejected
        };
        assert_eq!(output.outcome.rejected_data_points, rejected);
        assert_eq!(
            output.outcome.accepted_data_points,
            fixture.points as i64 - rejected
        );
        group.throughput(Throughput::Elements(fixture.points));
        group.bench_function(fixture.name, |b| {
            b.iter_batched(
                || fixture.request.clone(),
                |request| {
                    black_box(convert(request));
                },
                BatchSize::LargeInput,
            );
        });
    }
    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(20).warm_up_time(Duration::from_secs(1)).measurement_time(Duration::from_secs(3));
    targets = bench_preflight, bench_ingestion
}
criterion_main!(benches);
