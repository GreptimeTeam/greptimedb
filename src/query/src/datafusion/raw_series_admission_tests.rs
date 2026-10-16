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

use std::sync::Arc;

use common_query::request::{
    INITIAL_REMOTE_DYN_FILTER_REGISTRATIONS_EXTENSION_KEY, RawSeriesRequest,
};
use datafusion::datasource::DefaultTableSource;
use datafusion_expr::{LogicalPlan, LogicalPlanBuilder, col, lit};
use session::context::{QueryContext, QueryContextBuilder};
use store_api::storage::RegionId;

use crate::QueryEngineRef;
use crate::datafusion::QUERY_PARALLELISM_HINT;
use crate::options::QueryOptions;
use crate::query_engine::QueryEngineFactory;

async fn engine() -> QueryEngineRef {
    QueryEngineFactory::new(
        catalog::memory::new_memory_catalog_manager().unwrap(),
        None,
        None,
        None,
        None,
        false,
        QueryOptions::default(),
    )
    .query_engine()
}

fn mock_scan() -> (LogicalPlan, RawSeriesRequest) {
    let provider = crate::optimizer::test_util::mock_table_provider(RegionId::new(1, 1));
    let metadata = provider.region_metadata();
    let source = Arc::new(DefaultTableSource::new(Arc::new(provider)));
    let scan = LogicalPlanBuilder::scan("raw", source, Some(vec![2]))
        .unwrap()
        .build()
        .unwrap();
    let binding = RawSeriesRequest {
        version: 1,
        physical_metadata: metadata.as_ref().clone(),
        metric_name: "m".to_string(),
        logical_tags: vec![],
        field_ids: vec![3],
    };
    (scan, binding)
}

async fn assert_rejected(
    engine: &QueryEngineRef,
    plan: LogicalPlan,
    ctx: Arc<QueryContext>,
    binding: RawSeriesRequest,
) {
    assert!(
        engine
            .prepare_raw_series(plan.clone(), ctx.clone(), binding.clone())
            .await
            .unwrap()
            .is_none()
    );
    let error = match engine.execute_raw_series(plan, ctx, binding).await {
        Err(error) => error,
        Ok(_) => panic!("unsupported raw-series plan was accepted"),
    };
    assert!(matches!(
        error,
        crate::error::Error::Unimplemented { operation, .. }
            if operation == "unsupported native raw-series plan"
    ));
}

#[tokio::test]
async fn raw_series_admission_rejects_invalid_parallelism_hints() {
    let engine = engine().await;
    let (scan, binding) = mock_scan();
    for hint in ["invalid", "0", "2"] {
        let ctx = Arc::new(
            QueryContextBuilder::default()
                .set_extension(QUERY_PARALLELISM_HINT.to_string(), hint.to_string())
                .build(),
        );
        assert_rejected(&engine, scan.clone(), ctx, binding.clone()).await;
    }
}

#[tokio::test]
async fn raw_series_admission_rejects_dynamic_filters_and_parent_plans() {
    let engine = engine().await;
    let (scan, binding) = mock_scan();
    let ctx = Arc::new(
        QueryContextBuilder::default()
            .set_extension(
                INITIAL_REMOTE_DYN_FILTER_REGISTRATIONS_EXTENSION_KEY.to_string(),
                "malformed marker".to_string(),
            )
            .build(),
    );
    assert_rejected(&engine, scan.clone(), ctx, binding.clone()).await;

    let parent = LogicalPlanBuilder::from(scan)
        .filter(col("v0").gt(lit(0.0)))
        .unwrap()
        .build()
        .unwrap();
    let ctx = Arc::new(
        QueryContextBuilder::default()
            .set_extension(QUERY_PARALLELISM_HINT.to_string(), "1".to_string())
            .build(),
    );
    assert_rejected(&engine, parent, ctx, binding).await;
}
