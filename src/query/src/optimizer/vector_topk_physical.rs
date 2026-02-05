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

//! Physical optimizer rule to fuse vector top-k into VectorTopKExec.

use std::collections::HashSet;
use std::sync::Arc;

use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::coalesce_batches::CoalesceBatchesExec;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::limit::{GlobalLimitExec, LocalLimitExec};
use datafusion::physical_plan::projection::{ProjectionExec, ProjectionExpr};
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{DataFusionError, Result};
use datafusion_physical_expr::utils::collect_columns;
use datafusion_physical_expr::{LexOrdering, PhysicalExpr};

use crate::vector_scan::{
    ProjectionConfig, VectorSearchScanExec, VectorTopKConfig, VectorTopKExec,
};

#[derive(Debug)]
pub struct VectorTopKPhysicalRule;

impl PhysicalOptimizerRule for VectorTopKPhysicalRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_up(|plan| {
            if let Some(rewrite) = try_rewrite_plan(plan.clone())? {
                return Ok(Transformed::yes(rewrite));
            }
            Ok(Transformed::no(plan))
        })
        .map(|plan| plan.data)
    }

    fn name(&self) -> &str {
        "VectorTopKPhysicalRule"
    }

    fn schema_check(&self) -> bool {
        false
    }
}

fn try_rewrite_plan(plan: Arc<dyn ExecutionPlan>) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    if let Some(local_limit) = plan.as_any().downcast_ref::<LocalLimitExec>() {
        return try_rewrite_plan(local_limit.input().clone());
    }

    if let Some(coalesce) = plan.as_any().downcast_ref::<CoalescePartitionsExec>() {
        return try_rewrite_plan(coalesce.input().clone());
    }

    if let Some(limit) = plan.as_any().downcast_ref::<GlobalLimitExec>() {
        let Some(fetch) = limit.fetch() else {
            return Ok(None);
        };
        let skip = limit.skip();
        let input = limit.input().clone();
        let Some(rewrite) = try_rewrite_sort_chain(input, fetch, skip)? else {
            return Ok(None);
        };
        return Ok(Some(rewrite));
    }

    if let Some(sort_merge) = plan.as_any().downcast_ref::<SortPreservingMergeExec>() {
        let input = sort_merge.input().clone();
        if let Some(sort) = input.as_any().downcast_ref::<SortExec>()
            && let Some(fetch) = sort.fetch()
        {
            let sort_exprs = sort.expr().to_vec();
            let Some(rewrite) = try_rewrite_chain(sort.input().clone(), sort_exprs, fetch, 0)?
            else {
                return Ok(None);
            };
            return Ok(Some(rewrite));
        }
        return Ok(None);
    }

    if let Some(sort) = plan.as_any().downcast_ref::<SortExec>()
        && let Some(fetch) = sort.fetch()
    {
        let sort_exprs = sort.expr().to_vec();
        let Some(rewrite) = try_rewrite_chain(sort.input().clone(), sort_exprs, fetch, 0)? else {
            return Ok(None);
        };
        return Ok(Some(rewrite));
    }

    Ok(None)
}

fn try_rewrite_sort_chain(
    plan: Arc<dyn ExecutionPlan>,
    fetch: usize,
    skip: usize,
) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    if fetch == 0 {
        return Ok(None);
    }

    if let Some(local_limit) = plan.as_any().downcast_ref::<LocalLimitExec>() {
        return try_rewrite_sort_chain(local_limit.input().clone(), fetch, skip);
    }

    if let Some(coalesce) = plan.as_any().downcast_ref::<CoalescePartitionsExec>() {
        return try_rewrite_sort_chain(coalesce.input().clone(), fetch, skip);
    }

    if let Some(sort_merge) = plan.as_any().downcast_ref::<SortPreservingMergeExec>() {
        let input = sort_merge.input().clone();
        if let Some(sort) = input.as_any().downcast_ref::<SortExec>() {
            let sort_exprs = sort.expr().to_vec();
            return try_rewrite_chain(sort.input().clone(), sort_exprs, fetch, skip);
        }
        return Ok(None);
    }

    if let Some(sort) = plan.as_any().downcast_ref::<SortExec>() {
        let sort_exprs = sort.expr().to_vec();
        return try_rewrite_chain(sort.input().clone(), sort_exprs, fetch, skip);
    }

    Ok(None)
}

fn try_rewrite_chain(
    input: Arc<dyn ExecutionPlan>,
    sort_exprs: Vec<datafusion_physical_expr::PhysicalSortExpr>,
    fetch: usize,
    skip: usize,
) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    if fetch == 0 {
        return Ok(None);
    }
    let mut current = input;
    let mut predicate: Option<Arc<dyn PhysicalExpr>> = None;
    let mut projection: Option<ProjectionConfig> = None;
    let mut projection_before_filter = false;

    loop {
        if let Some(filter) = current.as_any().downcast_ref::<FilterExec>() {
            if predicate.is_some() {
                return Ok(None);
            }
            predicate = Some(filter.predicate().clone());
            projection_before_filter = projection.is_none();
            current = filter.input().clone();
            continue;
        }

        if let Some(proj) = current.as_any().downcast_ref::<ProjectionExec>() {
            if projection.is_some() {
                return Ok(None);
            }
            let exprs: Vec<ProjectionExpr> = proj.expr().to_vec();
            let schema = proj.schema();
            projection = Some(ProjectionConfig { exprs, schema });
            if predicate.is_none() {
                projection_before_filter = false;
            }
            current = proj.input().clone();
            continue;
        }

        if let Some(repartition) = current.as_any().downcast_ref::<RepartitionExec>() {
            current = repartition.input().clone();
            continue;
        }

        if let Some(coalesce) = current.as_any().downcast_ref::<CoalesceBatchesExec>() {
            current = coalesce.input().clone();
            continue;
        }

        if let Some(coalesce) = current.as_any().downcast_ref::<CoalescePartitionsExec>() {
            current = coalesce.input().clone();
            continue;
        }

        if current
            .as_any()
            .is::<datafusion::physical_plan::coop::CooperativeExec>()
        {
            let Some(child) = current.children().into_iter().next() else {
                return Err(DataFusionError::Internal(
                    "CooperativeExec missing child".to_string(),
                ));
            };
            current = child.clone();
            continue;
        }

        break;
    }

    let Some(vector_scan) = current.as_any().downcast_ref::<VectorSearchScanExec>() else {
        return Ok(None);
    };

    if vector_scan.config().base_request.vector_search.is_none() {
        return Ok(None);
    }

    if projection.is_none() {
        projection_before_filter = false;
    }
    if let (Some(predicate), Some(projection)) = (&predicate, &projection) {
        let projected: HashSet<&str> = projection
            .schema
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect();
        let predicate_columns = collect_columns(predicate);
        if predicate_columns
            .iter()
            .any(|column| !projected.contains(column.name()))
        {
            projection_before_filter = false;
        }
    }

    let Some(sort_exprs) = LexOrdering::new(sort_exprs) else {
        return Ok(None);
    };

    let topk = VectorTopKConfig {
        sort_exprs,
        fetch,
        skip,
        predicate,
        projection,
        projection_before_filter,
    };
    let exec = VectorTopKExec::try_new(vector_scan.config(), current, topk)?;
    Ok(Some(Arc::new(exec)))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::{DataType, Field, Schema};
    use datafusion::physical_optimizer::PhysicalOptimizerRule;
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::physical_plan::filter::FilterExec;
    use datafusion::physical_plan::projection::{ProjectionExec, ProjectionExpr};
    use datafusion::physical_plan::sorts::sort::SortExec;
    use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
    use datafusion_common::Result;
    use datafusion_expr::Operator;
    use datafusion_physical_expr::expressions::{BinaryExpr, col, lit};
    use datafusion_physical_expr::{LexOrdering, PhysicalSortExpr};
    use datatypes::compute::SortOptions;
    use store_api::storage::{ScanRequest, VectorDistanceMetric, VectorSearchRequest};

    use crate::optimizer::test_util::MetaRegionEngine;
    use crate::optimizer::vector_topk_physical::VectorTopKPhysicalRule;
    use crate::test_util::MockInputExec;
    use crate::vector_scan::{VectorSearchScanExec, VectorTopKExec};

    fn build_vector_scan(schema: Arc<Schema>) -> Arc<VectorSearchScanExec> {
        let region_id = store_api::storage::RegionId::new(1, 0);
        let mut builder = store_api::metadata::RegionMetadataBuilder::new(region_id);
        builder
            .push_column_metadata(store_api::metadata::ColumnMetadata {
                column_schema: datatypes::schema::ColumnSchema::new(
                    "vec_id",
                    store_api::storage::ConcreteDataType::int32_datatype(),
                    false,
                ),
                semantic_type: api::v1::SemanticType::Tag,
                column_id: 1,
            })
            .push_column_metadata(store_api::metadata::ColumnMetadata {
                column_schema: datatypes::schema::ColumnSchema::new(
                    "embedding",
                    store_api::storage::ConcreteDataType::float32_datatype(),
                    false,
                ),
                semantic_type: api::v1::SemanticType::Field,
                column_id: 2,
            })
            .primary_key(vec![1]);
        let metadata = builder.build().unwrap();
        let engine = Arc::new(MetaRegionEngine::with_metadata(Arc::new(metadata)));
        let request = ScanRequest {
            vector_search: Some(VectorSearchRequest {
                column_id: 2,
                query_vector: vec![0.0, 0.0],
                k: 3,
                metric: VectorDistanceMetric::L2sq,
            }),
            ..Default::default()
        };
        let mock_input = Arc::new(MockInputExec::new(vec![Vec::new()], schema.clone()));
        Arc::new(VectorSearchScanExec::new(
            engine, region_id, request, mock_input, schema, None, false,
        ))
    }

    #[test]
    fn test_fuse_vector_topk_into_scan() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("vec_id", DataType::Int32, false),
            Field::new("embedding", DataType::Float32, false),
        ]));
        let scan = build_vector_scan(schema.clone());

        let proj_exprs = vec![
            ProjectionExpr {
                expr: col("vec_id", &schema)?,
                alias: "vec_id".to_string(),
            },
            ProjectionExpr {
                expr: col("embedding", &schema)?,
                alias: "embedding".to_string(),
            },
        ];
        let projection = Arc::new(ProjectionExec::try_new(proj_exprs, scan.clone())?);

        let sort_exprs = vec![PhysicalSortExpr {
            expr: col("embedding", &projection.schema())?,
            options: SortOptions::default(),
        }];
        let ordering = LexOrdering::new(sort_exprs.clone()).unwrap();
        let sort = Arc::new(SortExec::new(ordering.clone(), projection).with_fetch(Some(3)));
        let sort_merge = Arc::new(SortPreservingMergeExec::new(ordering, sort));

        let rule = VectorTopKPhysicalRule;
        let optimized =
            rule.optimize(sort_merge, &datafusion_common::config::ConfigOptions::new())?;

        let topk_exec = optimized
            .as_any()
            .downcast_ref::<VectorTopKExec>()
            .expect("expect VectorTopKExec after rewrite");
        let topk_config = topk_exec.topk();
        assert_eq!(topk_config.fetch, 3);
        assert_eq!(topk_config.skip, 0);
        assert!(topk_config.projection.is_some());
        assert!(!topk_config.projection_before_filter);
        Ok(())
    }

    #[test]
    fn test_projection_before_filter_flag() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("vec_id", DataType::Int32, false),
            Field::new("embedding", DataType::Float32, false),
        ]));
        let scan = build_vector_scan(schema.clone());

        let proj_exprs = vec![ProjectionExpr {
            expr: col("vec_id", &schema)?,
            alias: "vec_id".to_string(),
        }];
        let projection = Arc::new(ProjectionExec::try_new(proj_exprs, scan.clone())?);

        let predicate = Arc::new(BinaryExpr::new(
            col("vec_id", &projection.schema())?,
            Operator::Gt,
            lit(0i32),
        ));
        let filter = Arc::new(FilterExec::try_new(predicate, projection)?);

        let sort_exprs = vec![PhysicalSortExpr {
            expr: col("vec_id", &filter.schema())?,
            options: SortOptions::default(),
        }];
        let ordering = LexOrdering::new(sort_exprs.clone()).unwrap();
        let sort = Arc::new(SortExec::new(ordering.clone(), filter).with_fetch(Some(3)));
        let sort_merge = Arc::new(SortPreservingMergeExec::new(ordering, sort));

        let rule = VectorTopKPhysicalRule;
        let optimized =
            rule.optimize(sort_merge, &datafusion_common::config::ConfigOptions::new())?;

        let topk_exec = optimized
            .as_any()
            .downcast_ref::<VectorTopKExec>()
            .expect("expect VectorTopKExec after rewrite");
        let topk_config = topk_exec.topk();
        assert!(topk_config.projection_before_filter);
        Ok(())
    }

    #[test]
    fn test_filter_before_projection_flag() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("vec_id", DataType::Int32, false),
            Field::new("embedding", DataType::Float32, false),
        ]));
        let scan = build_vector_scan(schema.clone());

        let predicate = Arc::new(BinaryExpr::new(
            col("vec_id", &schema)?,
            Operator::Gt,
            lit(0i32),
        ));
        let filter = Arc::new(FilterExec::try_new(predicate, scan.clone())?);

        let proj_exprs = vec![ProjectionExpr {
            expr: col("vec_id", &filter.schema())?,
            alias: "vec_id".to_string(),
        }];
        let projection = Arc::new(ProjectionExec::try_new(proj_exprs, filter)?);

        let sort_exprs = vec![PhysicalSortExpr {
            expr: col("vec_id", &projection.schema())?,
            options: SortOptions::default(),
        }];
        let ordering = LexOrdering::new(sort_exprs.clone()).unwrap();
        let sort = Arc::new(SortExec::new(ordering.clone(), projection).with_fetch(Some(3)));
        let sort_merge = Arc::new(SortPreservingMergeExec::new(ordering, sort));

        let rule = VectorTopKPhysicalRule;
        let optimized =
            rule.optimize(sort_merge, &datafusion_common::config::ConfigOptions::new())?;

        let topk_exec = optimized
            .as_any()
            .downcast_ref::<VectorTopKExec>()
            .expect("expect VectorTopKExec after rewrite");
        let topk_config = topk_exec.topk();
        assert!(!topk_config.projection_before_filter);
        Ok(())
    }
}
