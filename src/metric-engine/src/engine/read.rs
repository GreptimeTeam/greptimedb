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

use std::collections::HashSet;
use std::sync::Arc;

use api::v1::SemanticType;
use common_telemetry::{debug, error, tracing};
use datafusion::arrow::datatypes::DataType as ArrowDataType;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion, TreeNodeRewriter};
use datafusion::common::{Column, Result as DataFusionResult};
use datafusion::functions::expr_fn::coalesce;
use datafusion::logical_expr::expr_fn::cast;
use datafusion::logical_expr::utils::expr_to_columns;
use datafusion::logical_expr::{self, Expr};
use datatypes::prelude::ConcreteDataType;
use snafu::{OptionExt, ResultExt};
use store_api::metadata::{RegionMetadataBuilder, RegionMetadataRef};
use store_api::metric_engine_consts::{
    DATA_SCHEMA_TABLE_ID_COLUMN_NAME, is_metric_engine_value_int_column,
    metric_engine_value_int_column_name,
};
use store_api::region_engine::{RegionEngine, RegionScannerRef};
use store_api::region_request::AlterKind;
use store_api::storage::{RegionId, ScanRequest, SequenceNumber};

use crate::engine::MetricEngineInner;
use crate::error::{
    InvalidMetadataSnafu, InvalidRequestSnafu, LogicalRegionNotFoundSnafu, MitoReadOperationSnafu,
    Result,
};
use crate::metrics::MITO_OPERATION_ELAPSED;
use crate::utils;
use crate::value_split::{ValueColumnProjection, ValueSplitProjectionMapper, ValueSplitScanner};

impl MetricEngineInner {
    #[tracing::instrument(skip_all)]
    pub async fn read_region(
        &self,
        region_id: RegionId,
        request: ScanRequest,
    ) -> Result<RegionScannerRef> {
        let is_reading_physical_region = self.is_physical_region(region_id);

        if is_reading_physical_region {
            debug!(
                "Metric region received read request {request:?} on physical region {region_id:?}"
            );
            self.read_physical_region(region_id, request).await
        } else {
            self.read_logical_region(region_id, request).await
        }
    }

    /// Proxy the read request to underlying physical region (mito engine).
    async fn read_physical_region(
        &self,
        region_id: RegionId,
        request: ScanRequest,
    ) -> Result<RegionScannerRef> {
        let _timer = MITO_OPERATION_ELAPSED
            .with_label_values(&["read_physical"])
            .start_timer();

        let data_region_id = utils::to_data_region_id(region_id);
        let physical_metadata = self
            .mito
            .get_metadata(data_region_id)
            .await
            .context(MitoReadOperationSnafu)?;
        let visible_metadata = visible_physical_region_metadata(&physical_metadata)?;
        let (request, mapper) = self.transform_request_with_mapper(
            data_region_id,
            request,
            &visible_metadata,
            &physical_metadata,
            None,
        )?;

        let scanner = self
            .mito
            .handle_query(data_region_id, request)
            .await
            .context(MitoReadOperationSnafu)?;

        Ok(Box::new(ValueSplitScanner::new(
            scanner,
            visible_metadata,
            mapper,
        )))
    }

    async fn read_logical_region(
        &self,
        logical_region_id: RegionId,
        request: ScanRequest,
    ) -> Result<RegionScannerRef> {
        let _timer = MITO_OPERATION_ELAPSED
            .with_label_values(&["read"])
            .start_timer();

        let physical_region_id = self.get_physical_region_id(logical_region_id).await?;
        let data_region_id = utils::to_data_region_id(physical_region_id);
        let logical_metadata = self
            .logical_region_metadata(physical_region_id, logical_region_id)
            .await?;
        let (request, mapper) = self
            .transform_logical_request_with_mapper(
                physical_region_id,
                logical_region_id,
                request,
                &logical_metadata,
            )
            .await?;
        let mut scanner = self
            .mito
            .handle_query(data_region_id, request)
            .await
            .context(MitoReadOperationSnafu)?;
        scanner.set_logical_region(true);
        scanner.set_query_load_region_id(data_region_id);

        Ok(Box::new(ValueSplitScanner::new(
            scanner,
            logical_metadata,
            mapper,
        )))
    }

    pub async fn get_last_seq_num(&self, region_id: RegionId) -> Result<SequenceNumber> {
        let region_id = if self.is_physical_region(region_id) {
            region_id
        } else {
            let physical_region_id = self.get_physical_region_id(region_id).await?;
            utils::to_data_region_id(physical_region_id)
        };
        self.mito
            .get_committed_sequence(region_id)
            .await
            .context(MitoReadOperationSnafu)
    }

    pub async fn load_region_metadata(&self, region_id: RegionId) -> Result<RegionMetadataRef> {
        let is_reading_physical_region =
            self.state.read().unwrap().exist_physical_region(region_id);

        if is_reading_physical_region {
            let data_region_id = utils::to_data_region_id(region_id);
            let physical_metadata = self
                .mito
                .get_metadata(data_region_id)
                .await
                .context(MitoReadOperationSnafu)?;
            visible_physical_region_metadata(&physical_metadata)
        } else {
            let physical_region_id = self.get_physical_region_id(region_id).await?;
            self.logical_region_metadata(physical_region_id, region_id)
                .await
        }
    }

    /// Returns true if it's a physical region.
    pub fn is_physical_region(&self, region_id: RegionId) -> bool {
        self.state.read().unwrap().exist_physical_region(region_id)
    }

    async fn get_physical_region_id(&self, logical_region_id: RegionId) -> Result<RegionId> {
        let state = &self.state.read().unwrap();
        state
            .get_physical_region_id(logical_region_id)
            .with_context(|| {
                error!("Trying to read an nonexistent region {logical_region_id}");
                LogicalRegionNotFoundSnafu {
                    region_id: logical_region_id,
                }
            })
    }

    /// Transform the [ScanRequest] from logical region to physical data region.
    #[cfg(test)]
    async fn transform_request(
        &self,
        physical_region_id: RegionId,
        logical_region_id: RegionId,
        request: ScanRequest,
    ) -> Result<ScanRequest> {
        let logical_metadata = self
            .logical_region_metadata(physical_region_id, logical_region_id)
            .await?;
        self.transform_logical_request_with_mapper(
            physical_region_id,
            logical_region_id,
            request,
            &logical_metadata,
        )
        .await
        .map(|(request, _)| request)
    }

    async fn transform_logical_request_with_mapper(
        &self,
        physical_region_id: RegionId,
        logical_region_id: RegionId,
        request: ScanRequest,
        logical_metadata: &RegionMetadataRef,
    ) -> Result<(ScanRequest, ValueSplitProjectionMapper)> {
        let data_region_id = utils::to_data_region_id(physical_region_id);
        let physical_metadata = self
            .mito
            .get_metadata(data_region_id)
            .await
            .context(MitoReadOperationSnafu)?;
        self.transform_request_with_mapper(
            logical_region_id,
            request,
            logical_metadata,
            &physical_metadata,
            Some(logical_region_id),
        )
    }

    fn transform_request_with_mapper(
        &self,
        region_id: RegionId,
        mut request: ScanRequest,
        visible_metadata: &RegionMetadataRef,
        physical_metadata: &RegionMetadataRef,
        logical_region_id: Option<RegionId>,
    ) -> Result<(ScanRequest, ValueSplitProjectionMapper)> {
        let split_value_columns = split_value_columns(visible_metadata, physical_metadata);
        let mut residual_column_names = HashSet::new();
        let residual_filters = request
            .filters
            .iter()
            .filter(|filter| {
                let mut columns = HashSet::new();
                let is_residual = expr_to_columns(filter, &mut columns).is_ok()
                    && columns
                        .iter()
                        .any(|column| split_value_columns.contains(&column.name));
                if is_residual {
                    residual_column_names.extend(columns.into_iter().map(|column| column.name));
                }
                is_residual
            })
            .cloned()
            .collect::<Vec<_>>();
        let mut visible_projection = match request.projection_input.as_ref() {
            Some(projection_input) => projection_input.projection.clone(),
            None => (0..visible_metadata.column_metadatas.len()).collect(),
        };
        let visible_columns = visible_projection.len();
        let mut projected = visible_projection.iter().copied().collect::<HashSet<_>>();
        for (index, column) in visible_metadata.column_metadatas.iter().enumerate() {
            if residual_column_names.contains(&column.column_schema.name) && projected.insert(index)
            {
                visible_projection.push(index);
            }
        }
        let (physical_projection, mapper) = self.transform_projection_with_mapper(
            region_id,
            &visible_projection,
            visible_metadata,
            physical_metadata,
            visible_columns,
            residual_filters,
        )?;

        // Top-level projections are indices; nested paths are column names.
        request.projection_input.get_or_insert_default().projection = physical_projection;
        request.filters = request
            .filters
            .into_iter()
            .map(|filter| rewrite_metric_value_filter(region_id, filter, &split_value_columns))
            .collect::<Result<Vec<_>>>()?;
        if let Some(logical_region_id) = logical_region_id {
            request.filters.push(
                logical_expr::col(DATA_SCHEMA_TABLE_ID_COLUMN_NAME)
                    .eq(logical_expr::lit(logical_region_id.table_id())),
            );
        }

        Ok((request, mapper))
    }

    fn transform_projection_with_mapper(
        &self,
        logical_region_id: RegionId,
        origin_projection: &[usize],
        logical_metadata: &RegionMetadataRef,
        physical_metadata: &RegionMetadataRef,
        visible_columns: usize,
        residual_filters: Vec<Expr>,
    ) -> Result<(Vec<usize>, ValueSplitProjectionMapper)> {
        let mut physical_projection = Vec::with_capacity(origin_projection.len());
        let mut output_columns = Vec::with_capacity(origin_projection.len());

        for logical_idx in origin_projection {
            let logical_column = logical_metadata
                .column_metadatas
                .get(*logical_idx)
                .with_context(|| InvalidRequestSnafu {
                    region_id: logical_region_id,
                    reason: format!("projection index {} is out of bound", logical_idx),
                })?;
            let name = &logical_column.column_schema.name;
            // Safety: logical columns is a strict subset of physical columns
            let float_index = physical_metadata.column_index_by_name(name).unwrap();
            let input_float_index = physical_projection.len();
            physical_projection.push(float_index);

            if logical_column.semantic_type == SemanticType::Field
                && logical_column.column_schema.data_type == ConcreteDataType::float64_datatype()
                && let Some(int_index) = physical_metadata
                    .column_index_by_name(&metric_engine_value_int_column_name(name))
            {
                let input_int_index = physical_projection.len();
                physical_projection.push(int_index);
                output_columns.push(ValueColumnProjection::Split {
                    float_index: input_float_index,
                    int_index: input_int_index,
                    output_schema: logical_column.column_schema.clone(),
                });
            } else {
                output_columns.push(ValueColumnProjection::Direct {
                    input_index: input_float_index,
                    output_schema: logical_column.column_schema.clone(),
                });
            }
        }

        Ok((
            physical_projection,
            ValueSplitProjectionMapper::new(output_columns, visible_columns, residual_filters),
        ))
    }

    pub async fn logical_region_metadata(
        &self,
        physical_region_id: RegionId,
        logical_region_id: RegionId,
    ) -> Result<RegionMetadataRef> {
        let logical_columns = self
            .load_logical_columns(physical_region_id, logical_region_id)
            .await?;

        let primary_keys = logical_columns
            .iter()
            .filter_map(|col| {
                if col.semantic_type == SemanticType::Tag {
                    Some(col.column_id)
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        let mut logical_metadata_builder = RegionMetadataBuilder::new(logical_region_id);
        for col in logical_columns {
            logical_metadata_builder.push_column_metadata(col);
        }
        logical_metadata_builder.primary_key(primary_keys);
        let logical_metadata = logical_metadata_builder
            .build()
            .context(InvalidMetadataSnafu)?;

        Ok(Arc::new(logical_metadata))
    }
}

fn visible_physical_region_metadata(
    physical_metadata: &RegionMetadataRef,
) -> Result<RegionMetadataRef> {
    let visible_columns = physical_metadata
        .column_metadatas
        .iter()
        .filter(|column| !is_metric_engine_value_int_column(&column.column_schema.name))
        .cloned()
        .collect::<Vec<_>>();
    if visible_columns.len() == physical_metadata.column_metadatas.len() {
        return Ok(physical_metadata.clone());
    }

    let primary_key = physical_metadata.primary_key.clone();
    let mut builder = RegionMetadataBuilder::from_existing((**physical_metadata).clone());
    builder
        .alter(AlterKind::SyncColumns {
            column_metadatas: visible_columns,
        })
        .context(InvalidMetadataSnafu)?;
    builder.primary_key(primary_key);
    builder.build().map(Arc::new).context(InvalidMetadataSnafu)
}

fn split_value_columns(
    logical_metadata: &RegionMetadataRef,
    physical_metadata: &RegionMetadataRef,
) -> HashSet<String> {
    logical_metadata
        .column_metadatas
        .iter()
        .filter(|column| {
            column.semantic_type == SemanticType::Field
                && column.column_schema.data_type == ConcreteDataType::float64_datatype()
        })
        .filter_map(|column| {
            let value_name = &column.column_schema.name;
            physical_metadata
                .column_by_name(&metric_engine_value_int_column_name(value_name))
                .filter(|int_column| {
                    int_column.semantic_type == SemanticType::Field
                        && int_column.column_schema.data_type == ConcreteDataType::int64_datatype()
                })
                .map(|_| value_name.clone())
        })
        .collect()
}

fn rewrite_metric_value_filter(
    logical_region_id: RegionId,
    filter: Expr,
    split_value_columns: &HashSet<String>,
) -> Result<Expr> {
    if split_value_columns.is_empty() {
        return Ok(filter);
    }

    let filter_display = filter.to_string();
    let mut rewriter = MetricValueFilterRewriter {
        split_value_columns,
    };
    filter
        .rewrite(&mut rewriter)
        .map(|rewritten| rewritten.data)
        .map_err(|err| {
            InvalidRequestSnafu {
                region_id: logical_region_id,
                reason: format!("failed to rewrite metric value filter {filter_display}: {err}"),
            }
            .build()
        })
}

struct MetricValueFilterRewriter<'a> {
    split_value_columns: &'a HashSet<String>,
}

impl TreeNodeRewriter for MetricValueFilterRewriter<'_> {
    type Node = Expr;

    fn f_down(&mut self, expr: Expr) -> DataFusionResult<Transformed<Expr>> {
        let recursion = if matches!(
            expr,
            Expr::Exists(_) | Expr::InSubquery(_) | Expr::ScalarSubquery(_)
        ) {
            TreeNodeRecursion::Jump
        } else {
            TreeNodeRecursion::Continue
        };

        Ok(Transformed::new(expr, false, recursion))
    }

    fn f_up(&mut self, expr: Expr) -> DataFusionResult<Transformed<Expr>> {
        let Expr::Column(column) = expr else {
            return Ok(Transformed::no(expr));
        };

        if !self.split_value_columns.contains(&column.name) {
            return Ok(Transformed::no(Expr::Column(column)));
        }

        let int_column = Column {
            relation: column.relation.clone(),
            name: metric_engine_value_int_column_name(&column.name),
            spans: column.spans.clone(),
        };
        let float_expr = Expr::Column(column);
        let int_expr = cast(Expr::Column(int_column), ArrowDataType::Float64);
        Ok(Transformed::yes(coalesce(vec![int_expr, float_expr])))
    }
}

#[cfg(test)]
impl MetricEngineInner {
    pub async fn scan_to_stream(
        &self,
        region_id: RegionId,
        request: ScanRequest,
    ) -> Result<common_recordbatch::SendableRecordBatchStream, common_error::ext::BoxedError> {
        let scanner = self
            .read_region(region_id, request)
            .await
            .map_err(common_error::ext::BoxedError::new)?;
        let metrics_set = datafusion::physical_plan::metrics::ExecutionPlanMetricsSet::new();
        let streams = (0..scanner.properties().num_partitions())
            .map(|partition| {
                scanner.scan_partition(
                    &store_api::region_engine::QueryScanContext::default(),
                    &metrics_set,
                    partition,
                )
            })
            .collect::<std::result::Result<Vec<_>, _>>()?;
        common_recordbatch::util::ChainedRecordBatchStream::new(streams)
            .map(|stream| Box::pin(stream) as _)
            .map_err(common_error::ext::BoxedError::new)
    }
}

#[cfg(test)]
mod test {
    use store_api::region_request::RegionRequest;

    use super::*;
    use crate::test_util::{
        TestEnv, alter_logical_region_add_tag_columns, create_logical_region_request,
    };

    #[tokio::test]
    async fn test_transform_scan_req() {
        let env = TestEnv::new().await;
        env.init_metric_region().await;

        let logical_region_id = env.default_logical_region_id();
        let physical_region_id = env.default_physical_region_id();

        // create another logical region
        let logical_region_id2 = RegionId::new(1112345678, 999);
        let create_request =
            create_logical_region_request(&["123", "456", "789"], physical_region_id, "blabla");
        env.metric()
            .handle_request(logical_region_id2, RegionRequest::Create(create_request))
            .await
            .unwrap();

        // add columns to the first logical region
        let alter_request =
            alter_logical_region_add_tag_columns(123456, &["987", "798", "654", "321"]);
        env.metric()
            .handle_request(logical_region_id, RegionRequest::Alter(alter_request))
            .await
            .unwrap();

        // check explicit projection
        let projection_input = Some(vec![0, 1, 2, 3, 4, 5, 6].into());
        let scan_req = ScanRequest {
            projection_input,
            filters: vec![],
            ..Default::default()
        };

        let scan_req = env
            .metric()
            .inner
            .transform_request(physical_region_id, logical_region_id, scan_req)
            .await
            .unwrap();

        assert_eq!(
            scan_req.projection_indices().unwrap(),
            &[12, 11, 10, 9, 0, 1, 2, 5]
        );
        assert_eq!(scan_req.filters.len(), 1);
        assert_eq!(
            scan_req.filters[0],
            logical_expr::col(DATA_SCHEMA_TABLE_ID_COLUMN_NAME)
                .eq(logical_expr::lit(logical_region_id.table_id()))
        );

        // check default projection
        let scan_req = ScanRequest::default();
        let scan_req = env
            .metric()
            .inner
            .transform_request(physical_region_id, logical_region_id, scan_req)
            .await
            .unwrap();
        assert_eq!(
            scan_req.projection_indices().unwrap(),
            &[12, 11, 10, 9, 0, 1, 2, 5]
        );
    }
}
