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

pub(crate) mod file_stream;

use std::collections::HashSet;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use common_datasource::object_store::build_backend;
use common_query::prelude::Expr;
use common_recordbatch::error::Result as RecordBatchResult;
use common_recordbatch::{RecordBatch, RecordBatchStream, SendableRecordBatchStream};
use datafusion::logical_expr::utils as df_logical_expr_utils;
use datatypes::prelude::DataType;
use datatypes::schema::{Schema, SchemaRef};
use futures::Stream;
use snafu::{ensure, ResultExt};
use store_api::storage::ScanRequest;

use self::file_stream::{CreateScanPlanContext, ScanPlanConfig};
use crate::error::{
    BuildBackendSnafu, ExtractColumnFromFilterSnafu, ProjectionOutOfBoundsSnafu, Result,
};
use crate::region::FileRegion;

impl FileRegion {
    pub fn query(&self, request: ScanRequest) -> Result<SendableRecordBatchStream> {
        let store = build_backend(&self.url, &self.options).context(BuildBackendSnafu)?;

        let file_projection = self.projection_region_to_file(&request.projection)?;
        let file_filters = self.filters_region_to_file(&request.filters)?;
        let file_schema = Arc::new(Schema::new(self.file_options.file_column_schemas.clone()));

        let file_stream = file_stream::create_stream(
            &self.format,
            &CreateScanPlanContext::default(),
            &ScanPlanConfig {
                file_schema,
                files: &self.file_options.files,
                projection: file_projection.as_ref(),
                filters: &file_filters,
                limit: request.limit,
                store,
            },
        )?;

        let region_schema = if let Some(indices) = &request.projection {
            Arc::new(self.metadata.schema.try_project(indices).unwrap()) // TODO(zhongzc): error
        } else {
            self.metadata.schema.clone()
        };

        Ok(Box::pin(FileToRegionStream::new(
            region_schema,
            file_stream,
        )))
    }

    fn projection_region_to_file(
        &self,
        region_projection: &Option<Vec<usize>>,
    ) -> Result<Option<Vec<usize>>> {
        let Some(region_projection) = region_projection.as_ref() else {
            return Ok(None);
        };

        let file_column_schemas = &self.file_options.file_column_schemas;
        let mut file_projection = Vec::with_capacity(region_projection.len());
        for column_index in region_projection {
            ensure!(
                *column_index < self.metadata.schema.num_columns(),
                ProjectionOutOfBoundsSnafu {
                    column_index: *column_index,
                    bounds: self.metadata.schema.num_columns()
                }
            );

            let column_name = self.metadata.schema.column_name_by_index(*column_index);
            let file_column_index = file_column_schemas
                .iter()
                .position(|c| c.name == column_name);
            if let Some(file_column_index) = file_column_index {
                file_projection.push(file_column_index);
            }
        }
        Ok(Some(file_projection))
    }

    fn filters_region_to_file(&self, region_filters: &[Expr]) -> Result<Vec<Expr>> {
        let mut file_filters = Vec::with_capacity(region_filters.len());

        let file_column_names = self
            .file_options
            .file_column_schemas
            .iter()
            .map(|c| &c.name)
            .collect::<HashSet<_>>();

        let mut aux_column_set = HashSet::new();
        for region_filter in region_filters {
            df_logical_expr_utils::expr_to_columns(region_filter.df_expr(), &mut aux_column_set)
                .context(ExtractColumnFromFilterSnafu)?;

            let all_file_columns = aux_column_set
                .iter()
                .all(|column_in_expr| file_column_names.contains(&column_in_expr.name));
            if all_file_columns {
                file_filters.push(region_filter.clone());
            }
            aux_column_set.clear();
        }
        Ok(file_filters)
    }
}

struct FileToRegionStream {
    region_schema: SchemaRef,
    file_stream: SendableRecordBatchStream,
}

impl FileToRegionStream {
    fn new(region_schema: SchemaRef, file_stream: SendableRecordBatchStream) -> Self {
        Self {
            region_schema,
            file_stream,
        }
    }
}

impl RecordBatchStream for FileToRegionStream {
    fn schema(&self) -> SchemaRef {
        self.region_schema.clone()
    }
}

impl Stream for FileToRegionStream {
    type Item = RecordBatchResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.file_stream).poll_next(ctx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Some(file_record_batch)) => {
                let file_record_batch = file_record_batch?;
                let file_row_count = file_record_batch.num_rows();

                let region_schema = self.region_schema.clone();
                let mut columns = Vec::with_capacity(region_schema.num_columns());
                for region_column_schema in region_schema.column_schemas() {
                    let region_data_type = &region_column_schema.data_type;

                    let file_column = file_record_batch.column_by_name(&region_column_schema.name);
                    let column = if let Some(file_column) = file_column {
                        if &file_column.data_type() != region_data_type {
                            file_column.cast(region_data_type).unwrap() // TODO(zhongzc): error
                        } else {
                            file_column.clone()
                        }
                    } else if let Some(constraint) = region_column_schema.default_constraint() {
                        constraint
                            .create_default_vector(region_data_type, true, file_row_count)
                            .unwrap() // TODO(zhongzc): error
                    } else {
                        let mut mutable_vector = region_data_type.create_mutable_vector(1);
                        mutable_vector.push_null();
                        let base_vector = mutable_vector.to_vector();
                        base_vector.replicate(&[file_row_count])
                    };
                    columns.push(column);
                }

                let region_record_batch = RecordBatch::new(region_schema, columns)?;
                Poll::Ready(Some(Ok(region_record_batch)))
            }
            Poll::Ready(None) => Poll::Ready(None),
        }
    }
}
