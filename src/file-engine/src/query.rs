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
use common_error::ext::BoxedError;
use common_query::prelude::Expr;
use common_recordbatch::error::{CastVectorSnafu, ExternalSnafu, Result as RecordBatchResult};
use common_recordbatch::{RecordBatch, RecordBatchStream, SendableRecordBatchStream};
use datafusion::logical_expr::utils as df_logical_expr_utils;
use datatypes::schema::{ColumnSchema, Schema, SchemaRef};
use datatypes::vectors::VectorRef;
use futures::Stream;
use snafu::{ensure, OptionExt, ResultExt};
use store_api::storage::ScanRequest;

use self::file_stream::{CreateScanPlanContext, ScanPlanConfig};
use crate::error::{
    BuildBackendSnafu, CreateDefaultSnafu, ExtractColumnFromFilterSnafu,
    MissingColumnNoDefaultSnafu, ProjectSchemaSnafu, ProjectionOutOfBoundsSnafu, Result,
};
use crate::region::FileRegion;

impl FileRegion {
    pub fn query(&self, request: ScanRequest) -> Result<SendableRecordBatchStream> {
        let store = build_backend(&self.url, &self.options).context(BuildBackendSnafu)?;

        let file_projection = self.projection_pushdown_to_file(&request.projection)?;
        let file_filters = self.filters_pushdown_to_file(&request.filters)?;
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

        let scan_schema = self.scan_schema(&request.projection)?;

        Ok(Box::pin(FileToScanRegionStream::new(
            scan_schema,
            file_stream,
        )))
    }

    fn projection_pushdown_to_file(
        &self,
        req_projection: &Option<Vec<usize>>,
    ) -> Result<Option<Vec<usize>>> {
        let Some(scan_projection) = req_projection.as_ref() else {
            return Ok(None);
        };

        let file_column_schemas = &self.file_options.file_column_schemas;
        let mut file_projection = Vec::with_capacity(scan_projection.len());
        for column_index in scan_projection {
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

    // Collects filters that can be pushed down to the file, specifically filters where Expr
    // only contains columns from the file.
    fn filters_pushdown_to_file(&self, scan_filters: &[Expr]) -> Result<Vec<Expr>> {
        let mut file_filters = Vec::with_capacity(scan_filters.len());

        let file_column_names = self
            .file_options
            .file_column_schemas
            .iter()
            .map(|c| &c.name)
            .collect::<HashSet<_>>();

        let mut aux_column_set = HashSet::new();
        for scan_filter in scan_filters {
            df_logical_expr_utils::expr_to_columns(scan_filter.df_expr(), &mut aux_column_set)
                .context(ExtractColumnFromFilterSnafu)?;

            let all_file_columns = aux_column_set
                .iter()
                .all(|column_in_expr| file_column_names.contains(&column_in_expr.name));
            if all_file_columns {
                file_filters.push(scan_filter.clone());
            }
            aux_column_set.clear();
        }
        Ok(file_filters)
    }

    fn scan_schema(&self, req_projection: &Option<Vec<usize>>) -> Result<SchemaRef> {
        let schema = if let Some(indices) = req_projection {
            Arc::new(
                self.metadata
                    .schema
                    .try_project(indices)
                    .context(ProjectSchemaSnafu)?,
            )
        } else {
            self.metadata.schema.clone()
        };

        Ok(schema)
    }
}

struct FileToScanRegionStream {
    scan_schema: SchemaRef,
    file_stream: SendableRecordBatchStream,
}

impl RecordBatchStream for FileToScanRegionStream {
    fn schema(&self) -> SchemaRef {
        self.scan_schema.clone()
    }
}

impl Stream for FileToScanRegionStream {
    type Item = RecordBatchResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.file_stream).poll_next(ctx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Some(file_record_batch)) => {
                let file_record_batch = file_record_batch?;

                if self.schema_type_match(&file_record_batch) {
                    return Poll::Ready(Some(Ok(file_record_batch)));
                }

                let file_row_count = file_record_batch.num_rows();
                let scan_schema = self.scan_schema.clone();
                let mut columns = Vec::with_capacity(scan_schema.num_columns());
                for scan_column_schema in scan_schema.column_schemas() {
                    let scan_data_type = &scan_column_schema.data_type;

                    let file_column = file_record_batch.column_by_name(&scan_column_schema.name);
                    let column = if let Some(file_column) = file_column {
                        if &file_column.data_type() == scan_data_type {
                            file_column.clone()
                        } else {
                            file_column.cast(scan_data_type).context(CastVectorSnafu {
                                from_type: file_column.data_type(),
                                to_type: scan_data_type.clone(),
                            })?
                        }
                    } else {
                        Self::create_default_vector(scan_column_schema, file_row_count)
                            .map_err(BoxedError::new)
                            .context(ExternalSnafu)?
                    };

                    columns.push(column);
                }

                let scan_record_batch = RecordBatch::new(scan_schema, columns)?;
                Poll::Ready(Some(Ok(scan_record_batch)))
            }
            Poll::Ready(None) => Poll::Ready(None),
        }
    }
}

impl FileToScanRegionStream {
    fn new(scan_schema: SchemaRef, file_stream: SendableRecordBatchStream) -> Self {
        Self {
            scan_schema,
            file_stream,
        }
    }

    fn schema_type_match(&self, file_record_batch: &RecordBatch) -> bool {
        self.scan_schema
            .column_schemas()
            .iter()
            .all(|scan_column_schema| {
                file_record_batch
                    .column_by_name(&scan_column_schema.name)
                    .map(|rb| rb.data_type() == scan_column_schema.data_type)
                    .unwrap_or_default()
            })
    }

    fn create_default_vector(column_schema: &ColumnSchema, num_rows: usize) -> Result<VectorRef> {
        column_schema
            .create_default_vector(num_rows)
            .with_context(|_| CreateDefaultSnafu {
                column: column_schema.name.clone(),
            })?
            .with_context(|| MissingColumnNoDefaultSnafu {
                column: column_schema.name.clone(),
            })
    }
}
