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
use common_recordbatch::adapter::RecordBatchMetrics;
use common_recordbatch::error::{self as recordbatch_error, Result as RecordBatchResult};
use common_recordbatch::{
    DfSendableRecordBatchStream, OrderOption, RecordBatch, RecordBatchStream,
    SendableRecordBatchStream,
};
use datafusion::logical_expr::utils as df_logical_expr_utils;
use datafusion_expr::expr::Expr;
use datatypes::arrow::compute as arrow_compute;
use datatypes::data_type::DataType;
use datatypes::schema::{Schema, SchemaRef};
use datatypes::vectors::Helper;
use futures::Stream;
use snafu::{GenerateImplicitData, ResultExt, ensure};
use store_api::storage::ScanRequest;

use self::file_stream::ScanPlanConfig;
use crate::error::{
    BuildBackendSnafu, ExtractColumnFromFilterSnafu, ProjectSchemaSnafu,
    ProjectionOutOfBoundsSnafu, Result,
};
use crate::region::FileRegion;

impl FileRegion {
    pub fn query(&self, request: ScanRequest) -> Result<SendableRecordBatchStream> {
        let store = build_backend(&self.url, &self.options).context(BuildBackendSnafu)?;

        let file_projection = self.projection_pushdown_to_file(&request.projection)?;
        let file_filters = self.filters_pushdown_to_file(&request.filters)?;
        let file_schema = Arc::new(Schema::new(self.file_options.file_column_schemas.clone()));

        let projected_file_schema = if let Some(projection) = &file_projection {
            Arc::new(
                file_schema
                    .try_project(projection)
                    .context(ProjectSchemaSnafu)?,
            )
        } else {
            file_schema.clone()
        };

        let file_stream = file_stream::create_stream(
            &self.format,
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
            projected_file_schema,
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
            df_logical_expr_utils::expr_to_columns(scan_filter, &mut aux_column_set)
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
    file_stream: DfSendableRecordBatchStream,
    /// Maps columns in `scan_schema` to their index in the projected file schema.
    /// `None` means the column doesn't exist in the file and should be filled with default values.
    scan_to_file_projection: Vec<Option<usize>>,
}

impl RecordBatchStream for FileToScanRegionStream {
    fn schema(&self) -> SchemaRef {
        self.scan_schema.clone()
    }

    fn output_ordering(&self) -> Option<&[OrderOption]> {
        None
    }

    fn metrics(&self) -> Option<RecordBatchMetrics> {
        None
    }
}

impl Stream for FileToScanRegionStream {
    type Item = RecordBatchResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.file_stream).poll_next(ctx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Some(Ok(file_record_batch))) => {
                let num_rows = file_record_batch.num_rows();
                let mut columns = Vec::with_capacity(self.scan_schema.num_columns());

                for (idx, column_schema) in self.scan_schema.column_schemas().iter().enumerate() {
                    if let Some(file_idx) = self.scan_to_file_projection[idx] {
                        let expected_arrow_type = column_schema.data_type.as_arrow_type();
                        let mut array = file_record_batch.column(file_idx).clone();

                        if array.data_type() != &expected_arrow_type {
                            array = arrow_compute::cast(array.as_ref(), &expected_arrow_type)
                                .context(recordbatch_error::ArrowComputeSnafu)?;
                        }

                        let vector = Helper::try_into_vector(array)
                            .context(recordbatch_error::DataTypesSnafu)?;
                        columns.push(vector);
                    } else {
                        let vector = column_schema
                            .create_default_vector(num_rows)
                            .context(recordbatch_error::DataTypesSnafu)?
                            .ok_or_else(|| {
                                recordbatch_error::CreateRecordBatchesSnafu {
                                    reason: format!(
                                        "column {} is missing from file source and has no default",
                                        column_schema.name
                                    ),
                                }
                                .build()
                            })?;
                        columns.push(vector);
                    }
                }

                let record_batch = RecordBatch::new(self.scan_schema.clone(), columns)?;

                Poll::Ready(Some(Ok(record_batch)))
            }
            Poll::Ready(Some(Err(error))) => {
                Poll::Ready(Some(Err(recordbatch_error::Error::PollStream {
                    error,
                    location: snafu::Location::generate(),
                })))
            }
            Poll::Ready(None) => Poll::Ready(None),
        }
    }
}

impl FileToScanRegionStream {
    fn new(
        scan_schema: SchemaRef,
        file_schema: SchemaRef,
        file_stream: DfSendableRecordBatchStream,
    ) -> Self {
        let scan_to_file_projection = scan_schema
            .column_schemas()
            .iter()
            .map(|column| file_schema.column_index_by_name(&column.name))
            .collect();

        Self {
            scan_schema,
            file_stream,
            scan_to_file_projection,
        }
    }
}
