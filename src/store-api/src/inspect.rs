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

use common_recordbatch::DfRecordBatch;
use datafusion_common::DataFusionError;
use datafusion_expr::{LogicalPlan, LogicalPlanBuilder, LogicalTableSource};
use datatypes::arrow::array::ArrayRef;
use datatypes::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use datatypes::arrow::error::ArrowError;
use datatypes::arrow::record_batch::RecordBatchOptions;
use datatypes::schema::SchemaRef;

use crate::storage::ScanRequest;

/// Builds projected inspection batches while sharing arrays for repeated columns.
pub(crate) struct ProjectedBatchBuilder {
    schema: ArrowSchemaRef,
    duplicates: Vec<Option<usize>>,
}

impl ProjectedBatchBuilder {
    /// Prepares duplicate-column mappings once per scan.
    pub(crate) fn new(schema: ArrowSchemaRef) -> Self {
        let duplicates = schema
            .fields()
            .iter()
            .enumerate()
            .map(|(index, field)| {
                schema.fields()[..index]
                    .iter()
                    .position(|previous| previous == field)
            })
            .collect();
        Self { schema, duplicates }
    }

    /// Builds each distinct projected column once and preserves empty-projection row counts.
    pub(crate) fn build(
        &self,
        row_count: usize,
        mut build_column: impl FnMut(&str) -> Result<ArrayRef, ArrowError>,
    ) -> Result<DfRecordBatch, ArrowError> {
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(self.schema.fields().len());
        for (index, field) in self.schema.fields().iter().enumerate() {
            let array = match self.duplicates[index] {
                Some(previous) => columns[previous].clone(),
                None => build_column(field.name())?,
            };
            columns.push(array);
        }
        DfRecordBatch::try_new_with_options(
            self.schema.clone(),
            columns,
            &RecordBatchOptions::new().with_row_count(Some(row_count)),
        )
    }
}

/// Builds an inspection scan with filtering before projection and limit.
pub(crate) fn build_plan(
    scan_request: ScanRequest,
    table_name: &str,
    schema: SchemaRef,
) -> Result<LogicalPlan, DataFusionError> {
    let table_source = LogicalTableSource::new(schema.arrow_schema().clone());

    // Filters may reference columns omitted from the requested output. Apply
    // them to the full schema and let the optimizer push the minimal projection.
    let projection = scan_request.projection;
    let mut builder = LogicalPlanBuilder::scan(table_name, Arc::new(table_source), None)?;

    for filter in scan_request.filters {
        builder = builder.filter(filter)?;
    }

    if let Some(projection) = projection {
        let projected = schema.arrow_schema().project(&projection)?;
        builder = builder.project(
            projected
                .fields()
                .iter()
                .map(|field| datafusion_expr::col(field.name())),
        )?;
    }

    if let Some(limit) = scan_request.limit {
        builder = builder.limit(0, Some(limit))?;
    }

    builder.build()
}
