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

use common_datasource::file_format::csv::{CsvConfigBuilder, CsvFormat, CsvOpener};
use common_datasource::file_format::json::{JsonFormat, JsonOpener};
use common_datasource::file_format::orc::{OrcFormat, OrcOpener};
use common_datasource::file_format::parquet::{DefaultParquetFileReaderFactory, ParquetFormat};
use common_datasource::file_format::Format;
use common_query::prelude::Expr;
use common_query::DfPhysicalPlan;
use common_recordbatch::adapter::RecordBatchStreamAdapter;
use common_recordbatch::SendableRecordBatchStream;
use datafusion::common::ToDFSchema;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::datasource::physical_plan::{FileOpener, FileScanConfig, FileStream, ParquetExec};
use datafusion::optimizer::utils::conjunction;
use datafusion::physical_expr::create_physical_expr;
use datafusion::physical_expr::execution_props::ExecutionProps;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::prelude::SessionContext;
use datatypes::arrow::datatypes::Schema as ArrowSchema;
use datatypes::schema::SchemaRef;
use object_store::ObjectStore;
use snafu::ResultExt;

use crate::error::{self, Result};

const DEFAULT_BATCH_SIZE: usize = 8192;

#[derive(Debug, Clone, Copy, Default)]
pub struct CreateScanPlanContext {}

fn build_csv_opener(
    file_schema: Arc<ArrowSchema>,
    config: &ScanPlanConfig,
    format: &CsvFormat,
) -> Result<CsvOpener> {
    let csv_config = CsvConfigBuilder::default()
        .batch_size(DEFAULT_BATCH_SIZE)
        .file_schema(file_schema)
        .file_projection(config.projection.cloned())
        .delimiter(format.delimiter)
        .has_header(format.has_header)
        .build()
        .context(error::BuildCsvConfigSnafu)?;
    Ok(CsvOpener::new(
        csv_config,
        config.store.clone(),
        format.compression_type,
    ))
}

fn build_json_opener(
    file_schema: Arc<ArrowSchema>,
    config: &ScanPlanConfig,
    format: &JsonFormat,
) -> Result<JsonOpener> {
    let projected_schema = if let Some(projection) = config.projection {
        Arc::new(
            file_schema
                .project(projection)
                .context(error::ProjectArrowSchemaSnafu)?,
        )
    } else {
        file_schema
    };
    Ok(JsonOpener::new(
        DEFAULT_BATCH_SIZE,
        projected_schema,
        config.store.clone(),
        format.compression_type,
    ))
}

fn build_orc_opener(output_schema: Arc<ArrowSchema>, config: &ScanPlanConfig) -> Result<OrcOpener> {
    Ok(OrcOpener::new(
        config.store.clone(),
        output_schema,
        config.projection.cloned(),
    ))
}

fn build_record_batch_stream<T: FileOpener + Send + 'static>(
    opener: T,
    file_schema: Arc<ArrowSchema>,
    files: &[String],
    projection: Option<&Vec<usize>>,
    limit: Option<usize>,
) -> Result<SendableRecordBatchStream> {
    let stream = FileStream::new(
        &FileScanConfig {
            object_store_url: ObjectStoreUrl::parse("empty://").unwrap(), // won't be used
            file_schema,
            file_groups: vec![files
                .iter()
                .map(|filename| PartitionedFile::new(filename.to_string(), 0))
                .collect::<Vec<_>>()],
            statistics: Default::default(),
            projection: projection.cloned(),
            limit,
            table_partition_cols: vec![],
            output_ordering: vec![],
            infinite_source: false,
        },
        0, // partition: hard-code
        opener,
        &ExecutionPlanMetricsSet::new(),
    )
    .context(error::BuildStreamSnafu)?;
    let adapter = RecordBatchStreamAdapter::try_new(Box::pin(stream))
        .context(error::BuildStreamAdapterSnafu)?;
    Ok(Box::pin(adapter))
}

fn new_csv_stream(
    _ctx: &CreateScanPlanContext,
    config: &ScanPlanConfig,
    format: &CsvFormat,
) -> Result<SendableRecordBatchStream> {
    let file_schema = config.file_schema.arrow_schema().clone();
    let opener = build_csv_opener(file_schema.clone(), config, format)?;
    // push down limit only if there is no filter
    let limit = config.filters.is_empty().then_some(config.limit).flatten();
    build_record_batch_stream(opener, file_schema, config.files, config.projection, limit)
}

fn new_json_stream(
    _ctx: &CreateScanPlanContext,
    config: &ScanPlanConfig,
    format: &JsonFormat,
) -> Result<SendableRecordBatchStream> {
    let file_schema = config.file_schema.arrow_schema().clone();
    let opener = build_json_opener(file_schema.clone(), config, format)?;
    // push down limit only if there is no filter
    let limit = config.filters.is_empty().then_some(config.limit).flatten();
    build_record_batch_stream(opener, file_schema, config.files, config.projection, limit)
}

fn new_parquet_stream_with_exec_plan(
    _ctx: &CreateScanPlanContext,
    config: &ScanPlanConfig,
    _format: &ParquetFormat,
) -> Result<SendableRecordBatchStream> {
    let file_schema = config.file_schema.arrow_schema().clone();
    let ScanPlanConfig {
        files,
        projection,
        limit,
        filters,
        store,
        ..
    } = config;

    // construct config for ParquetExec
    let scan_config = FileScanConfig {
        object_store_url: ObjectStoreUrl::parse("empty://").unwrap(), // won't be used
        file_schema: file_schema.clone(),
        file_groups: vec![files
            .iter()
            .map(|filename| PartitionedFile::new(filename.to_string(), 0))
            .collect::<Vec<_>>()],
        statistics: Default::default(),
        projection: projection.cloned(),
        limit: *limit,
        table_partition_cols: vec![],
        output_ordering: vec![],
        infinite_source: false,
    };

    // build predicate filter
    let filters = filters
        .iter()
        .map(|f| f.df_expr().clone())
        .collect::<Vec<_>>();
    let filters = if let Some(expr) = conjunction(filters) {
        let df_schema = file_schema
            .clone()
            .to_dfschema_ref()
            .context(error::ParquetScanPlanSnafu)?;

        let filters = create_physical_expr(&expr, &df_schema, &file_schema, &ExecutionProps::new())
            .context(error::ParquetScanPlanSnafu)?;
        Some(filters)
    } else {
        None
    };

    // TODO(ruihang): get this from upper layer
    let task_ctx = SessionContext::default().task_ctx();
    let parquet_exec = ParquetExec::new(scan_config, filters, None)
        .with_parquet_file_reader_factory(Arc::new(DefaultParquetFileReaderFactory::new(
            store.clone(),
        )));
    let stream = parquet_exec
        .execute(0, task_ctx)
        .context(error::ParquetScanPlanSnafu)?;

    Ok(Box::pin(
        RecordBatchStreamAdapter::try_new(stream).context(error::BuildStreamAdapterSnafu)?,
    ))
}

fn new_orc_stream(
    _ctx: &CreateScanPlanContext,
    config: &ScanPlanConfig,
    _format: &OrcFormat,
) -> Result<SendableRecordBatchStream> {
    let file_schema = config.file_schema.arrow_schema().clone();
    let opener = build_orc_opener(file_schema.clone(), config)?;
    // push down limit only if there is no filter
    let limit = config.filters.is_empty().then_some(config.limit).flatten();
    build_record_batch_stream(opener, file_schema, config.files, config.projection, limit)
}

#[derive(Debug, Clone)]
pub struct ScanPlanConfig<'a> {
    pub file_schema: SchemaRef,
    pub files: &'a Vec<String>,
    pub projection: Option<&'a Vec<usize>>,
    pub filters: &'a [Expr],
    pub limit: Option<usize>,
    pub store: ObjectStore,
}

pub fn create_stream(
    format: &Format,
    ctx: &CreateScanPlanContext,
    config: &ScanPlanConfig,
) -> Result<SendableRecordBatchStream> {
    match format {
        Format::Csv(format) => new_csv_stream(ctx, config, format),
        Format::Json(format) => new_json_stream(ctx, config, format),
        Format::Parquet(format) => new_parquet_stream_with_exec_plan(ctx, config, format),
        Format::Orc(format) => new_orc_stream(ctx, config, format),
    }
}
