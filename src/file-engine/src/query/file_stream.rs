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

use common_datasource::file_format::csv::CsvFormat;
use common_datasource::file_format::parquet::DefaultParquetFileReaderFactory;
use common_datasource::file_format::Format;
use common_recordbatch::adapter::RecordBatchStreamAdapter;
use common_recordbatch::SendableRecordBatchStream;
use datafusion::common::ToDFSchema;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::datasource::physical_plan::{
    CsvSource, FileGroup, FileScanConfigBuilder, FileSource, FileStream, JsonSource, ParquetSource,
};
use datafusion::datasource::source::DataSourceExec;
use datafusion::physical_expr::create_physical_expr;
use datafusion::physical_expr::execution_props::ExecutionProps;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use datafusion_expr::expr::Expr;
use datafusion_expr::utils::conjunction;
use datafusion_orc::OrcSource;
use datatypes::arrow::datatypes::Schema as ArrowSchema;
use datatypes::schema::SchemaRef;
use object_store::ObjectStore;
use snafu::ResultExt;

use crate::error::{self, Result};

const DEFAULT_BATCH_SIZE: usize = 8192;

fn build_record_batch_stream(
    scan_plan_config: &ScanPlanConfig,
    file_schema: Arc<ArrowSchema>,
    limit: Option<usize>,
    file_source: Arc<dyn FileSource>,
) -> Result<SendableRecordBatchStream> {
    let files = scan_plan_config
        .files
        .iter()
        .map(|filename| PartitionedFile::new(filename.to_string(), 0))
        .collect::<Vec<_>>();

    let config = FileScanConfigBuilder::new(
        ObjectStoreUrl::local_filesystem(),
        file_schema,
        file_source.clone(),
    )
    .with_projection(scan_plan_config.projection.cloned())
    .with_limit(limit)
    .with_file_group(FileGroup::new(files))
    .build();

    let store = Arc::new(object_store_opendal::OpendalStore::new(
        scan_plan_config.store.clone(),
    ));

    let file_opener = file_source.create_file_opener(store, &config, 0);
    let stream = FileStream::new(
        &config,
        0, // partition: hard-code
        file_opener,
        &ExecutionPlanMetricsSet::new(),
    )
    .context(error::BuildStreamSnafu)?;
    let adapter = RecordBatchStreamAdapter::try_new(Box::pin(stream))
        .context(error::BuildStreamAdapterSnafu)?;
    Ok(Box::pin(adapter))
}

fn new_csv_stream(
    config: &ScanPlanConfig,
    format: &CsvFormat,
) -> Result<SendableRecordBatchStream> {
    let file_schema = config.file_schema.arrow_schema().clone();

    // push down limit only if there is no filter
    let limit = config.filters.is_empty().then_some(config.limit).flatten();

    let csv_source = CsvSource::new(format.has_header, format.delimiter, b'"')
        .with_schema(file_schema.clone())
        .with_batch_size(DEFAULT_BATCH_SIZE);

    build_record_batch_stream(config, file_schema, limit, csv_source)
}

fn new_json_stream(config: &ScanPlanConfig) -> Result<SendableRecordBatchStream> {
    let file_schema = config.file_schema.arrow_schema().clone();

    // push down limit only if there is no filter
    let limit = config.filters.is_empty().then_some(config.limit).flatten();

    let file_source = JsonSource::new().with_batch_size(DEFAULT_BATCH_SIZE);
    build_record_batch_stream(config, file_schema, limit, file_source)
}

fn new_parquet_stream_with_exec_plan(config: &ScanPlanConfig) -> Result<SendableRecordBatchStream> {
    let file_schema = config.file_schema.arrow_schema().clone();
    let ScanPlanConfig {
        files,
        projection,
        limit,
        filters,
        store,
        ..
    } = config;

    let file_group = FileGroup::new(
        files
            .iter()
            .map(|filename| PartitionedFile::new(filename.to_string(), 0))
            .collect::<Vec<_>>(),
    );

    let mut parquet_source = ParquetSource::default().with_parquet_file_reader_factory(Arc::new(
        DefaultParquetFileReaderFactory::new(store.clone()),
    ));

    // build predicate filter
    let filters = filters.to_vec();
    if let Some(expr) = conjunction(filters) {
        let df_schema = file_schema
            .clone()
            .to_dfschema_ref()
            .context(error::ParquetScanPlanSnafu)?;

        let filters = create_physical_expr(&expr, &df_schema, &ExecutionProps::new())
            .context(error::ParquetScanPlanSnafu)?;
        parquet_source = parquet_source.with_predicate(filters);
    };

    let file_scan_config = FileScanConfigBuilder::new(
        ObjectStoreUrl::local_filesystem(),
        file_schema,
        Arc::new(parquet_source),
    )
    .with_file_group(file_group)
    .with_projection(projection.cloned())
    .with_limit(*limit)
    .build();

    // TODO(ruihang): get this from upper layer
    let task_ctx = SessionContext::default().task_ctx();

    let parquet_exec = DataSourceExec::from_data_source(file_scan_config);
    let stream = parquet_exec
        .execute(0, task_ctx)
        .context(error::ParquetScanPlanSnafu)?;

    Ok(Box::pin(
        RecordBatchStreamAdapter::try_new(stream).context(error::BuildStreamAdapterSnafu)?,
    ))
}

fn new_orc_stream(config: &ScanPlanConfig) -> Result<SendableRecordBatchStream> {
    let file_schema = config.file_schema.arrow_schema().clone();

    // push down limit only if there is no filter
    let limit = config.filters.is_empty().then_some(config.limit).flatten();

    let file_source = OrcSource::default().with_batch_size(DEFAULT_BATCH_SIZE);
    build_record_batch_stream(config, file_schema, limit, file_source)
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
    config: &ScanPlanConfig,
) -> Result<SendableRecordBatchStream> {
    match format {
        Format::Csv(format) => new_csv_stream(config, format),
        Format::Json(_) => new_json_stream(config),
        Format::Parquet(_) => new_parquet_stream_with_exec_plan(config),
        Format::Orc(_) => new_orc_stream(config),
    }
}
