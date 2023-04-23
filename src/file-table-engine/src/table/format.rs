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

use std::collections::HashMap;
use std::fmt::Display;
use std::sync::Arc;

use common_datasource::file_format::csv::{CsvConfigBuilder, CsvFormat, CsvOpener};
use common_datasource::file_format::json::{JsonFormat, JsonOpener};
use common_query::physical_plan::PhysicalPlanRef;
use common_query::prelude::Expr;
use common_recordbatch::adapter::RecordBatchStreamAdapter;
use datafusion::arrow::datatypes::Schema;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::physical_plan::file_format::{FileOpener, FileScanConfig, FileStream};
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datatypes::schema::SchemaRef;
use object_store::ObjectStore;
use snafu::ResultExt;
use table::table::scan::SimpleTableScan;

use crate::error::{self, Result};
use crate::table::immutable;

const DEFAULT_BATCH_SIZE: usize = 8192;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Format {
    Csv(CsvFormat),
    Json(JsonFormat),
    Parquet,
}

impl Display for Format {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Csv(_) => "CSV",
            Self::Json(_) => "JSON",
            Self::Parquet => "PARQUET",
        })
    }
}

pub const FORMAT_COMPRESSION_TYPE: &str = "COMPRESSION_TYPE";
pub const FORMAT_DELIMTERL: &str = "DELIMTERL";
pub const FORMAT_SCHEMA_INFER_MAX_RECORD: &str = "SCHEMA_INFER_MAX_RECORD";

impl TryFrom<&HashMap<String, String>> for Format {
    type Error = error::Error;

    fn try_from(value: &HashMap<String, String>) -> Result<Self> {
        let format = value
            .get(immutable::IMMUTABLE_TABLE_FORMAT_KEY)
            .cloned()
            .unwrap_or_default()
            .to_uppercase();

        match format.as_str() {
            "CSV" => Ok(Self::Csv(
                CsvFormat::try_from(value).context(error::ParseFileFormatSnafu)?,
            )),
            "JSON" => Ok(Self::Json(
                JsonFormat::try_from(value).context(error::ParseFileFormatSnafu)?,
            )),
            format => error::UnsupportedFileFormatSnafu { format }.fail(),
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct CreateScanPlanContext {}

impl Format {
    fn build_csv_opener(
        file_schema: Arc<Schema>,
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
        file_schema: Arc<Schema>,
        config: &ScanPlanConfig,
        format: &JsonFormat,
    ) -> Result<JsonOpener> {
        let projected_schema = if let Some(projection) = config.projection {
            Arc::new(
                file_schema
                    .project(projection)
                    .context(error::ProjectSchemaSnafu)?,
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

    fn build_scan_plan<T: FileOpener + Send + 'static>(
        opener: T,
        file_schema: Arc<Schema>,
        files: &[String],
        projection: Option<&Vec<usize>>,
        limit: Option<usize>,
    ) -> Result<PhysicalPlanRef> {
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
                output_ordering: None,
                infinite_source: false,
            },
            0, // partition: hard-code
            opener,
            &ExecutionPlanMetricsSet::new(),
        )
        .context(error::BuildStreamSnafu)?;
        let adapter = RecordBatchStreamAdapter::try_new(Box::pin(stream))
            .context(error::BuildStreamAdapterSnafu)?;
        Ok(Arc::new(SimpleTableScan::new(Box::pin(adapter))))
    }

    fn new_csv_scan_plan(
        _ctx: &CreateScanPlanContext,
        config: &ScanPlanConfig,
        format: &CsvFormat,
    ) -> Result<PhysicalPlanRef> {
        let file_schema = config.file_schema.arrow_schema().clone();
        let opener = Format::build_csv_opener(file_schema.clone(), config, format)?;
        Format::build_scan_plan(
            opener,
            file_schema,
            config.files,
            config.projection,
            config.limit,
        )
    }

    fn new_json_scan_plan(
        _ctx: &CreateScanPlanContext,
        config: &ScanPlanConfig,
        format: &JsonFormat,
    ) -> Result<PhysicalPlanRef> {
        let file_schema = config.file_schema.arrow_schema().clone();
        let opener = Format::build_json_opener(file_schema.clone(), config, format)?;
        Format::build_scan_plan(
            opener,
            file_schema,
            config.files,
            config.projection,
            config.limit,
        )
    }
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

impl Format {
    pub fn create_physical_plan(
        &self,
        ctx: &CreateScanPlanContext,
        config: &ScanPlanConfig,
    ) -> Result<PhysicalPlanRef> {
        match self {
            Format::Csv(format) => Format::new_csv_scan_plan(ctx, config, format),
            Format::Json(format) => Format::new_json_scan_plan(ctx, config, format),
            Format::Parquet => error::UnsupportedFileFormatSnafu { format: "parquet" }.fail(),
        }
    }
}
