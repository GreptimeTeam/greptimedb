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

//! `information_schema.table_semantics`: the queryable view over the table
//! semantic layer. One row per table that carries at least one
//! `greptime.semantic.*` option, so a consumer can discover the observability
//! concept a table stands for with a single SQL query instead of parsing every
//! table's `create_options`.
//!
//! The few signal-agnostic keys are promoted to their own columns
//! (`signal_type` / `source` / `pipeline` / `metadata_quality`); the remaining
//! signal-specific keys are folded into a `semantic_options` JSON string, keyed
//! by the option name with the `greptime.semantic.` prefix stripped.

use std::collections::BTreeMap;
use std::sync::{Arc, Weak};

use arrow_schema::SchemaRef as ArrowSchemaRef;
use common_catalog::consts::INFORMATION_SCHEMA_TABLE_SEMANTICS_TABLE_ID;
use common_error::ext::BoxedError;
use common_recordbatch::adapter::RecordBatchStreamAdapter;
use common_recordbatch::{RecordBatch, SendableRecordBatchStream};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::SendableRecordBatchStream as DfSendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter as DfRecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::PartitionStream as DfPartitionStream;
use datatypes::prelude::{ConcreteDataType, ScalarVectorBuilder, VectorRef};
use datatypes::schema::{ColumnSchema, Schema, SchemaRef};
use datatypes::value::Value;
use datatypes::vectors::{StringVectorBuilder, UInt32VectorBuilder};
use futures::TryStreamExt;
use snafu::{OptionExt, ResultExt};
use store_api::storage::{ScanRequest, TableId};
use table::metadata::TableInfo;
use table::requests::{
    SEMANTIC_METRIC_METADATA_QUALITY, SEMANTIC_PIPELINE, SEMANTIC_PREFIX, SEMANTIC_SIGNAL_TYPE,
    SEMANTIC_SOURCE, is_semantic_option_key,
};

use crate::CatalogManager;
use crate::error::{
    CreateRecordBatchSnafu, InternalSnafu, Result, UpgradeWeakCatalogManagerRefSnafu,
};
use crate::system_schema::information_schema::{InformationTable, Predicates, TABLE_SEMANTICS};

pub const TABLE_CATALOG: &str = "table_catalog";
pub const TABLE_SCHEMA: &str = "table_schema";
pub const TABLE_NAME: &str = "table_name";
pub const TABLE_ID: &str = "table_id";
pub const SIGNAL_TYPE: &str = "signal_type";
pub const SOURCE: &str = "source";
pub const PIPELINE: &str = "pipeline";
pub const METADATA_QUALITY: &str = "metadata_quality";
pub const SEMANTIC_OPTIONS: &str = "semantic_options";

const INIT_CAPACITY: usize = 42;

fn optional_value(v: Option<&str>) -> Value {
    v.map(Value::from).unwrap_or(Value::Null)
}

/// The semantic projection of a single table: the signal-agnostic keys promoted
/// to columns, plus a JSON tail for the rest. Borrows from the table's options.
struct SemanticRow<'a> {
    signal_type: Option<&'a str>,
    source: Option<&'a str>,
    pipeline: Option<&'a str>,
    metadata_quality: Option<&'a str>,
    options_json: Option<String>,
}

impl<'a> SemanticRow<'a> {
    /// Projects a table's options onto the semantic schema, or `None` when the
    /// table carries no semantic key at all.
    fn extract(table_info: &'a TableInfo) -> Option<Self> {
        let mut signal_type = None;
        let mut source = None;
        let mut pipeline = None;
        let mut metadata_quality = None;
        let mut rest = BTreeMap::new();

        for (key, value) in &table_info.meta.options.extra_options {
            if !is_semantic_option_key(key) {
                continue;
            }
            match key.as_str() {
                SEMANTIC_SIGNAL_TYPE => signal_type = Some(value.as_str()),
                SEMANTIC_SOURCE => source = Some(value.as_str()),
                SEMANTIC_PIPELINE => pipeline = Some(value.as_str()),
                SEMANTIC_METRIC_METADATA_QUALITY => metadata_quality = Some(value.as_str()),
                _ => {
                    let short = key.strip_prefix(SEMANTIC_PREFIX).unwrap_or(key);
                    rest.insert(short, value.as_str());
                }
            }
        }

        let has_any = signal_type.is_some()
            || source.is_some()
            || pipeline.is_some()
            || metadata_quality.is_some()
            || !rest.is_empty();
        if !has_any {
            return None;
        }

        // `rest` is a `BTreeMap`, so the JSON keys come out sorted and the output
        // is stable across runs. Serializing a string map can't realistically fail,
        // but fold a failure into `None` rather than panicking the query path.
        let options_json = (!rest.is_empty())
            .then(|| serde_json::to_string(&rest).ok())
            .flatten();

        Some(Self {
            signal_type,
            source,
            pipeline,
            metadata_quality,
            options_json,
        })
    }
}

#[derive(Debug)]
pub(super) struct InformationSchemaTableSemantics {
    schema: SchemaRef,
    catalog_name: String,
    catalog_manager: Weak<dyn CatalogManager>,
}

impl InformationSchemaTableSemantics {
    pub(super) fn new(catalog_name: String, catalog_manager: Weak<dyn CatalogManager>) -> Self {
        Self {
            schema: Self::schema(),
            catalog_name,
            catalog_manager,
        }
    }

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            ColumnSchema::new(TABLE_CATALOG, ConcreteDataType::string_datatype(), false),
            ColumnSchema::new(TABLE_SCHEMA, ConcreteDataType::string_datatype(), false),
            ColumnSchema::new(TABLE_NAME, ConcreteDataType::string_datatype(), false),
            ColumnSchema::new(TABLE_ID, ConcreteDataType::uint32_datatype(), false),
            ColumnSchema::new(SIGNAL_TYPE, ConcreteDataType::string_datatype(), true),
            ColumnSchema::new(SOURCE, ConcreteDataType::string_datatype(), true),
            ColumnSchema::new(PIPELINE, ConcreteDataType::string_datatype(), true),
            ColumnSchema::new(METADATA_QUALITY, ConcreteDataType::string_datatype(), true),
            ColumnSchema::new(SEMANTIC_OPTIONS, ConcreteDataType::string_datatype(), true),
        ]))
    }

    fn builder(&self) -> InformationSchemaSemanticTablesBuilder {
        InformationSchemaSemanticTablesBuilder::new(
            self.schema.clone(),
            self.catalog_name.clone(),
            self.catalog_manager.clone(),
        )
    }
}

impl InformationTable for InformationSchemaTableSemantics {
    fn table_id(&self) -> TableId {
        INFORMATION_SCHEMA_TABLE_SEMANTICS_TABLE_ID
    }

    fn table_name(&self) -> &'static str {
        TABLE_SEMANTICS
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn to_stream(&self, request: ScanRequest) -> Result<SendableRecordBatchStream> {
        let schema = self.schema.arrow_schema().clone();
        let mut builder = self.builder();
        let stream = Box::pin(DfRecordBatchStreamAdapter::new(
            schema,
            futures::stream::once(async move {
                builder
                    .make_tables(Some(request))
                    .await
                    .map(|x| x.into_df_record_batch())
                    .map_err(|err| datafusion::error::DataFusionError::External(Box::new(err)))
            }),
        ));
        Ok(Box::pin(
            RecordBatchStreamAdapter::try_new(stream)
                .map_err(BoxedError::new)
                .context(InternalSnafu)?,
        ))
    }
}

struct InformationSchemaSemanticTablesBuilder {
    schema: SchemaRef,
    catalog_name: String,
    catalog_manager: Weak<dyn CatalogManager>,

    catalog_names: StringVectorBuilder,
    schema_names: StringVectorBuilder,
    table_names: StringVectorBuilder,
    table_ids: UInt32VectorBuilder,
    signal_types: StringVectorBuilder,
    sources: StringVectorBuilder,
    pipelines: StringVectorBuilder,
    metadata_qualities: StringVectorBuilder,
    semantic_options: StringVectorBuilder,
}

impl InformationSchemaSemanticTablesBuilder {
    fn new(
        schema: SchemaRef,
        catalog_name: String,
        catalog_manager: Weak<dyn CatalogManager>,
    ) -> Self {
        Self {
            schema,
            catalog_name,
            catalog_manager,
            catalog_names: StringVectorBuilder::with_capacity(INIT_CAPACITY),
            schema_names: StringVectorBuilder::with_capacity(INIT_CAPACITY),
            table_names: StringVectorBuilder::with_capacity(INIT_CAPACITY),
            table_ids: UInt32VectorBuilder::with_capacity(INIT_CAPACITY),
            signal_types: StringVectorBuilder::with_capacity(INIT_CAPACITY),
            sources: StringVectorBuilder::with_capacity(INIT_CAPACITY),
            pipelines: StringVectorBuilder::with_capacity(INIT_CAPACITY),
            metadata_qualities: StringVectorBuilder::with_capacity(INIT_CAPACITY),
            semantic_options: StringVectorBuilder::with_capacity(INIT_CAPACITY),
        }
    }

    async fn make_tables(&mut self, request: Option<ScanRequest>) -> Result<RecordBatch> {
        let catalog_name = self.catalog_name.clone();
        let catalog_manager = self
            .catalog_manager
            .upgrade()
            .context(UpgradeWeakCatalogManagerRefSnafu)?;
        let predicates = Predicates::from_scan_request(&request);

        for schema_name in catalog_manager.schema_names(&catalog_name, None).await? {
            let mut table_stream = catalog_manager.tables(&catalog_name, &schema_name, None);
            while let Some(table) = table_stream.try_next().await? {
                self.add_table(&predicates, &catalog_name, &schema_name, table.table_info());
            }
        }

        self.finish()
    }

    fn add_table(
        &mut self,
        predicates: &Predicates,
        catalog_name: &str,
        schema_name: &str,
        table_info: Arc<TableInfo>,
    ) {
        // A table with no semantic key is not part of the semantic layer.
        let Some(row) = SemanticRow::extract(&table_info) else {
            return;
        };

        let table_name = table_info.name.as_ref();
        let catalog_v = Value::from(catalog_name);
        let schema_v = Value::from(schema_name);
        let name_v = Value::from(table_name);
        let signal_v = optional_value(row.signal_type);
        let source_v = optional_value(row.source);
        let pipeline_v = optional_value(row.pipeline);
        let quality_v = optional_value(row.metadata_quality);
        let predicate_row = [
            (TABLE_CATALOG, &catalog_v),
            (TABLE_SCHEMA, &schema_v),
            (TABLE_NAME, &name_v),
            (SIGNAL_TYPE, &signal_v),
            (SOURCE, &source_v),
            (PIPELINE, &pipeline_v),
            (METADATA_QUALITY, &quality_v),
        ];
        if !predicates.eval(&predicate_row) {
            return;
        }

        self.catalog_names.push(Some(catalog_name));
        self.schema_names.push(Some(schema_name));
        self.table_names.push(Some(table_name));
        self.table_ids.push(Some(table_info.table_id()));
        self.signal_types.push(row.signal_type);
        self.sources.push(row.source);
        self.pipelines.push(row.pipeline);
        self.metadata_qualities.push(row.metadata_quality);
        self.semantic_options.push(row.options_json.as_deref());
    }

    fn finish(&mut self) -> Result<RecordBatch> {
        let columns: Vec<VectorRef> = vec![
            Arc::new(self.catalog_names.finish()),
            Arc::new(self.schema_names.finish()),
            Arc::new(self.table_names.finish()),
            Arc::new(self.table_ids.finish()),
            Arc::new(self.signal_types.finish()),
            Arc::new(self.sources.finish()),
            Arc::new(self.pipelines.finish()),
            Arc::new(self.metadata_qualities.finish()),
            Arc::new(self.semantic_options.finish()),
        ];
        RecordBatch::new(self.schema.clone(), columns).context(CreateRecordBatchSnafu)
    }
}

impl DfPartitionStream for InformationSchemaTableSemantics {
    fn schema(&self) -> &ArrowSchemaRef {
        self.schema.arrow_schema()
    }

    fn execute(&self, _: Arc<TaskContext>) -> DfSendableRecordBatchStream {
        let schema = self.schema.arrow_schema().clone();
        let mut builder = self.builder();
        Box::pin(DfRecordBatchStreamAdapter::new(
            schema,
            futures::stream::once(async move {
                builder
                    .make_tables(None)
                    .await
                    .map(|x| x.into_df_record_batch())
                    .map_err(Into::into)
            }),
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, MITO_ENGINE};
    use datatypes::schema::SchemaBuilder;
    use table::metadata::{TableInfoBuilder, TableMeta, TableType};
    use table::requests::{SEMANTIC_METRIC_TYPE, SEMANTIC_METRIC_UNIT, TableOptions};

    use super::*;

    fn table_info(extra: &[(&str, &str)]) -> TableInfo {
        let schema = Arc::new(
            SchemaBuilder::try_from_columns(vec![ColumnSchema::new(
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            )])
            .unwrap()
            .build()
            .unwrap(),
        );
        let options = TableOptions {
            extra_options: extra
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect::<HashMap<_, _>>(),
            ..Default::default()
        };
        let meta = TableMeta {
            schema,
            primary_key_indices: vec![],
            value_indices: vec![],
            engine: MITO_ENGINE.to_string(),
            next_column_id: 1,
            options,
            created_on: Default::default(),
            updated_on: Default::default(),
            partition_key_indices: vec![],
            column_ids: vec![],
        };
        TableInfoBuilder::default()
            .table_id(1)
            .name("t")
            .catalog_name(DEFAULT_CATALOG_NAME)
            .schema_name(DEFAULT_SCHEMA_NAME)
            .table_version(0)
            .table_type(TableType::Base)
            .meta(meta)
            .build()
            .unwrap()
    }

    #[test]
    fn extract_promotes_core_keys_and_folds_the_rest() {
        let info = table_info(&[
            (SEMANTIC_SIGNAL_TYPE, "metric"),
            (SEMANTIC_SOURCE, "opentelemetry"),
            (SEMANTIC_PIPELINE, "greptime_metric_v1"),
            (SEMANTIC_METRIC_METADATA_QUALITY, "declared"),
            (SEMANTIC_METRIC_TYPE, "counter"),
            (SEMANTIC_METRIC_UNIT, "By"),
            // A non-semantic option must be ignored entirely.
            ("ttl", "7d"),
        ]);

        let row = SemanticRow::extract(&info).unwrap();
        assert_eq!(row.signal_type, Some("metric"));
        assert_eq!(row.source, Some("opentelemetry"));
        assert_eq!(row.pipeline, Some("greptime_metric_v1"));
        assert_eq!(row.metadata_quality, Some("declared"));
        // Promoted keys stay out of the JSON tail; remaining keys are sorted and
        // prefix-stripped.
        assert_eq!(
            row.options_json.as_deref(),
            Some(r#"{"metric.type":"counter","metric.unit":"By"}"#)
        );
    }

    #[test]
    fn extract_skips_untagged_table() {
        let info = table_info(&[("ttl", "7d")]);
        assert!(SemanticRow::extract(&info).is_none());
    }

    #[test]
    fn extract_omits_json_when_only_core_keys_present() {
        let info = table_info(&[(SEMANTIC_SIGNAL_TYPE, "log")]);
        let row = SemanticRow::extract(&info).unwrap();
        assert_eq!(row.signal_type, Some("log"));
        assert!(row.source.is_none());
        assert!(row.options_json.is_none());
    }
}
