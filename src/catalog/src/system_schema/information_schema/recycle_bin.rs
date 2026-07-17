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

use std::sync::{Arc, Weak};

use common_catalog::consts::INFORMATION_SCHEMA_RECYCLE_BIN_TABLE_ID;
use common_error::ext::BoxedError;
use common_meta::key::DroppedTableName;
use common_recordbatch::adapter::RecordBatchStreamAdapter;
use common_recordbatch::{RecordBatch, SendableRecordBatchStream};
use common_time::timestamp::Timestamp;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter as DfRecordBatchStreamAdapter;
use datatypes::prelude::{ConcreteDataType, ScalarVectorBuilder, VectorRef};
use datatypes::schema::{ColumnSchema, Schema, SchemaRef};
use datatypes::timestamp::TimestampMillisecond;
use datatypes::value::Value;
use datatypes::vectors::{
    BooleanVectorBuilder, StringVectorBuilder, TimestampMillisecondVectorBuilder,
    UInt64VectorBuilder,
};
use snafu::ResultExt;
use store_api::storage::{ScanRequest, TableId};

use crate::CatalogManager;
use crate::error::{CreateRecordBatchSnafu, InternalSnafu, Result, TableMetadataManagerSnafu};
use crate::system_schema::information_schema::{InformationTable, Predicates, RECYCLE_BIN};
use crate::system_schema::utils;

const OBJECT_ID: &str = "object_id";
const OBJECT_TYPE: &str = "object_type";
const ORIGINAL_OBJECT_NAME: &str = "original_object_name";
const ORIGINAL_CATALOG_NAME: &str = "original_catalog_name";
const ORIGINAL_SCHEMA_NAME: &str = "original_schema_name";
const DROPPED_AT: &str = "dropped_at";
const DROPPED_BY: &str = "dropped_by";
const RETENTION_EXPIRES_AT: &str = "retention_expires_at";
const PURGE_STATUS: &str = "purge_status";
const RESTORABLE: &str = "restorable";
const RESTORED_AT: &str = "restored_at";
const RESTORED_BY: &str = "restored_by";

const TABLE_OBJECT_TYPE: &str = "TABLE";
const ACTIVE_PURGE_STATUS: &str = "ACTIVE";
const INIT_CAPACITY: usize = 42;

#[derive(Debug)]
pub(super) struct InformationSchemaRecycleBin {
    schema: SchemaRef,
    catalog_name: String,
    catalog_manager: Weak<dyn CatalogManager>,
}

impl InformationSchemaRecycleBin {
    pub(super) fn new(catalog_name: String, catalog_manager: Weak<dyn CatalogManager>) -> Self {
        Self {
            schema: Self::schema(),
            catalog_name,
            catalog_manager,
        }
    }

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            ColumnSchema::new(OBJECT_ID, ConcreteDataType::uint64_datatype(), false),
            ColumnSchema::new(OBJECT_TYPE, ConcreteDataType::string_datatype(), false),
            ColumnSchema::new(
                ORIGINAL_OBJECT_NAME,
                ConcreteDataType::string_datatype(),
                false,
            ),
            ColumnSchema::new(
                ORIGINAL_CATALOG_NAME,
                ConcreteDataType::string_datatype(),
                false,
            ),
            ColumnSchema::new(
                ORIGINAL_SCHEMA_NAME,
                ConcreteDataType::string_datatype(),
                false,
            ),
            ColumnSchema::new(
                DROPPED_AT,
                ConcreteDataType::timestamp_millisecond_datatype(),
                true,
            ),
            ColumnSchema::new(DROPPED_BY, ConcreteDataType::string_datatype(), true),
            ColumnSchema::new(
                RETENTION_EXPIRES_AT,
                ConcreteDataType::timestamp_millisecond_datatype(),
                true,
            ),
            ColumnSchema::new(PURGE_STATUS, ConcreteDataType::string_datatype(), false),
            ColumnSchema::new(RESTORABLE, ConcreteDataType::boolean_datatype(), false),
            ColumnSchema::new(
                RESTORED_AT,
                ConcreteDataType::timestamp_millisecond_datatype(),
                true,
            ),
            ColumnSchema::new(RESTORED_BY, ConcreteDataType::string_datatype(), true),
        ]))
    }

    fn builder(&self) -> InformationSchemaRecycleBinBuilder {
        InformationSchemaRecycleBinBuilder::new(
            self.schema.clone(),
            self.catalog_name.clone(),
            self.catalog_manager.clone(),
        )
    }
}

impl InformationTable for InformationSchemaRecycleBin {
    fn table_id(&self) -> TableId {
        INFORMATION_SCHEMA_RECYCLE_BIN_TABLE_ID
    }

    fn table_name(&self) -> &'static str {
        RECYCLE_BIN
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
                    .make_recycle_bin(Some(request))
                    .await
                    .map(|batch| batch.into_df_record_batch())
                    .map_err(Into::into)
            }),
        ));
        Ok(Box::pin(
            RecordBatchStreamAdapter::try_new(stream)
                .map_err(BoxedError::new)
                .context(InternalSnafu)?,
        ))
    }
}

struct InformationSchemaRecycleBinBuilder {
    schema: SchemaRef,
    catalog_name: String,
    catalog_manager: Weak<dyn CatalogManager>,
    object_ids: UInt64VectorBuilder,
    object_types: StringVectorBuilder,
    original_object_names: StringVectorBuilder,
    original_catalog_names: StringVectorBuilder,
    original_schema_names: StringVectorBuilder,
    dropped_at: TimestampMillisecondVectorBuilder,
    dropped_by: StringVectorBuilder,
    retention_expires_at: TimestampMillisecondVectorBuilder,
    purge_statuses: StringVectorBuilder,
    restorables: BooleanVectorBuilder,
    restored_at: TimestampMillisecondVectorBuilder,
    restored_by: StringVectorBuilder,
}

impl InformationSchemaRecycleBinBuilder {
    fn new(
        schema: SchemaRef,
        catalog_name: String,
        catalog_manager: Weak<dyn CatalogManager>,
    ) -> Self {
        Self {
            schema,
            catalog_name,
            catalog_manager,
            object_ids: UInt64VectorBuilder::with_capacity(INIT_CAPACITY),
            object_types: StringVectorBuilder::with_capacity(INIT_CAPACITY),
            original_object_names: StringVectorBuilder::with_capacity(INIT_CAPACITY),
            original_catalog_names: StringVectorBuilder::with_capacity(INIT_CAPACITY),
            original_schema_names: StringVectorBuilder::with_capacity(INIT_CAPACITY),
            dropped_at: TimestampMillisecondVectorBuilder::with_capacity(INIT_CAPACITY),
            dropped_by: StringVectorBuilder::with_capacity(INIT_CAPACITY),
            retention_expires_at: TimestampMillisecondVectorBuilder::with_capacity(INIT_CAPACITY),
            purge_statuses: StringVectorBuilder::with_capacity(INIT_CAPACITY),
            restorables: BooleanVectorBuilder::with_capacity(INIT_CAPACITY),
            restored_at: TimestampMillisecondVectorBuilder::with_capacity(INIT_CAPACITY),
            restored_by: StringVectorBuilder::with_capacity(INIT_CAPACITY),
        }
    }

    async fn make_recycle_bin(&mut self, request: Option<ScanRequest>) -> Result<RecordBatch> {
        let Some(table_metadata_manager) = utils::table_meta_manager(&self.catalog_manager)? else {
            return self.finish();
        };
        let mut dropped_tables = table_metadata_manager
            .list_dropped_tables_by_catalog(&self.catalog_name)
            .await
            .context(TableMetadataManagerSnafu)?;
        dropped_tables.retain(|table| !table.purging);
        dropped_tables.sort_unstable_by(|a, b| {
            (
                &a.table_name.catalog_name,
                &a.table_name.schema_name,
                &a.table_name.table_name,
                a.table_id,
            )
                .cmp(&(
                    &b.table_name.catalog_name,
                    &b.table_name.schema_name,
                    &b.table_name.table_name,
                    b.table_id,
                ))
        });

        let predicates = Predicates::from_scan_request(&request);
        for dropped_table in &dropped_tables {
            self.add_table(&predicates, dropped_table);
        }
        self.finish()
    }

    fn add_table(&mut self, predicates: &Predicates, dropped_table: &DroppedTableName) {
        let table_name = &dropped_table.table_name;
        let object_id = u64::from(dropped_table.table_id);
        let dropped_at = dropped_table
            .dropped_at
            .map(|value| TimestampMillisecond(Timestamp::new_millisecond(value)));
        let retention_expires_at = dropped_table
            .retention_expires_at
            .map(|value| TimestampMillisecond(Timestamp::new_millisecond(value)));
        let object_id_value = Value::from(object_id);
        let object_type_value = Value::from(TABLE_OBJECT_TYPE);
        let object_name_value = Value::from(table_name.table_name.as_str());
        let catalog_name_value = Value::from(table_name.catalog_name.as_str());
        let schema_name_value = Value::from(table_name.schema_name.as_str());
        let purge_status_value = Value::from(ACTIVE_PURGE_STATUS);
        let restorable_value = Value::from(true);
        let dropped_at_value = dropped_at.map(Value::from);
        let retention_expires_at_value = retention_expires_at.map(Value::from);

        let mut row = Vec::with_capacity(9);
        row.extend([
            (OBJECT_ID, &object_id_value),
            (OBJECT_TYPE, &object_type_value),
            (ORIGINAL_OBJECT_NAME, &object_name_value),
            (ORIGINAL_CATALOG_NAME, &catalog_name_value),
            (ORIGINAL_SCHEMA_NAME, &schema_name_value),
            (PURGE_STATUS, &purge_status_value),
            (RESTORABLE, &restorable_value),
        ]);
        if let Some(value) = &dropped_at_value {
            row.push((DROPPED_AT, value));
        }
        if let Some(value) = &retention_expires_at_value {
            row.push((RETENTION_EXPIRES_AT, value));
        }
        if !predicates.eval(&row) {
            return;
        }

        self.object_ids.push(Some(object_id));
        self.object_types.push(Some(TABLE_OBJECT_TYPE));
        self.original_object_names
            .push(Some(&table_name.table_name));
        self.original_catalog_names
            .push(Some(&table_name.catalog_name));
        self.original_schema_names
            .push(Some(&table_name.schema_name));
        self.dropped_at.push(dropped_at);
        self.dropped_by.push(None);
        self.retention_expires_at.push(retention_expires_at);
        self.purge_statuses.push(Some(ACTIVE_PURGE_STATUS));
        self.restorables.push(Some(true));
        self.restored_at.push(None);
        self.restored_by.push(None);
    }

    fn finish(&mut self) -> Result<RecordBatch> {
        let columns: Vec<VectorRef> = vec![
            Arc::new(self.object_ids.finish()),
            Arc::new(self.object_types.finish()),
            Arc::new(self.original_object_names.finish()),
            Arc::new(self.original_catalog_names.finish()),
            Arc::new(self.original_schema_names.finish()),
            Arc::new(self.dropped_at.finish()),
            Arc::new(self.dropped_by.finish()),
            Arc::new(self.retention_expires_at.finish()),
            Arc::new(self.purge_statuses.finish()),
            Arc::new(self.restorables.finish()),
            Arc::new(self.restored_at.finish()),
            Arc::new(self.restored_by.finish()),
        ];
        RecordBatch::new(self.schema.clone(), columns).context(CreateRecordBatchSnafu)
    }
}
