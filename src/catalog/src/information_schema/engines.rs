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

use arrow_schema::SchemaRef as ArrowSchemaRef;
use common_catalog::consts::INFORMATION_SCHEMA_ENGINES_TABLE_ID;
use common_error::ext::BoxedError;
use common_query::physical_plan::TaskContext;
use common_recordbatch::adapter::RecordBatchStreamAdapter;
use common_recordbatch::{RecordBatch, SendableRecordBatchStream};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter as DfRecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::PartitionStream as DfPartitionStream;
use datafusion::physical_plan::SendableRecordBatchStream as DfSendableRecordBatchStream;
use datatypes::prelude::{ConcreteDataType, VectorRef};
use datatypes::schema::{ColumnSchema, Schema, SchemaRef};
use datatypes::vectors::StringVector;
use snafu::ResultExt;
use store_api::storage::TableId;

use super::ENGINES;
use crate::error::{CreateRecordBatchSnafu, InternalSnafu, Result};
use crate::information_schema::InformationTable;

pub(super) struct InformationSchemaEngines {
    schema: SchemaRef,
}

impl InformationSchemaEngines {
    pub(super) fn new() -> Self {
        Self {
            schema: Self::schema(),
        }
    }

    pub(crate) fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            // The name of the storage engine.
            ColumnSchema::new("engine", ConcreteDataType::string_datatype(), false),
            // The level of support that the server has on the storage engine
            ColumnSchema::new("support", ConcreteDataType::string_datatype(), false),
            // The brief comment on the storage engine
            ColumnSchema::new("comment", ConcreteDataType::string_datatype(), false),
            // Whether the storage engine supports transactions.
            ColumnSchema::new("transactions", ConcreteDataType::string_datatype(), false),
            // Whether the storage engine supports XA transactions.
            ColumnSchema::new("xa", ConcreteDataType::string_datatype(), true),
            // Whether the storage engine supports `savepoints`.
            ColumnSchema::new("savepoints", ConcreteDataType::string_datatype(), true),
        ]))
    }

    fn builder(&self) -> InformationSchemaEnginesBuilder {
        InformationSchemaEnginesBuilder::new(self.schema.clone())
    }
}

impl InformationTable for InformationSchemaEngines {
    fn table_id(&self) -> TableId {
        INFORMATION_SCHEMA_ENGINES_TABLE_ID
    }

    fn table_name(&self) -> &'static str {
        ENGINES
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn to_stream(&self) -> Result<SendableRecordBatchStream> {
        let schema = self.schema.arrow_schema().clone();
        let mut builder = self.builder();
        let stream = Box::pin(DfRecordBatchStreamAdapter::new(
            schema,
            futures::stream::once(async move {
                builder
                    .make_engines()
                    .await
                    .map(|x| x.into_df_record_batch())
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

struct InformationSchemaEnginesBuilder {
    schema: SchemaRef,
}

impl InformationSchemaEnginesBuilder {
    fn new(schema: SchemaRef) -> Self {
        Self { schema }
    }

    /// Construct the `information_schema.engines` virtual table
    async fn make_engines(&mut self) -> Result<RecordBatch> {
        let columns: Vec<VectorRef> = vec![
            Arc::new(StringVector::from(vec!["Mito"])),
            Arc::new(StringVector::from(vec!["DEFAULT"])),
            Arc::new(StringVector::from(vec![
                "Storage engine for time-series data",
            ])),
            Arc::new(StringVector::from(vec!["NO"])),
            Arc::new(StringVector::from(vec!["NO"])),
            Arc::new(StringVector::from(vec!["NO"])),
        ];
        RecordBatch::new(self.schema.clone(), columns).context(CreateRecordBatchSnafu)
    }
}

impl DfPartitionStream for InformationSchemaEngines {
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
                    .make_engines()
                    .await
                    .map(|x| x.into_df_record_batch())
                    .map_err(Into::into)
            }),
        ))
    }
}
