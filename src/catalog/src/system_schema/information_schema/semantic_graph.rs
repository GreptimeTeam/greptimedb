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

//! The computed entity-graph tables `information_schema.semantic_entities` and
//! `information_schema.semantic_relationships`.
//!
//! These are thin forwarders: their rows are derived at read time by the injected
//! [`EntityGraphProvider`], which enumerates the `table_semantics` declarations,
//! builds the derivation SQL, and executes it via the query engine. When no
//! provider is injected (e.g. before the engine is up, or on a non-frontend node)
//! they stream empty. The fixed schemas here must match the columns the provider's
//! SQL projects (the provider casts JSON/attribute columns to STRING so the output
//! schema is deterministic).

use std::sync::{Arc, Weak};

use common_catalog::consts::{
    INFORMATION_SCHEMA_SEMANTIC_ENTITIES_TABLE_ID,
    INFORMATION_SCHEMA_SEMANTIC_RELATIONSHIPS_TABLE_ID,
};
use common_error::ext::BoxedError;
use common_recordbatch::adapter::RecordBatchStreamAdapter;
use common_recordbatch::{RecordBatch, SendableRecordBatchStream};
use datafusion::error::DataFusionError;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter as DfRecordBatchStreamAdapter;
use datatypes::prelude::ConcreteDataType;
use datatypes::schema::{ColumnSchema, Schema, SchemaRef};
use futures::TryStreamExt;
use snafu::ResultExt;
use store_api::storage::{ScanRequest, TableId};

use super::{SEMANTIC_ENTITIES, SEMANTIC_RELATIONSHIPS};
use crate::CatalogManager;
use crate::error::{InternalSnafu, Result};
use crate::system_schema::information_schema::InformationTable;
use crate::system_schema::utils;

fn ts() -> ConcreteDataType {
    ConcreteDataType::timestamp_millisecond_datatype()
}

fn string() -> ConcreteDataType {
    ConcreteDataType::string_datatype()
}

/// Schema of `semantic_entities` (matches the registry derivation projection).
fn entities_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        ColumnSchema::new("observed_at", ts(), false),
        ColumnSchema::new("window_start", ts(), true),
        ColumnSchema::new("window_end", ts(), true),
        ColumnSchema::new("fresh_until", ts(), true),
        ColumnSchema::new("entity_type", string(), false),
        ColumnSchema::new("entity_id", string(), false),
        ColumnSchema::new("entity_id_attrs", string(), true),
        ColumnSchema::new("scope", string(), true),
        ColumnSchema::new("descriptive", string(), true),
        ColumnSchema::new("source_tables", string(), true),
    ]))
}

/// Schema of `semantic_relationships` (the 18-column UNION contract).
fn relationships_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        ColumnSchema::new("observed_at", ts(), false),
        ColumnSchema::new("window_start", ts(), true),
        ColumnSchema::new("window_end", ts(), true),
        ColumnSchema::new("fresh_until", ts(), true),
        ColumnSchema::new("src_type", string(), false),
        ColumnSchema::new("src_id", string(), false),
        ColumnSchema::new("dst_type", string(), false),
        ColumnSchema::new("dst_id", string(), false),
        ColumnSchema::new("rel_type", string(), false),
        ColumnSchema::new("provenance", string(), false),
        ColumnSchema::new("confidence", ConcreteDataType::float64_datatype(), true),
        ColumnSchema::new("scope", string(), true),
        ColumnSchema::new("generation_id", string(), true),
        ColumnSchema::new("request_count", ConcreteDataType::int64_datatype(), true),
        ColumnSchema::new("error_count", ConcreteDataType::int64_datatype(), true),
        ColumnSchema::new("duration_sum", ConcreteDataType::float64_datatype(), true),
        ColumnSchema::new("duration_count", ConcreteDataType::int64_datatype(), true),
        ColumnSchema::new("attributes", string(), true),
    ]))
}

/// Which computed table this shell represents, so the two share one forwarder.
#[derive(Clone, Copy)]
enum GraphTableKind {
    Entities,
    Relationships,
}

/// Forwarder for a computed entity-graph table.
pub(super) struct InformationSchemaSemanticGraph {
    kind: GraphTableKind,
    schema: SchemaRef,
    catalog_name: String,
    catalog_manager: Weak<dyn CatalogManager>,
}

impl InformationSchemaSemanticGraph {
    pub(super) fn entities(
        catalog_name: String,
        catalog_manager: Weak<dyn CatalogManager>,
    ) -> Self {
        Self {
            kind: GraphTableKind::Entities,
            schema: entities_schema(),
            catalog_name,
            catalog_manager,
        }
    }

    pub(super) fn relationships(
        catalog_name: String,
        catalog_manager: Weak<dyn CatalogManager>,
    ) -> Self {
        Self {
            kind: GraphTableKind::Relationships,
            schema: relationships_schema(),
            catalog_name,
            catalog_manager,
        }
    }

    async fn derive(
        kind: GraphTableKind,
        catalog: String,
        catalog_manager: Weak<dyn CatalogManager>,
        request: ScanRequest,
    ) -> Result<Vec<RecordBatch>> {
        let provider = utils::entity_graph_provider(&catalog_manager)?;
        // No provider (engine not up / non-frontend node): stream empty.
        let Some(provider) = provider else {
            return Ok(vec![]);
        };
        match kind {
            GraphTableKind::Entities => provider.scan_entities(&catalog, request).await,
            GraphTableKind::Relationships => provider.scan_relationships(&catalog, request).await,
        }
        .context(InternalSnafu)
    }
}

impl InformationTable for InformationSchemaSemanticGraph {
    fn table_id(&self) -> TableId {
        match self.kind {
            GraphTableKind::Entities => INFORMATION_SCHEMA_SEMANTIC_ENTITIES_TABLE_ID,
            GraphTableKind::Relationships => INFORMATION_SCHEMA_SEMANTIC_RELATIONSHIPS_TABLE_ID,
        }
    }

    fn table_name(&self) -> &'static str {
        match self.kind {
            GraphTableKind::Entities => SEMANTIC_ENTITIES,
            GraphTableKind::Relationships => SEMANTIC_RELATIONSHIPS,
        }
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn to_stream(&self, request: ScanRequest) -> Result<SendableRecordBatchStream> {
        let arrow_schema = self.schema.arrow_schema().clone();
        let kind = self.kind;
        let catalog = self.catalog_name.clone();
        let catalog_manager = self.catalog_manager.clone();

        let batches = futures::stream::once(async move {
            Self::derive(kind, catalog, catalog_manager, request)
                .await
                .map(|batches| {
                    futures::stream::iter(
                        batches.into_iter().map(|rb| Ok(rb.into_df_record_batch())),
                    )
                })
                .map_err(|err| DataFusionError::External(Box::new(err)))
        })
        .try_flatten();

        let stream = Box::pin(DfRecordBatchStreamAdapter::new(arrow_schema, batches));
        Ok(Box::pin(
            RecordBatchStreamAdapter::try_new(stream)
                .map_err(BoxedError::new)
                .context(InternalSnafu)?,
        ))
    }
}
