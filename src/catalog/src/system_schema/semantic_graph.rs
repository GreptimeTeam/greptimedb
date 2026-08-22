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

//! The computed entity-graph tables `greptime_private.semantic_entities` and
//! `greptime_private.semantic_relationships`.
//!
//! They live in `greptime_private`, not `information_schema`: scanning them
//! triggers read-time derivation over telemetry tables (trace self-joins, ...),
//! which breaks the "cheap, metadata-only" expectation users have of
//! `information_schema`. `greptime_private` also hosts the physical
//! declared-edge table (`semantic_relationships_declared`), whose rows are
//! unioned into the computed `semantic_relationships`.
//!
//! These are thin forwarders: their rows are derived at read time by the injected
//! [`EntityGraphProvider`], which enumerates the `table_semantics` declarations,
//! builds typed DataFusion derivation plans, and executes them via the query
//! engine. When no provider is injected (e.g. before the engine is up, or on a
//! non-frontend node) they stream empty. The fixed schemas here must match the
//! columns the provider's plans project; JSON columns are `json` (JSONB), whose
//! Arrow storage type is `Binary` — the derived batches are rebuilt against the
//! declared Arrow schema (which carries the `json` extension metadata) in
//! [`SystemTable::to_stream`].

use std::sync::{Arc, LazyLock, Weak};

use common_catalog::consts::{
    CONFIDENCE_COLUMN, DEFAULT_PRIVATE_SCHEMA_NAME, DST_ID_COLUMN, DST_TYPE_COLUMN,
    DURATION_COUNT_COLUMN, DURATION_SUM_COLUMN, EDGE_ATTRIBUTES_COLUMN, ENTITY_DESCRIPTIVE_COLUMN,
    ENTITY_ID_ATTRS_COLUMN, ENTITY_ID_COLUMN, ENTITY_SCOPE_COLUMN, ENTITY_TYPE_COLUMN,
    ERROR_COUNT_COLUMN, FRESH_UNTIL_COLUMN, OBSERVED_AT_COLUMN, PROVENANCE_COLUMN, REL_TYPE_COLUMN,
    REQUEST_COUNT_COLUMN, SEMANTIC_ENTITIES_TABLE_ID,
    SEMANTIC_ENTITIES_TABLE_NAME as SEMANTIC_ENTITIES, SEMANTIC_RELATIONSHIPS_TABLE_ID,
    SEMANTIC_RELATIONSHIPS_TABLE_NAME as SEMANTIC_RELATIONSHIPS, SOURCE_TABLES_COLUMN,
    SRC_ID_COLUMN, SRC_TYPE_COLUMN, WINDOW_END_COLUMN, WINDOW_START_COLUMN,
};
use common_error::ext::BoxedError;
use common_recordbatch::adapter::AsyncRecordBatchStreamAdapter;
use common_recordbatch::{
    DfRecordBatch, EmptyRecordBatchStream, RecordBatch, RecordBatchStreamWrapper,
    SendableRecordBatchStream,
};
use datatypes::prelude::ConcreteDataType;
use datatypes::schema::{ColumnSchema, Schema, SchemaRef};
use futures::StreamExt;
use session::context::QueryContextRef;
use snafu::ResultExt;
use store_api::storage::{ScanRequest, TableId};
use table::TableRef;
use table::metadata::TableInfo;

use crate::CatalogManager;
use crate::error::{InternalSnafu, Result};
use crate::system_schema::{SystemSchemaProviderInner, SystemTable, SystemTableRef, utils};

pub type EntityGraphProviderRef = Arc<dyn EntityGraphProvider>;

/// Where a table's entity declaration came from.
pub enum DeclarationOrigin {
    /// A `greptime.semantic.entity.<type>.*` table option.
    Declared,
    /// The built-in derivation conventions shipped with the binary.
    Convention,
}

impl DeclarationOrigin {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Declared => "declared",
            Self::Convention => "convention",
        }
    }
}

/// One entity a table declares, as `information_schema.table_semantics`
/// reports it. A catalog-side projection of the derivation's own declaration
/// type, which lives above this crate.
pub struct TableEntityDeclaration {
    pub entity_type: String,
    pub origin: DeclarationOrigin,
    pub id_columns: Vec<String>,
    pub id_qualifier: Option<String>,
    /// Columns whose presence on a row withdraws this declaration for that row.
    /// Reported because the declaration otherwise reads as unconditional.
    pub suppressed_by_columns: Vec<String>,
    pub descriptive_columns: Vec<String>,
}

/// Produces the rows of the computed entity-graph tables at read time.
///
/// Implemented above the query engine (in the frontend) and injected into the
/// catalog manager *after* construction — the provider needs the engine, which
/// needs the catalog manager — so this late binding breaks the
/// `catalog -> query` dependency cycle.
///
/// `query_ctx` is the outer query's context, captured when the computed table
/// is resolved: the derivation must read only sources that context may read
/// and execute under it, inheriting the caller's permissions, cancellation and
/// deadline (the RFC's derivation contract). `None` (context-less internal
/// resolution) keeps the provider's default behaviour.
#[async_trait::async_trait]
pub trait EntityGraphProvider: Send + Sync {
    /// Produces the entity registry (`semantic_entities`) rows for `catalog`.
    /// `None` means no source table declared an entity.
    async fn scan_entities(
        &self,
        catalog: &str,
        request: ScanRequest,
        query_ctx: Option<QueryContextRef>,
    ) -> std::result::Result<Option<SendableRecordBatchStream>, BoxedError>;

    /// Produces the relationship set (`semantic_relationships`) rows for `catalog`.
    async fn scan_relationships(
        &self,
        catalog: &str,
        request: ScanRequest,
        query_ctx: Option<QueryContextRef>,
    ) -> std::result::Result<Option<SendableRecordBatchStream>, BoxedError>;

    /// The entities `table_info` contributes to the graph: its explicit
    /// declarations merged with the ones the conventions derive. This is the
    /// only way an operator can see the derived half, so it backs
    /// `information_schema.table_semantics`. Metadata-only by contract — it
    /// runs per table on that view's scan and must not touch the query engine.
    fn table_declarations(&self, table_info: &TableInfo) -> Vec<TableEntityDeclaration>;
}

/// Serves the computed graph tables under `greptime_private`, overlaid on the
/// schema's physical tables the same way the `numbers` table overlays `public`
/// (the system catalog is consulted before physical table resolution).
pub(crate) struct SemanticGraphTableProvider {
    catalog_name: String,
    catalog_manager: Weak<dyn CatalogManager>,
    /// The resolving query's context; captured per resolution (the provider is
    /// built on demand, never cached), so it cannot leak across sessions.
    query_ctx: Option<QueryContextRef>,
}

impl SemanticGraphTableProvider {
    pub(crate) fn new(
        catalog_name: String,
        catalog_manager: Weak<dyn CatalogManager>,
        query_ctx: Option<QueryContextRef>,
    ) -> Self {
        Self {
            catalog_name,
            catalog_manager,
            query_ctx,
        }
    }

    pub(crate) fn table_names() -> Vec<String> {
        vec![
            SEMANTIC_ENTITIES.to_string(),
            SEMANTIC_RELATIONSHIPS.to_string(),
        ]
    }

    pub(crate) fn table_exists(name: &str) -> bool {
        name == SEMANTIC_ENTITIES || name == SEMANTIC_RELATIONSHIPS
    }

    pub(crate) fn table(&self, name: &str) -> Option<TableRef> {
        self.build_table(name)
    }
}

impl SystemSchemaProviderInner for SemanticGraphTableProvider {
    fn catalog_name(&self) -> &str {
        &self.catalog_name
    }

    fn schema_name() -> &'static str {
        DEFAULT_PRIVATE_SCHEMA_NAME
    }

    fn system_table(&self, name: &str) -> Option<SystemTableRef> {
        let kind = match name {
            SEMANTIC_ENTITIES => GraphTableKind::Entities,
            SEMANTIC_RELATIONSHIPS => GraphTableKind::Relationships,
            _ => return None,
        };
        Some(Arc::new(SemanticGraphTable::new(
            kind,
            self.catalog_name.clone(),
            self.catalog_manager.clone(),
            self.query_ctx.clone(),
        )) as _)
    }
}

fn ts() -> ConcreteDataType {
    ConcreteDataType::timestamp_millisecond_datatype()
}

fn string() -> ConcreteDataType {
    ConcreteDataType::string_datatype()
}

fn json() -> ConcreteDataType {
    ConcreteDataType::json_datatype()
}

/// Schema of `semantic_entities` — the node set of the graph, one row per entity
/// observed in a time window. Must match the registry derivation projection.
///
/// Columns:
/// - `observed_at`   — TIME INDEX; the 60s time bucket the entity was observed in.
/// - `window_start`  — start of that observation window.
/// - `window_end`    — end of the window (`window_start` + 60s).
/// - `fresh_until`   — time up to which the entity is considered present; equals
///   `window_end` for derived rows (the graph is a sliding window, not a
///   current-state table: an entity exists in a query window only if it has
///   observed evidence there).
/// - `entity_type`   — the entity's type, e.g. `service`, `host`, `k8s.pod`,
///   `process`, `service.instance` (the OTel-style, possibly dotted, type).
/// - `entity_id`     — canonical identifier: the value verbatim for a
///   single-attribute identity, or a sorted `k=v,k=v` rendering for a composite.
/// - `entity_id_attrs` — JSON object of the identifying attributes (the
///   escaping-safe source of truth for composite ids); NULL for single-attribute ids.
/// - `scope`         — namespace/environment the id is scoped to; empty when none.
/// - `descriptive`   — JSON snapshot of the entity's descriptive (non-identifying)
///   attributes; NULL when no descriptive columns were declared.
/// - `source_tables` — JSON array of the telemetry tables that contributed this entity.
static ENTITIES_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        ColumnSchema::new(OBSERVED_AT_COLUMN, ts(), false).with_time_index(true),
        ColumnSchema::new(WINDOW_START_COLUMN, ts(), true),
        ColumnSchema::new(WINDOW_END_COLUMN, ts(), true),
        ColumnSchema::new(FRESH_UNTIL_COLUMN, ts(), true),
        ColumnSchema::new(ENTITY_TYPE_COLUMN, string(), false),
        ColumnSchema::new(ENTITY_ID_COLUMN, string(), false),
        ColumnSchema::new(ENTITY_ID_ATTRS_COLUMN, json(), true),
        ColumnSchema::new(ENTITY_SCOPE_COLUMN, string(), true),
        ColumnSchema::new(ENTITY_DESCRIPTIVE_COLUMN, json(), true),
        ColumnSchema::new(SOURCE_TABLES_COLUMN, json(), true),
    ]))
});

/// Schema of `semantic_relationships` — the edge set of the graph, one row per
/// edge observed in a time window. This is the 16-column contract every derived
/// branch and the declared-edge table must project for the top-level `UNION ALL`.
///
/// Columns:
/// - `observed_at`   — TIME INDEX; the 60s time bucket the edge was observed in.
/// - `window_start` / `window_end` — the observation window (`window_start` + 60s).
/// - `fresh_until`   — time up to which the edge is considered live; equals
///   `window_end` for derived edges (from `valid_until` for declared edges).
/// - `src_type` / `src_id` — type and canonical id of the source endpoint.
/// - `dst_type` / `dst_id` — type and canonical id of the destination endpoint.
/// - `rel_type`      — relationship kind, e.g. `calls`, `runs_on`, `contains`,
///   `part_of`, `depends_on` (direction is src → dst; the inverse is a query concern).
/// - `provenance`    — how the edge was obtained: `trace` (derived from spans),
///   `attribute` (shared-identity join), `declared` (hand-inserted), or `agent`
///   (agent-inferred). Part of the edge identity, so edges of different provenance
///   for the same pair coexist.
/// - `confidence`    — derivation certainty in `[0, 1]`: `1.0` for paired or
///   declared edges, lower for virtual-node or agent-inferred edges. It does
///   not correct for trace sampling.
/// - `request_count` — RED: number of requests over the window (`calls` edges).
/// - `error_count`   — RED: number of errored requests over the window.
/// - `duration_sum`  — RED: sum of request durations, in seconds, over the window.
/// - `duration_count`— RED: number of durations summed (pair with `duration_sum`
///   to get an average).
/// - `attributes`    — JSON of edge attributes, e.g. `connection_type`,
///   `db.system`, `peer.service`.
static RELATIONSHIPS_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        ColumnSchema::new(OBSERVED_AT_COLUMN, ts(), false).with_time_index(true),
        ColumnSchema::new(WINDOW_START_COLUMN, ts(), true),
        ColumnSchema::new(WINDOW_END_COLUMN, ts(), true),
        ColumnSchema::new(FRESH_UNTIL_COLUMN, ts(), true),
        ColumnSchema::new(SRC_TYPE_COLUMN, string(), false),
        ColumnSchema::new(SRC_ID_COLUMN, string(), false),
        ColumnSchema::new(DST_TYPE_COLUMN, string(), false),
        ColumnSchema::new(DST_ID_COLUMN, string(), false),
        ColumnSchema::new(REL_TYPE_COLUMN, string(), false),
        ColumnSchema::new(PROVENANCE_COLUMN, string(), false),
        ColumnSchema::new(
            CONFIDENCE_COLUMN,
            ConcreteDataType::float64_datatype(),
            true,
        ),
        ColumnSchema::new(
            REQUEST_COUNT_COLUMN,
            ConcreteDataType::int64_datatype(),
            true,
        ),
        ColumnSchema::new(ERROR_COUNT_COLUMN, ConcreteDataType::int64_datatype(), true),
        ColumnSchema::new(
            DURATION_SUM_COLUMN,
            ConcreteDataType::float64_datatype(),
            true,
        ),
        ColumnSchema::new(
            DURATION_COUNT_COLUMN,
            ConcreteDataType::int64_datatype(),
            true,
        ),
        ColumnSchema::new(EDGE_ATTRIBUTES_COLUMN, json(), true),
    ]))
});

/// Which computed table this shell represents, so the two share one forwarder.
#[derive(Clone, Copy)]
enum GraphTableKind {
    Entities,
    Relationships,
}

/// Forwarder for a computed entity-graph table.
struct SemanticGraphTable {
    kind: GraphTableKind,
    schema: SchemaRef,
    catalog_name: String,
    catalog_manager: Weak<dyn CatalogManager>,
    query_ctx: Option<QueryContextRef>,
}

impl SemanticGraphTable {
    fn new(
        kind: GraphTableKind,
        catalog_name: String,
        catalog_manager: Weak<dyn CatalogManager>,
        query_ctx: Option<QueryContextRef>,
    ) -> Self {
        let schema = match kind {
            GraphTableKind::Entities => ENTITIES_SCHEMA.clone(),
            GraphTableKind::Relationships => RELATIONSHIPS_SCHEMA.clone(),
        };
        Self {
            kind,
            schema,
            catalog_name,
            catalog_manager,
            query_ctx,
        }
    }

    async fn derive(
        kind: GraphTableKind,
        catalog: String,
        catalog_manager: Weak<dyn CatalogManager>,
        request: ScanRequest,
        query_ctx: Option<QueryContextRef>,
    ) -> Result<Option<SendableRecordBatchStream>> {
        let provider = utils::entity_graph_provider(&catalog_manager)?;
        // No provider (engine not up / non-frontend node): stream empty.
        let Some(provider) = provider else {
            return Ok(None);
        };
        match kind {
            GraphTableKind::Entities => provider.scan_entities(&catalog, request, query_ctx).await,
            GraphTableKind::Relationships => {
                provider
                    .scan_relationships(&catalog, request, query_ctx)
                    .await
            }
        }
        .context(InternalSnafu)
    }

    fn align_schema(
        stream: SendableRecordBatchStream,
        schema: SchemaRef,
    ) -> SendableRecordBatchStream {
        let batch_schema = schema.clone();
        let arrow_schema = schema.arrow_schema().clone();
        let batches = stream.map(move |batch| {
            let batch = batch?;
            // The derivation output is structurally identical, but its JSON
            // fields are plain Binary without the declared extension metadata.
            let batch = DfRecordBatch::try_new(
                arrow_schema.clone(),
                batch.into_df_record_batch().columns().to_vec(),
            )
            .context(common_recordbatch::error::NewDfRecordBatchSnafu)?;
            Ok(RecordBatch::from_df_record_batch(
                batch_schema.clone(),
                batch,
            ))
        });
        Box::pin(RecordBatchStreamWrapper::new(schema, Box::pin(batches)))
    }
}

impl SystemTable for SemanticGraphTable {
    fn table_id(&self) -> TableId {
        match self.kind {
            GraphTableKind::Entities => SEMANTIC_ENTITIES_TABLE_ID,
            GraphTableKind::Relationships => SEMANTIC_RELATIONSHIPS_TABLE_ID,
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
        let schema = self.schema.clone();
        let kind = self.kind;
        let catalog = self.catalog_name.clone();
        let catalog_manager = self.catalog_manager.clone();
        let query_ctx = self.query_ctx.clone();

        let stream_schema = schema.clone();
        let stream = async move {
            let stream = Self::derive(kind, catalog, catalog_manager, request, query_ctx)
                .await
                .map_err(BoxedError::new)
                .context(common_recordbatch::error::ExternalSnafu)?;
            Ok(match stream {
                Some(stream) => Self::align_schema(stream, stream_schema.clone()),
                None => Box::pin(EmptyRecordBatchStream::new(stream_schema.clone())),
            })
        };

        Ok(Box::pin(AsyncRecordBatchStreamAdapter::new(
            schema,
            Box::pin(stream),
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn graph_tables_use_observed_at_as_time_index() {
        for schema in [&*ENTITIES_SCHEMA, &*RELATIONSHIPS_SCHEMA] {
            assert_eq!(
                schema.timestamp_column().map(|column| column.name.as_str()),
                Some("observed_at")
            );
        }
    }

    #[test]
    fn relationship_schema_does_not_expose_generation_id() {
        assert!(
            RELATIONSHIPS_SCHEMA
                .column_schema_by_name("generation_id")
                .is_none()
        );
    }
}
