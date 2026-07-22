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
//! `information_schema`. `greptime_private` already signals "system-managed,
//! computed data objects" and also hosts the physical declared-edge table
//! (`semantic_relationships_declared`), so derived and declared edges share one
//! schema.
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

use std::sync::{Arc, Weak};

use common_catalog::consts::{
    DEFAULT_PRIVATE_SCHEMA_NAME, SEMANTIC_ENTITIES_TABLE_ID, SEMANTIC_RELATIONSHIPS_TABLE_ID,
};
pub use common_catalog::consts::{
    SEMANTIC_ENTITIES_TABLE_NAME as SEMANTIC_ENTITIES,
    SEMANTIC_RELATIONSHIPS_TABLE_NAME as SEMANTIC_RELATIONSHIPS,
};
use common_error::ext::BoxedError;
use common_recordbatch::adapter::RecordBatchStreamAdapter;
use common_recordbatch::{DfRecordBatch, RecordBatch, SendableRecordBatchStream};
use datafusion::error::DataFusionError;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter as DfRecordBatchStreamAdapter;
use datatypes::prelude::ConcreteDataType;
use datatypes::schema::{ColumnSchema, Schema, SchemaRef};
use futures::TryStreamExt;
use snafu::ResultExt;
use store_api::storage::{ScanRequest, TableId};
use table::TableRef;

use crate::CatalogManager;
use crate::error::{InternalSnafu, Result};
use crate::system_schema::{SystemSchemaProviderInner, SystemTable, SystemTableRef, utils};

pub type EntityGraphProviderRef = Arc<dyn EntityGraphProvider>;

/// Produces the rows of the computed entity-graph tables
/// (`semantic_entities` / `semantic_relationships`) at read time.
///
/// The computed tables are thin forwarders to this provider, which is
/// implemented above the query engine (in the frontend): it enumerates the entity
/// declarations from `table_semantics`, builds the typed derivation plans, and
/// executes them. It is injected into the catalog manager *after* construction —
/// the provider needs the engine, which needs the catalog manager — so this late
/// binding breaks the `catalog -> query` dependency cycle. Keeping derivation out
/// of `catalog` also respects the crate layering (the plan builders live in
/// `operator`). See `docs/rfcs/2026-06-25-entity-relationships-and-graph-query.md`.
#[async_trait::async_trait]
pub trait EntityGraphProvider: Send + Sync {
    /// Produces the entity registry (`semantic_entities`) rows for `catalog`. The
    /// graph is small relative to raw telemetry, so the provider collects the
    /// derivation into batches (rather than a live stream), keeping the forwarding
    /// table trivial.
    async fn scan_entities(
        &self,
        catalog: &str,
        request: ScanRequest,
    ) -> std::result::Result<Vec<RecordBatch>, BoxedError>;

    /// Produces the relationship set (`semantic_relationships`) rows for `catalog`.
    async fn scan_relationships(
        &self,
        catalog: &str,
        request: ScanRequest,
    ) -> std::result::Result<Vec<RecordBatch>, BoxedError>;
}

/// Serves the computed graph tables under `greptime_private`, overlaid on the
/// schema's physical tables the same way the `numbers` table overlays `public`
/// (the system catalog is consulted before physical table resolution).
pub struct SemanticGraphTableProvider {
    catalog_name: String,
    catalog_manager: Weak<dyn CatalogManager>,
}

impl SemanticGraphTableProvider {
    pub fn new(catalog_name: String, catalog_manager: Weak<dyn CatalogManager>) -> Self {
        Self {
            catalog_name,
            catalog_manager,
        }
    }

    pub fn table_names() -> Vec<String> {
        vec![
            SEMANTIC_ENTITIES.to_string(),
            SEMANTIC_RELATIONSHIPS.to_string(),
        ]
    }

    pub fn table_exists(name: &str) -> bool {
        name == SEMANTIC_ENTITIES || name == SEMANTIC_RELATIONSHIPS
    }

    pub fn table(&self, name: &str) -> Option<TableRef> {
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
        match name {
            SEMANTIC_ENTITIES => Some(Arc::new(SemanticGraphTable::entities(
                self.catalog_name.clone(),
                self.catalog_manager.clone(),
            )) as _),
            SEMANTIC_RELATIONSHIPS => Some(Arc::new(SemanticGraphTable::relationships(
                self.catalog_name.clone(),
                self.catalog_manager.clone(),
            )) as _),
            _ => None,
        }
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
fn entities_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        // 60s time bucket the entity was observed in (TIME INDEX).
        ColumnSchema::new("observed_at", ts(), false),
        // Start of the observation window this row covers.
        ColumnSchema::new("window_start", ts(), true),
        // End of the observation window (window_start + 60s).
        ColumnSchema::new("window_end", ts(), true),
        // Time up to which the entity is considered present (= window_end for
        // derived rows). The graph is a temporal window, not a current-state table.
        ColumnSchema::new("fresh_until", ts(), true),
        // Entity type, e.g. `service` / `host` / `k8s.pod` / `process`.
        ColumnSchema::new("entity_type", string(), false),
        // Canonical id: value verbatim (single attr) or sorted `k=v,k=v` (composite).
        ColumnSchema::new("entity_id", string(), false),
        // JSON object of identifying attributes (source of truth for composite ids);
        // NULL for single-attribute ids.
        ColumnSchema::new("entity_id_attrs", json(), true),
        // Namespace/environment the id is scoped to; empty string when none.
        ColumnSchema::new("scope", string(), true),
        // JSON snapshot of descriptive (non-identifying) attributes; NULL if none.
        ColumnSchema::new("descriptive", json(), true),
        // JSON array of the telemetry tables that contributed this entity.
        ColumnSchema::new("source_tables", json(), true),
    ]))
}

/// Schema of `semantic_relationships` — the edge set of the graph, one row per
/// edge observed in a time window. This is the 18-column contract every derived
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
/// - `confidence`    — confidence in `[0, 1]`: `1.0` for observed/declared edges,
///   lower for sampled traces, virtual nodes, or agent-inferred edges.
/// - `scope`         — namespace/environment; empty string when none.
/// - `generation_id` — deterministic key of the producing (window, run) that makes
///   re-derivation idempotent; empty for read-time derived rows (load-bearing only
///   for a future maintained/materialised graph).
/// - `request_count` — RED: number of requests over the window (`calls` edges).
/// - `error_count`   — RED: number of errored requests over the window.
/// - `duration_sum`  — RED: sum of request durations, in seconds, over the window.
/// - `duration_count`— RED: number of durations summed (pair with `duration_sum`
///   to get an average).
/// - `attributes`    — JSON of edge attributes, e.g. `connection_type`,
///   `db.system`, `peer.service`.
fn relationships_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        // 60s time bucket the edge was observed in (TIME INDEX).
        ColumnSchema::new("observed_at", ts(), false),
        // Start of the observation window this edge covers.
        ColumnSchema::new("window_start", ts(), true),
        // End of the observation window (window_start + 60s).
        ColumnSchema::new("window_end", ts(), true),
        // Time up to which the edge is considered live (= window_end for derived
        // edges, from valid_until for declared edges).
        ColumnSchema::new("fresh_until", ts(), true),
        // Type and canonical id of the source endpoint.
        ColumnSchema::new("src_type", string(), false),
        ColumnSchema::new("src_id", string(), false),
        // Type and canonical id of the destination endpoint.
        ColumnSchema::new("dst_type", string(), false),
        ColumnSchema::new("dst_id", string(), false),
        // Relationship kind (src -> dst), e.g. `calls` / `runs_on` / `depends_on`.
        ColumnSchema::new("rel_type", string(), false),
        // Origin of the edge: `trace` | `attribute` | `declared` | `agent`
        // (part of the edge identity).
        ColumnSchema::new("provenance", string(), false),
        // Confidence in [0, 1]; 1.0 for observed/declared, lower for sampled /
        // virtual-node / inferred edges.
        ColumnSchema::new("confidence", ConcreteDataType::float64_datatype(), true),
        // Namespace/environment; empty string when none.
        ColumnSchema::new("scope", string(), true),
        // Deterministic (window, run) key for idempotent re-derivation; empty for
        // read-time derived rows.
        ColumnSchema::new("generation_id", string(), true),
        // RED: request count over the window.
        ColumnSchema::new("request_count", ConcreteDataType::int64_datatype(), true),
        // RED: errored-request count over the window.
        ColumnSchema::new("error_count", ConcreteDataType::int64_datatype(), true),
        // RED: sum of request durations (seconds) over the window.
        ColumnSchema::new("duration_sum", ConcreteDataType::float64_datatype(), true),
        // RED: number of durations summed (pair with duration_sum for an average).
        ColumnSchema::new("duration_count", ConcreteDataType::int64_datatype(), true),
        // JSON of edge attributes: connection_type, db.system, peer.service, ...
        ColumnSchema::new("attributes", json(), true),
    ]))
}

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
}

impl SemanticGraphTable {
    fn entities(catalog_name: String, catalog_manager: Weak<dyn CatalogManager>) -> Self {
        Self {
            kind: GraphTableKind::Entities,
            schema: entities_schema(),
            catalog_name,
            catalog_manager,
        }
    }

    fn relationships(catalog_name: String, catalog_manager: Weak<dyn CatalogManager>) -> Self {
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
        let arrow_schema = self.schema.arrow_schema().clone();
        let kind = self.kind;
        let catalog = self.catalog_name.clone();
        let catalog_manager = self.catalog_manager.clone();

        let batch_schema = arrow_schema.clone();
        let batches = futures::stream::once(async move {
            Self::derive(kind, catalog, catalog_manager, request)
                .await
                .map(move |batches| {
                    futures::stream::iter(batches.into_iter().map(move |rb| {
                        // Rebuild against the declared Arrow schema: the derived
                        // batches are structurally identical, but their fields
                        // lack the `json` extension metadata (JSON columns are
                        // plain `Binary` in the derivation output).
                        DfRecordBatch::try_new(
                            batch_schema.clone(),
                            rb.into_df_record_batch().columns().to_vec(),
                        )
                        .map_err(DataFusionError::from)
                    }))
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
