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
        ColumnSchema::new("entity_id_attrs", string(), true),
        // Namespace/environment the id is scoped to; empty string when none.
        ColumnSchema::new("scope", string(), true),
        // JSON snapshot of descriptive (non-identifying) attributes; NULL if none.
        ColumnSchema::new("descriptive", string(), true),
        // JSON array of the telemetry tables that contributed this entity.
        ColumnSchema::new("source_tables", string(), true),
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
