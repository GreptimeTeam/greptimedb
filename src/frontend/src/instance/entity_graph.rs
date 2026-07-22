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

//! Frontend implementation of the [`EntityGraphProvider`]: the live connector that
//! makes the computed `greptime_private.semantic_entities` /
//! `semantic_relationships` tables produce rows.
//!
//! It enumerates the entity-identity declarations by iterating the catalog's
//! `TableInfo` options (`greptime.semantic.entity.*`), builds the read-time
//! derivation SQL (`operator::statement::semantic_graph`), and executes it through
//! the query engine. Injected into the catalog manager after the engine is built,
//! breaking the `catalog -> query` cycle.

use std::collections::HashMap;
use std::sync::Weak;

use async_trait::async_trait;
use catalog::CatalogManager;
use catalog::system_schema::semantic_graph::EntityGraphProvider;
use common_catalog::consts::{
    DEFAULT_PRIVATE_SCHEMA_NAME, DEFAULT_SCHEMA_NAME, INFORMATION_SCHEMA_NAME, PG_CATALOG_NAME,
};
use common_error::ext::BoxedError;
use common_query::OutputData;
use common_recordbatch::{RecordBatch, util as record_util};
use futures::TryStreamExt;
use operator::statement::semantic_graph::{
    EntityDeclaration, GraphWindow, TraceSource, build_calls_sql, build_registry_sql,
};
use query::QueryEngineRef;
use query::parser::QueryLanguageParser;
use session::context::{QueryContextBuilder, QueryContextRef};
use store_api::storage::ScanRequest;
use table::metadata::TableInfo;
use table::requests::{SEMANTIC_ENTITY_PREFIX, TABLE_DATA_MODEL, TABLE_DATA_MODEL_TRACE_V1};

/// The live [`EntityGraphProvider`], backed by the query engine.
pub struct EntityGraphProviderImpl {
    query_engine: QueryEngineRef,
    catalog_manager: Weak<dyn CatalogManager>,
}

impl EntityGraphProviderImpl {
    pub fn new(query_engine: QueryEngineRef, catalog_manager: Weak<dyn CatalogManager>) -> Self {
        Self {
            query_engine,
            catalog_manager,
        }
    }

    /// Parses `greptime.semantic.entity.<type>.{id|descriptive|scope}` options of
    /// one table into per-type declarations. A type with no `id` columns is skipped.
    fn parse_declarations(
        table_info: &TableInfo,
        catalog: &str,
        schema: &str,
    ) -> Vec<EntityDeclaration> {
        let Some(time_index) = table_info
            .meta
            .schema
            .timestamp_column()
            .map(|c| c.name.clone())
        else {
            return vec![];
        };

        // entity_type -> (id_columns, descriptive_columns, scope_columns)
        type RoleColumns = (Vec<String>, Vec<String>, Vec<String>);
        let mut by_type: HashMap<String, RoleColumns> = HashMap::new();
        for (key, value) in &table_info.meta.options.extra_options {
            let Some(rest) = key.strip_prefix(SEMANTIC_ENTITY_PREFIX) else {
                continue;
            };
            let Some((entity_type, role)) = rest.rsplit_once('.') else {
                continue;
            };
            let cols: Vec<String> = value
                .split(',')
                .map(|c| c.trim().to_string())
                .filter(|c| !c.is_empty())
                .collect();
            let entry = by_type.entry(entity_type.to_string()).or_default();
            match role {
                "id" => entry.0 = cols,
                "descriptive" => entry.1 = cols,
                "scope" => entry.2 = cols,
                _ => {}
            }
        }

        by_type
            .into_iter()
            .filter(|(_, (id, _, _))| !id.is_empty())
            .map(
                |(entity_type, (id_columns, descriptive_columns, scope_columns))| {
                    EntityDeclaration {
                        catalog: catalog.to_string(),
                        schema: schema.to_string(),
                        table: table_info.name.clone(),
                        time_index: time_index.clone(),
                        entity_type,
                        id_columns,
                        descriptive_columns,
                        scope_columns,
                    }
                },
            )
            .collect()
    }

    /// Keyed off the engine-native `table_data_model` option (same check as the
    /// Jaeger query path) so pre-existing trace tables without the newer
    /// `greptime.semantic.*` stamps are recognized too. The v1 model is required
    /// because the derivation SQL relies on its fixed column names.
    fn is_trace_table(table_info: &TableInfo) -> bool {
        table_info
            .meta
            .options
            .extra_options
            .get(TABLE_DATA_MODEL)
            .map(|v| v == TABLE_DATA_MODEL_TRACE_V1)
            .unwrap_or(false)
    }

    /// Enumerates entity declarations and trace tables across a catalog.
    async fn enumerate(
        &self,
        catalog: &str,
    ) -> Result<(Vec<EntityDeclaration>, Vec<TraceSource>), BoxedError> {
        let Some(catalog_manager) = self.catalog_manager.upgrade() else {
            return Ok((vec![], vec![]));
        };

        let mut declarations = vec![];
        let mut traces = vec![];
        let schemas = catalog_manager
            .schema_names(catalog, None)
            .await
            .map_err(BoxedError::new)?;
        for schema in schemas {
            // User telemetry never lives in the system schemas; skip them to avoid
            // scanning information_schema (including the computed graph tables) etc.
            if schema == INFORMATION_SCHEMA_NAME
                || schema == PG_CATALOG_NAME
                || schema == DEFAULT_PRIVATE_SCHEMA_NAME
            {
                continue;
            }
            let mut tables = catalog_manager.tables(catalog, &schema, None);
            while let Some(table) = tables.try_next().await.map_err(BoxedError::new)? {
                let table_info = table.table_info();
                declarations.extend(Self::parse_declarations(&table_info, catalog, &schema));
                if Self::is_trace_table(&table_info) {
                    traces.push(TraceSource {
                        catalog: catalog.to_string(),
                        schema: schema.clone(),
                        table: table_info.name.clone(),
                    });
                }
            }
        }
        Ok((declarations, traces))
    }

    /// The time window to derive over, taken from the scan's time predicate.
    ///
    /// TODO(entity-graph): read the `observed_at` bounds out of `request.filters`
    /// and build the window from them. Until then this always returns the default
    /// (last hour), so a query whose time filter falls entirely outside the last
    /// hour derives nothing and returns empty — a known limitation tracked for the
    /// scan-time-predicate-pushdown follow-up.
    fn query_window(_request: &ScanRequest) -> GraphWindow {
        GraphWindow::default_last_hour()
    }

    /// Plans and executes a derivation SQL statement, collecting its rows.
    ///
    /// TODO(entity-graph): the QueryContext must come from the outer query
    /// instead of being built here, so the derivation inherits the caller's
    /// permissions, cancellation and deadline. Requires threading the context
    /// through the computed-table scan path (planned next PR).
    async fn run_sql(&self, catalog: &str, sql: &str) -> Result<Vec<RecordBatch>, BoxedError> {
        let query_ctx: QueryContextRef = QueryContextBuilder::default()
            .current_catalog(catalog.to_string())
            .current_schema(DEFAULT_SCHEMA_NAME.to_string())
            .build()
            .into();
        let stmt = QueryLanguageParser::parse_sql(sql, &query_ctx).map_err(BoxedError::new)?;
        let plan = self
            .query_engine
            .planner()
            .plan(&stmt, query_ctx.clone())
            .await
            .map_err(BoxedError::new)?;
        let output = self
            .query_engine
            .execute(plan, query_ctx)
            .await
            .map_err(BoxedError::new)?;
        let stream = match output.data {
            OutputData::Stream(stream) => stream,
            OutputData::RecordBatches(batches) => batches.as_stream(),
            OutputData::AffectedRows(_) => return Ok(vec![]),
        };
        record_util::collect(stream).await.map_err(BoxedError::new)
    }
}

#[async_trait]
impl EntityGraphProvider for EntityGraphProviderImpl {
    async fn scan_entities(
        &self,
        catalog: &str,
        request: ScanRequest,
    ) -> Result<Vec<RecordBatch>, BoxedError> {
        let (declarations, _) = self.enumerate(catalog).await?;
        let window = Self::query_window(&request);
        let Some(sql) = build_registry_sql(&declarations, &window) else {
            return Ok(vec![]);
        };
        self.run_sql(catalog, &sql).await
    }

    async fn scan_relationships(
        &self,
        catalog: &str,
        request: ScanRequest,
    ) -> Result<Vec<RecordBatch>, BoxedError> {
        let (_, traces) = self.enumerate(catalog).await?;
        let window = Self::query_window(&request);
        let mut batches = vec![];
        for trace in traces {
            let sql = build_calls_sql(&trace, &window);
            batches.extend(self.run_sql(catalog, &sql).await?);
        }
        Ok(batches)
    }
}
