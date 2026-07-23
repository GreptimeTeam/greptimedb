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
//! derivation plans as typed DataFusion `Expr`s over the declaring tables'
//! DataFrames (`operator::statement::semantic_graph`), and executes them through
//! the query engine. Injected into the catalog manager after the engine is built,
//! breaking the `catalog -> query` cycle.

use std::collections::HashMap;
use std::sync::Weak;

use async_trait::async_trait;
use catalog::CatalogManager;
use catalog::system_schema::semantic_graph::EntityGraphProvider;
use common_catalog::consts::{
    DEFAULT_PRIVATE_SCHEMA_NAME, DEFAULT_SCHEMA_NAME, INFORMATION_SCHEMA_NAME, PG_CATALOG_NAME,
    SERVICE_NAME_COLUMN,
};
use common_error::ext::BoxedError;
use common_query::OutputData;
use common_recordbatch::SendableRecordBatchStream;
use common_telemetry::warn;
use datafusion::dataframe::DataFrame;
use datafusion_expr::LogicalPlan;
use futures::TryStreamExt;
use operator::statement::semantic_graph::{
    EntityDeclaration, GraphWindow, build_calls_plan, build_registry_plan,
};
use query::QueryEngineRef;
use session::context::{QueryContextBuilder, QueryContextRef};
use snafu::ResultExt;
use store_api::storage::ScanRequest;
use table::TableRef;
use table::metadata::TableInfo;
use table::requests::{
    EntityRole, is_trace_v1_table, parse_entity_columns, parse_entity_option_key,
};

use crate::error;

/// The live [`EntityGraphProvider`], backed by the query engine.
pub struct EntityGraphProviderImpl {
    query_engine: QueryEngineRef,
    catalog_manager: Weak<dyn CatalogManager>,
}

struct EntitySource {
    declarations: Vec<EntityDeclaration>,
    table: TableRef,
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
    fn parse_declarations(table_info: &TableInfo) -> Vec<EntityDeclaration> {
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
            let Some((entity_type, role)) = parse_entity_option_key(key) else {
                continue;
            };
            let cols = parse_entity_columns(value);
            let entry = by_type.entry(entity_type.to_string()).or_default();
            match role {
                EntityRole::Id => entry.0 = cols,
                EntityRole::Descriptive => entry.1 = cols,
                EntityRole::Scope => entry.2 = cols,
            }
        }

        by_type
            .into_iter()
            .filter(|(_, (id, _, _))| !id.is_empty())
            .filter_map(
                |(entity_type, (id_columns, descriptive_columns, scope_columns))| {
                    // A stale declaration (e.g. its column was dropped later)
                    // must not poison every graph scan; skip it.
                    let schema = &table_info.meta.schema;
                    if let Some(missing) = id_columns
                        .iter()
                        .chain(&descriptive_columns)
                        .chain(&scope_columns)
                        .find(|c| schema.column_schema_by_name(c).is_none())
                    {
                        warn!(
                            "Skipping entity declaration `{}` of table `{}`: column `{}` not found",
                            entity_type, table_info.name, missing
                        );
                        return None;
                    }
                    Some(EntityDeclaration {
                        schema: table_info.schema_name.clone(),
                        table: table_info.name.clone(),
                        time_index: time_index.clone(),
                        entity_type,
                        id_columns,
                        descriptive_columns,
                        scope_columns,
                    })
                },
            )
            .collect()
    }

    /// All entity declarations of one table. Trace-v1 tables created before the
    /// ingest-side auto-stamp carry no `entity.service.id` option; synthesize it
    /// (their schema is fixed), so their `calls` edges have endpoint entities.
    /// A table with an explicit service declaration never gets the synthesized
    /// one — not even when the explicit declaration is invalid and skipped:
    /// silently falling back would change entity identity behind the user's back.
    fn declarations_for(table_info: &TableInfo) -> Vec<EntityDeclaration> {
        let mut declarations = Self::parse_declarations(table_info);
        let has_explicit_service = table_info.meta.options.extra_options.keys().any(|key| {
            parse_entity_option_key(key)
                .is_some_and(|(ty, role)| ty == "service" && role == EntityRole::Id)
        });
        if is_trace_v1_table(table_info)
            && !has_explicit_service
            && table_info
                .meta
                .schema
                .column_schema_by_name(SERVICE_NAME_COLUMN)
                .is_some()
            && let Some(time_index) = table_info
                .meta
                .schema
                .timestamp_column()
                .map(|c| c.name.clone())
        {
            declarations.push(EntityDeclaration {
                schema: table_info.schema_name.clone(),
                table: table_info.name.clone(),
                time_index,
                entity_type: "service".to_string(),
                id_columns: vec![SERVICE_NAME_COLUMN.to_string()],
                descriptive_columns: vec![],
                scope_columns: vec![],
            });
        }
        declarations
    }

    /// Enumerates entity declarations and trace tables across a catalog. Trace
    /// tables are keyed off the engine-native `table_data_model` option (same
    /// check as the Jaeger query path) so pre-existing trace tables without the
    /// newer `greptime.semantic.*` stamps are recognized too.
    async fn enumerate(
        &self,
        catalog: &str,
    ) -> Result<(Vec<EntitySource>, Vec<(EntityDeclaration, TableRef)>), BoxedError> {
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
                let table_declarations = Self::declarations_for(&table_info);
                if is_trace_v1_table(&table_info) {
                    // TODO(entity-graph): validate the complete fixed trace-v1
                    // schema before adding the table to calls derivation, and
                    // skip malformed/stale tables instead of letting one poison
                    // the whole relationship scan.
                    match table_declarations
                        .iter()
                        .find(|d| d.entity_type == "service")
                    {
                        Some(service) => traces.push((service.clone(), table.clone())),
                        // No usable service identity: the table cannot
                        // contribute calls edges (see declarations_for).
                        None => warn!(
                            "Trace table `{}` has no usable service declaration; skipping calls derivation",
                            table_info.name
                        ),
                    }
                }
                if !table_declarations.is_empty() {
                    declarations.push(EntitySource {
                        declarations: table_declarations,
                        table,
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

    fn read_table(&self, table: TableRef) -> Result<DataFrame, BoxedError> {
        self.query_engine.read_table(table).map_err(BoxedError::new)
    }

    /// Executes a derivation plan and returns its live result stream.
    ///
    /// TODO(entity-graph): the QueryContext must come from the outer query
    /// instead of being built here, so the derivation inherits the caller's
    /// permissions, cancellation and deadline. Requires threading the context
    /// through the computed-table scan path (planned next PR).
    async fn execute_plan(
        &self,
        catalog: &str,
        plan: LogicalPlan,
    ) -> Result<Option<SendableRecordBatchStream>, BoxedError> {
        let query_ctx: QueryContextRef = QueryContextBuilder::default()
            .current_catalog(catalog.to_string())
            .current_schema(DEFAULT_SCHEMA_NAME.to_string())
            .build()
            .into();
        let output = self
            .query_engine
            .execute(plan, query_ctx)
            .await
            .map_err(BoxedError::new)?;
        let stream = match output.data {
            OutputData::Stream(stream) => stream,
            OutputData::RecordBatches(batches) => batches.as_stream(),
            OutputData::AffectedRows(_) => return Ok(None),
        };
        Ok(Some(stream))
    }
}

#[async_trait]
impl EntityGraphProvider for EntityGraphProviderImpl {
    async fn scan_entities(
        &self,
        catalog: &str,
        request: ScanRequest,
    ) -> Result<Option<SendableRecordBatchStream>, BoxedError> {
        let (sources, _) = self.enumerate(catalog).await?;
        let mut plans = Vec::with_capacity(sources.len());
        for source in sources {
            plans.push((source.declarations, self.read_table(source.table)?));
        }
        let window = Self::query_window(&request);
        let Some(plan) = build_registry_plan(plans, &window)
            .context(error::DataFusionSnafu)
            .map_err(BoxedError::new)?
        else {
            return Ok(None);
        };
        self.execute_plan(catalog, plan).await
    }

    async fn scan_relationships(
        &self,
        catalog: &str,
        request: ScanRequest,
    ) -> Result<Option<SendableRecordBatchStream>, BoxedError> {
        let (_, traces) = self.enumerate(catalog).await?;
        let mut scans = Vec::with_capacity(traces.len());
        for (service, trace) in traces {
            scans.push((service, self.read_table(trace)?));
        }
        let window = Self::query_window(&request);
        let Some(plan) = build_calls_plan(scans, &window)
            .context(error::DataFusionSnafu)
            .map_err(BoxedError::new)?
        else {
            return Ok(None);
        };
        self.execute_plan(catalog, plan).await
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use common_catalog::consts::{DEFAULT_CATALOG_NAME, MITO_ENGINE};
    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::{ColumnSchema, SchemaBuilder};
    use table::metadata::{TableInfoBuilder, TableMeta, TableType};
    use table::requests::{TABLE_DATA_MODEL, TABLE_DATA_MODEL_TRACE_V1, TableOptions};

    use super::*;

    fn table_info(columns: &[&str], extra: &[(&str, &str)]) -> TableInfo {
        let mut column_schemas = vec![
            ColumnSchema::new(
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            )
            .with_time_index(true),
        ];
        column_schemas.extend(
            columns
                .iter()
                .map(|c| ColumnSchema::new(*c, ConcreteDataType::string_datatype(), true)),
        );
        let schema = Arc::new(
            SchemaBuilder::try_from_columns(column_schemas)
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
            .name("t1")
            .catalog_name(DEFAULT_CATALOG_NAME)
            .schema_name(DEFAULT_SCHEMA_NAME)
            .table_version(0)
            .table_type(TableType::Base)
            .meta(meta)
            .build()
            .unwrap()
    }

    #[test]
    fn declaration_referencing_missing_column_is_skipped() {
        let info = table_info(
            &["service_name"],
            &[
                ("greptime.semantic.entity.service.id", "service_name"),
                ("greptime.semantic.entity.host.id", "gone"),
            ],
        );
        let declarations = EntityGraphProviderImpl::declarations_for(&info);
        assert_eq!(declarations.len(), 1);
        assert_eq!(declarations[0].entity_type, "service");

        let info = table_info(
            &["service_name"],
            &[
                ("greptime.semantic.entity.service.id", "service_name"),
                ("greptime.semantic.entity.service.descriptive", "gone"),
            ],
        );
        assert!(EntityGraphProviderImpl::declarations_for(&info).is_empty());
    }

    #[test]
    fn trace_v1_table_gets_implicit_service_declaration() {
        let info = table_info(
            &["service_name"],
            &[(TABLE_DATA_MODEL, TABLE_DATA_MODEL_TRACE_V1)],
        );
        let declarations = EntityGraphProviderImpl::declarations_for(&info);
        assert_eq!(declarations.len(), 1);
        let decl = &declarations[0];
        assert_eq!(decl.entity_type, "service");
        assert_eq!(decl.id_columns, vec![SERVICE_NAME_COLUMN.to_string()]);
        assert_eq!(decl.time_index, "ts");

        // An explicit service declaration wins; no duplicate is synthesized.
        let info = table_info(
            &["service_name"],
            &[
                (TABLE_DATA_MODEL, TABLE_DATA_MODEL_TRACE_V1),
                ("greptime.semantic.entity.service.id", "service_name"),
            ],
        );
        assert_eq!(EntityGraphProviderImpl::declarations_for(&info).len(), 1);

        // A trace-model table without the fixed column synthesizes nothing.
        let info = table_info(&[], &[(TABLE_DATA_MODEL, TABLE_DATA_MODEL_TRACE_V1)]);
        assert!(EntityGraphProviderImpl::declarations_for(&info).is_empty());

        // An explicit but invalid declaration must not silently fall back to
        // the synthesized one: identity must not change behind the user's back.
        let info = table_info(
            &["service_name"],
            &[
                (TABLE_DATA_MODEL, TABLE_DATA_MODEL_TRACE_V1),
                ("greptime.semantic.entity.service.id", "gone"),
            ],
        );
        assert!(EntityGraphProviderImpl::declarations_for(&info).is_empty());
    }
}
