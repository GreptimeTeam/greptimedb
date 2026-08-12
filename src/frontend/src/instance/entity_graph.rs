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
use auth::{
    PermissionChecker, PermissionCheckerRef, PermissionReq, PermissionTableTarget,
    PermissionTableTargets, SEMANTIC_GRAPH_QUERY,
};
use catalog::CatalogManager;
use catalog::system_schema::semantic_graph::EntityGraphProvider;
use common_catalog::consts::{
    DEFAULT_PRIVATE_SCHEMA_NAME, DEFAULT_SCHEMA_NAME, INFORMATION_SCHEMA_NAME, PG_CATALOG_NAME,
    SEMANTIC_RELATIONSHIPS_DECLARED_TABLE_NAME, SERVICE_NAME_COLUMN,
};
use common_error::ext::{BoxedError, ErrorExt};
use common_error::status_code::StatusCode;
use common_query::OutputData;
use common_recordbatch::SendableRecordBatchStream;
use common_telemetry::{debug, warn};
use common_time::timestamp::TimeUnit;
use datafusion::dataframe::DataFrame;
use datafusion_expr::LogicalPlan;
use futures::TryStreamExt;
use operator::statement::semantic_graph::{
    CallsSource, CoDeclaredSource, DeclaredSource, EntityDeclaration, GraphQueryWindow,
    OBSERVED_AT_COLUMN, RegistrySource, RelationshipSources, build_registry_plan,
    build_relationships_plan, declared_relationships_schema_matches,
};
use query::QueryEngineRef;
use session::context::{QueryContext, QueryContextBuilder, QueryContextRef};
use snafu::ResultExt;
use store_api::storage::ScanRequest;
use table::TableRef;
use table::metadata::TableInfo;
use table::predicate::{TimeRangeExtraction, extract_time_range_strict};
use table::requests::{
    EntityRole, is_trace_v1_table, parse_entity_columns, parse_entity_option_key,
};

use crate::error;

/// The live [`EntityGraphProvider`], backed by the query engine.
pub struct EntityGraphProviderImpl {
    query_engine: QueryEngineRef,
    catalog_manager: Weak<dyn CatalogManager>,
    permission_checker: Option<PermissionCheckerRef>,
}

struct EntitySource {
    declarations: Vec<EntityDeclaration>,
    is_trace: bool,
    table: TableRef,
}

struct TraceSource {
    service: Option<EntityDeclaration>,
    agent: Option<EntityDeclaration>,
    table: TableRef,
}

impl EntityGraphProviderImpl {
    pub fn new(
        query_engine: QueryEngineRef,
        catalog_manager: Weak<dyn CatalogManager>,
        permission_checker: Option<PermissionCheckerRef>,
    ) -> Self {
        Self {
            query_engine,
            catalog_manager,
            permission_checker,
        }
    }

    /// Whether the caller may read the derivation sources named by `targets`.
    /// `Ok(false)` = denied: the source is silently excluded, per the
    /// derivation contract. Errors other than a denial abort the scan.
    fn authorize_sources(
        &self,
        query_ctx: Option<&QueryContext>,
        targets: PermissionTableTargets,
    ) -> Result<bool, BoxedError> {
        let Some(ctx) = query_ctx else {
            return Ok(true);
        };
        match self
            .permission_checker
            .as_ref()
            .check_permission_with_table_targets(
                ctx.current_user(),
                PermissionReq::Action(SEMANTIC_GRAPH_QUERY),
                targets,
            ) {
            Ok(_) => Ok(true),
            Err(err) if err.status_code() == StatusCode::PermissionDenied => Ok(false),
            Err(err) => Err(BoxedError::new(err)),
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
        query_ctx: Option<&QueryContext>,
    ) -> Result<(Vec<EntitySource>, Vec<TraceSource>), BoxedError> {
        let Some(catalog_manager) = self.catalog_manager.upgrade() else {
            return Ok((vec![], vec![]));
        };

        // A target-blind checker (e.g. the default mode-based one) answers the
        // same for every table: ask once up front instead of per table.
        let per_table_auth = match self.permission_checker.as_ref() {
            Some(checker) if query_ctx.is_some() => {
                if checker.uses_table_targets() {
                    true
                } else if self
                    .authorize_sources(query_ctx, PermissionTableTargets::resolved(vec![]))?
                {
                    false
                } else {
                    debug!(
                        "Caller lacks the entity-graph read permission; deriving an empty graph \
                         (catalog: {catalog})"
                    );
                    return Ok((vec![], vec![]));
                }
            }
            _ => false,
        };

        let mut declarations = vec![];
        let mut traces = vec![];
        let schemas = catalog_manager
            .schema_names(catalog, query_ctx)
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
            let mut tables = catalog_manager.tables(catalog, &schema, query_ctx);
            while let Some(table) = tables.try_next().await.map_err(BoxedError::new)? {
                let table_info = table.table_info();
                let table_declarations = Self::declarations_for(&table_info);
                let is_trace = is_trace_v1_table(&table_info);
                // Authorize only tables that would contribute rows.
                if per_table_auth
                    && (is_trace || !table_declarations.is_empty())
                    && !self.authorize_sources(
                        query_ctx,
                        PermissionTableTargets::resolved(vec![PermissionTableTarget::new(
                            catalog,
                            &schema,
                            &table_info.name,
                        )]),
                    )?
                {
                    debug!(
                        "Excluding `{schema}.{}` from the entity-graph derivation: caller lacks \
                         read permission",
                        table_info.name
                    );
                    continue;
                }
                if is_trace {
                    let find = |entity_type: &str| {
                        table_declarations
                            .iter()
                            .find(|d| d.entity_type == entity_type)
                            .cloned()
                    };
                    let service = find("service");
                    let agent = find("agent");
                    if service.is_none() {
                        // No usable service identity: the table cannot
                        // contribute service-calls edges (see declarations_for).
                        warn!(
                            "Trace table `{}` has no usable service declaration; skipping calls derivation",
                            table_info.name
                        );
                    }
                    if service.is_some() || agent.is_some() {
                        traces.push(TraceSource {
                            service,
                            agent,
                            table: table.clone(),
                        });
                    }
                }
                if !table_declarations.is_empty() {
                    declarations.push(EntitySource {
                        declarations: table_declarations,
                        is_trace,
                        table,
                    });
                }
            }
        }
        Ok((declarations, traces))
    }

    /// The time window to derive over, taken from the scan's `observed_at`
    /// predicate.
    ///
    /// The contract (RFC "The contract"): no `observed_at` predicate → the
    /// product default (last hour); a missing upper bound means "up to now"; a
    /// predicate without a lower bound or in a shape that cannot be safely
    /// extracted is an error asking for an explicit range — never a silent
    /// fallback into incomplete results.
    /// `Ok(None)` = the window cannot match anything (e.g. a lower bound in
    /// the future with the implicit "up to now" upper bound): the scan streams
    /// empty instead of deriving.
    fn query_window(request: &ScanRequest) -> Result<Option<GraphQueryWindow>, BoxedError> {
        let invalid =
            |err_msg: String| Err(BoxedError::new(error::InvalidSqlSnafu { err_msg }.build()));
        match extract_time_range_strict(OBSERVED_AT_COLUMN, TimeUnit::Millisecond, &request.filters)
        {
            TimeRangeExtraction::Absent => Ok(Some(GraphQueryWindow::default_last_hour())),
            TimeRangeExtraction::Extracted(range) => {
                let Some(start) = range.start() else {
                    return invalid(format!(
                        "the {OBSERVED_AT_COLUMN} filter has no lower bound; the graph cannot \
                         derive over unbounded history — add e.g. {OBSERVED_AT_COLUMN} >= \
                         '2026-01-01 00:00:00'"
                    ));
                };
                let end_ms = range
                    .end()
                    .map(|ts| ts.value())
                    .unwrap_or_else(common_time::util::current_time_millis);
                if start.value() >= end_ms {
                    return Ok(None);
                }
                Ok(Some(GraphQueryWindow::from_observed(start.value(), end_ms)))
            }
            TimeRangeExtraction::Unsupported => invalid(format!(
                "cannot derive the graph window from the {OBSERVED_AT_COLUMN} filter; use plain \
                 range predicates (>=, <, BETWEEN) with literal bounds"
            )),
        }
    }

    fn read_table(&self, table: TableRef) -> Result<DataFrame, BoxedError> {
        self.query_engine.read_table(table).map_err(BoxedError::new)
    }

    /// The declared-edge branch source, when the physical table exists, the
    /// caller may read it, and it still matches the canonical definition. A
    /// mismatch (upgrade skew) is an explicit error, not a silent skip:
    /// dropping declared edges would misrepresent the graph.
    async fn declared_source(
        &self,
        catalog: &str,
        query_ctx: Option<&QueryContext>,
    ) -> Result<Option<DeclaredSource>, BoxedError> {
        let Some(catalog_manager) = self.catalog_manager.upgrade() else {
            return Ok(None);
        };
        let Some(table) = catalog_manager
            .table(
                catalog,
                DEFAULT_PRIVATE_SCHEMA_NAME,
                SEMANTIC_RELATIONSHIPS_DECLARED_TABLE_NAME,
                query_ctx,
            )
            .await
            .map_err(BoxedError::new)?
        else {
            return Ok(None);
        };
        if !self.authorize_sources(
            query_ctx,
            PermissionTableTargets::resolved(vec![PermissionTableTarget::new(
                catalog,
                DEFAULT_PRIVATE_SCHEMA_NAME,
                SEMANTIC_RELATIONSHIPS_DECLARED_TABLE_NAME,
            )]),
        )? {
            debug!(
                "Excluding declared edges: caller lacks read permission on \
                 `{DEFAULT_PRIVATE_SCHEMA_NAME}.{SEMANTIC_RELATIONSHIPS_DECLARED_TABLE_NAME}`"
            );
            return Ok(None);
        }
        let table_info = table.table_info();
        if !declared_relationships_schema_matches(&table_info) {
            return Err(BoxedError::new(
                error::InvalidSqlSnafu {
                    err_msg: format!(
                        "{DEFAULT_PRIVATE_SCHEMA_NAME}.{SEMANTIC_RELATIONSHIPS_DECLARED_TABLE_NAME} \
                         does not match its canonical schema; declared edges cannot be derived"
                    ),
                }
                .build(),
            ));
        }
        Ok(Some(DeclaredSource {
            scan: self.read_table(table)?,
        }))
    }

    /// Executes a derivation plan under the caller's context (inheriting its
    /// permissions, cancellation and deadline); context-less internal scans
    /// get a minimal default.
    async fn execute_plan(
        &self,
        catalog: &str,
        plan: LogicalPlan,
        query_ctx: Option<QueryContextRef>,
    ) -> Result<Option<SendableRecordBatchStream>, BoxedError> {
        let query_ctx = query_ctx.unwrap_or_else(|| {
            QueryContextBuilder::default()
                .current_catalog(catalog.to_string())
                .current_schema(DEFAULT_SCHEMA_NAME.to_string())
                .build()
                .into()
        });
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
        query_ctx: Option<QueryContextRef>,
    ) -> Result<Option<SendableRecordBatchStream>, BoxedError> {
        let (sources, _) = self.enumerate(catalog, query_ctx.as_deref()).await?;
        let mut plans = Vec::with_capacity(sources.len());
        for source in sources {
            plans.push(RegistrySource {
                declarations: source.declarations,
                scan: self.read_table(source.table)?,
            });
        }
        let Some(window) = Self::query_window(&request)? else {
            return Ok(None);
        };
        let Some(plan) = build_registry_plan(plans, &window)
            .context(error::DataFusionSnafu)
            .map_err(BoxedError::new)?
        else {
            return Ok(None);
        };
        self.execute_plan(catalog, plan, query_ctx).await
    }

    async fn scan_relationships(
        &self,
        catalog: &str,
        request: ScanRequest,
        query_ctx: Option<QueryContextRef>,
    ) -> Result<Option<SendableRecordBatchStream>, BoxedError> {
        let (sources, traces) = self.enumerate(catalog, query_ctx.as_deref()).await?;
        let mut calls = Vec::with_capacity(traces.len());
        for trace in traces {
            calls.push(CallsSource {
                service: trace.service,
                agent: trace.agent,
                scan: self.read_table(trace.table)?,
            });
        }
        let mut co_declared = Vec::with_capacity(sources.len());
        for source in sources {
            co_declared.push(CoDeclaredSource {
                declarations: source.declarations,
                is_trace: source.is_trace,
                scan: self.read_table(source.table)?,
            });
        }
        let declared = self.declared_source(catalog, query_ctx.as_deref()).await?;
        let Some(window) = Self::query_window(&request)? else {
            return Ok(None);
        };
        let Some(plan) = build_relationships_plan(
            RelationshipSources {
                traces: calls,
                co_declared,
                declared,
            },
            &window,
        )
        .context(error::DataFusionSnafu)
        .map_err(BoxedError::new)?
        else {
            return Ok(None);
        };
        self.execute_plan(catalog, plan, query_ctx).await
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
        assemble_table_info(column_schemas, extra)
    }

    fn assemble_table_info(column_schemas: Vec<ColumnSchema>, extra: &[(&str, &str)]) -> TableInfo {
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
    fn future_lower_only_window_matches_nothing() {
        let future_ms = common_time::util::current_time_millis() + 86_400_000;
        let request = ScanRequest {
            filters: vec![
                datafusion_expr::col(OBSERVED_AT_COLUMN).gt_eq(datafusion_expr::lit(
                    datafusion::common::ScalarValue::TimestampMillisecond(Some(future_ms), None),
                )),
            ],
            ..Default::default()
        };
        assert!(
            EntityGraphProviderImpl::query_window(&request)
                .unwrap()
                .is_none()
        );

        assert!(
            EntityGraphProviderImpl::query_window(&ScanRequest::default())
                .unwrap()
                .is_some()
        );
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
