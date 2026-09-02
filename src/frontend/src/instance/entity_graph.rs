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

use std::collections::{BTreeMap, HashMap};
use std::sync::Weak;

use async_trait::async_trait;
use auth::{
    PermissionChecker, PermissionCheckerRef, PermissionReq, PermissionTableTarget,
    PermissionTableTargets, SEMANTIC_GRAPH_QUERY,
};
use catalog::CatalogManager;
use catalog::system_schema::semantic_graph::{
    DeclarationOrigin, EntityGraphProvider, TableEntityDeclaration,
};
use common_catalog::consts::{
    DEFAULT_PRIVATE_SCHEMA_NAME, DEFAULT_SCHEMA_NAME, INFORMATION_SCHEMA_NAME, OBSERVED_AT_COLUMN,
    PG_CATALOG_NAME, SEMANTIC_RELATIONSHIPS_DECLARED_TABLE_NAME,
};
use common_error::ext::{BoxedError, ErrorExt};
use common_error::status_code::StatusCode;
use common_query::OutputData;
use common_query::prelude::greptime_temporality_label;
use common_recordbatch::SendableRecordBatchStream;
use common_telemetry::{debug, warn};
use common_time::timestamp::TimeUnit;
use datafusion::dataframe::DataFrame;
use datafusion_expr::LogicalPlan;
use futures::TryStreamExt;
use operator::statement::semantic_graph::{
    CallsSource, CoDeclaredSource, Conventions, DeclaredSource, ENTITY_TYPE_GEN_AI_AGENT,
    ENTITY_TYPE_SERVICE, EntityDeclaration, GraphQueryWindow, ImplicitEntity, RegistrySource,
    RelationshipSources, build_registry_plan, build_relationships_plan, conventions,
    declared_relationships_schema_matches,
};
use query::QueryEngineRef;
use session::context::{QueryContext, QueryContextBuilder, QueryContextRef};
use snafu::ResultExt;
use store_api::storage::ScanRequest;
use table::TableRef;
use table::metadata::TableInfo;
use table::predicate::{TimeRangeExtraction, extract_time_range_strict};
use table::requests::{
    EntityRole, SEMANTIC_METRIC_TYPE, SEMANTIC_SIGNAL_TYPE, SEMANTIC_SOURCE, SIGNAL_TYPE_METRIC,
    SOURCE_OPENTELEMETRY, SOURCE_PROMETHEUS, is_trace_v1_table, parse_entity_columns,
    parse_entity_option_key,
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
        if by_type.is_empty() {
            return vec![];
        }
        let Some(time_index) = table_info
            .meta
            .schema
            .timestamp_column()
            .map(|c| c.name.clone())
        else {
            return vec![];
        };

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
                        id_qualifier: None,
                        superseded_by_columns: vec![],
                        descriptive_columns,
                        scope_columns,
                    })
                },
            )
            .collect()
    }

    /// All entity declarations of one table: the explicit options plus the
    /// zero-configuration conventions (`otlp_trace_entities` for trace-v1
    /// tables — including the `service` identity of tables created before the
    /// ingest-side auto-stamp — and the Prometheus/OTel descriptor
    /// whitelists). An explicit declaration of a type always suppresses the
    /// implicit one, even when the explicit declaration is invalid and
    /// skipped: silently falling back would change entity identity behind the
    /// user's back.
    fn declarations_for(
        table_info: &TableInfo,
        conventions: &Conventions,
    ) -> Vec<EntityDeclaration> {
        let mut declarations = Self::parse_declarations(table_info);
        let mut supersessions = Vec::new();
        if is_trace_v1_table(table_info) {
            Self::extend_with_implicit_entities(
                table_info,
                &conventions.otlp_trace_entities,
                &mut declarations,
                &mut supersessions,
            );
        }
        Self::extend_with_info_metric_conventions(
            table_info,
            &conventions.prometheus_info_metrics,
            SOURCE_PROMETHEUS,
            None,
            &mut declarations,
            &mut supersessions,
        );
        Self::extend_with_info_metric_conventions(
            table_info,
            &conventions.otel_info_metrics,
            SOURCE_OPENTELEMETRY,
            Some(servers::semantic::METRIC_TYPE_INFO),
            &mut declarations,
            &mut supersessions,
        );
        Self::resolve_supersessions(&mut declarations, supersessions);
        declarations
    }

    /// Binds each `superseded_by` to the identity the superseding type has on
    /// this table, once every declaration is known. A type nothing declares
    /// here leaves the guard empty, so the superseded entity stands instead of
    /// yielding to a node that will never be derived.
    fn resolve_supersessions(
        declarations: &mut [EntityDeclaration],
        supersessions: Vec<(usize, String)>,
    ) {
        for (index, entity_type) in supersessions {
            let identity = declarations
                .iter()
                .find(|declaration| declaration.entity_type == entity_type)
                .map(|declaration| declaration.id_columns.clone())
                .unwrap_or_default();
            declarations[index].superseded_by_columns = identity;
        }
    }

    /// Whether the table carries an explicit `entity.<type>.id` option.
    fn explicitly_declares(table_info: &TableInfo, entity_type: &str) -> bool {
        table_info.meta.options.extra_options.keys().any(|key| {
            parse_entity_option_key(key)
                .is_some_and(|(ty, role)| ty == entity_type && role == EntityRole::Id)
        })
    }

    /// Implicit declarations of the well-known entity-descriptor metrics
    /// (the `prometheus_info_metrics` / `otel_info_metrics` whitelists of
    /// `conventions.yaml`), gated on the ingest-stamped `signal_type=metric`
    /// option plus the whitelist's expected `source`; OTel descriptors also
    /// require `metric.type=info`. The metric engine's physical table
    /// aggregates every logical table's columns and must not contribute a
    /// duplicate source.
    fn extend_with_info_metric_conventions(
        table_info: &TableInfo,
        whitelist: &BTreeMap<String, Vec<ImplicitEntity>>,
        expected_source: &str,
        expected_metric_type: Option<&str>,
        declarations: &mut Vec<EntityDeclaration>,
        supersessions: &mut Vec<(usize, String)>,
    ) {
        let Some(implicit_entities) = whitelist.get(&table_info.name) else {
            return;
        };
        let options = &table_info.meta.options.extra_options;
        if options.get(SEMANTIC_SIGNAL_TYPE).map(String::as_str) != Some(SIGNAL_TYPE_METRIC)
            || options.get(SEMANTIC_SOURCE).map(String::as_str) != Some(expected_source)
            || expected_metric_type.is_some_and(|expected| {
                options.get(SEMANTIC_METRIC_TYPE).map(String::as_str) != Some(expected)
            })
            || table_info.is_physical_table()
        {
            debug!(
                "Table `{}` matches the info-metric whitelist but is not an eligible \
                 `{expected_source}` info-metric source; skipping its implicit declarations",
                table_info.name
            );
            return;
        }
        Self::extend_with_implicit_entities(
            table_info,
            implicit_entities,
            declarations,
            supersessions,
        );
    }

    /// Synthesizes the applicable subset of `entities` on `table_info`:
    /// explicit declarations win, every id column must exist (no guessing),
    /// descriptive columns are filtered to those present.
    fn extend_with_implicit_entities(
        table_info: &TableInfo,
        entities: &[ImplicitEntity],
        declarations: &mut Vec<EntityDeclaration>,
        supersessions: &mut Vec<(usize, String)>,
    ) {
        let schema = &table_info.meta.schema;
        let Some(time_index) = schema.timestamp_column().map(|c| c.name.clone()) else {
            debug!(
                "Table `{}` has no time index; skipping its implicit declarations",
                table_info.name
            );
            return;
        };
        for implicit in entities {
            if Self::explicitly_declares(table_info, &implicit.entity) {
                debug!(
                    "Table `{}` explicitly declares `{}`; the implicit declaration is suppressed",
                    table_info.name, implicit.entity
                );
                continue;
            }
            if let Some(missing) = implicit
                .id
                .iter()
                .find(|c| schema.column_schema_by_name(c).is_none())
            {
                debug!(
                    "Table `{}` lacks the id column `{}`; skipping the implicit `{}` declaration",
                    table_info.name, missing, implicit.entity
                );
                continue;
            }
            let descriptive_columns = if implicit.descriptive_rest {
                table_info
                    .meta
                    .row_key_column_names()
                    .filter(|c| !implicit.id.contains(c))
                    .filter(|c| c.as_str() != greptime_temporality_label())
                    .cloned()
                    .collect()
            } else {
                implicit
                    .descriptive
                    .iter()
                    .filter(|c| schema.column_schema_by_name(c).is_some())
                    .cloned()
                    .collect()
            };
            // A table predating the qualifier column keeps the unqualified
            // identity rather than losing the declaration.
            let id_qualifier = implicit
                .qualified_by
                .clone()
                .filter(|c| schema.column_schema_by_name(c).is_some());
            if let Some(entity_type) = &implicit.superseded_by {
                supersessions.push((declarations.len(), entity_type.clone()));
            }
            declarations.push(EntityDeclaration {
                schema: table_info.schema_name.clone(),
                table: table_info.name.clone(),
                time_index: time_index.clone(),
                entity_type: implicit.entity.clone(),
                id_columns: implicit.id.clone(),
                id_qualifier,
                superseded_by_columns: vec![],
                descriptive_columns,
                scope_columns: vec![],
            });
        }
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
        let conventions = conventions()
            .map_err(datafusion::error::DataFusionError::Internal)
            .context(error::DataFusionSnafu)
            .map_err(BoxedError::new)?;

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
                let table_declarations = Self::declarations_for(&table_info, conventions);
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
                    let service = find(ENTITY_TYPE_SERVICE);
                    let agent = find(ENTITY_TYPE_GEN_AI_AGENT);
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

    fn table_declarations(&self, table_info: &TableInfo) -> Vec<TableEntityDeclaration> {
        // A broken embedded file still leaves the explicit half reportable;
        // the scan paths surface the error itself.
        let derived = match conventions() {
            Ok(conventions) => Self::declarations_for(table_info, conventions),
            Err(_) => Self::parse_declarations(table_info),
        };
        let mut declarations = derived
            .into_iter()
            .map(|declaration| TableEntityDeclaration {
                origin: if Self::explicitly_declares(table_info, &declaration.entity_type) {
                    DeclarationOrigin::Declared
                } else {
                    DeclarationOrigin::Convention
                },
                entity_type: declaration.entity_type,
                id_columns: declaration.id_columns,
                id_qualifier: declaration.id_qualifier,
                superseded_by_columns: declaration.superseded_by_columns,
                descriptive_columns: declaration.descriptive_columns,
                scope_columns: declaration.scope_columns,
            })
            .collect::<Vec<_>>();
        declarations.sort_by(|a, b| a.entity_type.cmp(&b.entity_type));
        declarations
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use common_catalog::consts::{DEFAULT_CATALOG_NAME, MITO_ENGINE};
    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::{ColumnSchema, SchemaBuilder};
    use store_api::metric_engine_consts::PHYSICAL_TABLE_METADATA_KEY;
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
        named_table_info("t1", column_schemas, vec![], extra)
    }

    fn named_table_info(
        name: &str,
        column_schemas: Vec<ColumnSchema>,
        primary_key_indices: Vec<usize>,
        extra: &[(&str, &str)],
    ) -> TableInfo {
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
            primary_key_indices,
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
            .name(name)
            .catalog_name(DEFAULT_CATALOG_NAME)
            .schema_name(DEFAULT_SCHEMA_NAME)
            .table_version(0)
            .table_type(TableType::Base)
            .meta(meta)
            .build()
            .unwrap()
    }

    /// A metric-engine-logical-table shape: every label column is a tag.
    fn prom_table_info(name: &str, tags: &[&str], extra: &[(&str, &str)]) -> TableInfo {
        let mut column_schemas = vec![
            ColumnSchema::new(
                "greptime_timestamp",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            )
            .with_time_index(true),
        ];
        column_schemas.extend(
            tags.iter()
                .map(|c| ColumnSchema::new(*c, ConcreteDataType::string_datatype(), true)),
        );
        let primary_key_indices = (1..=tags.len()).collect();
        named_table_info(name, column_schemas, primary_key_indices, extra)
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
        let declarations = EntityGraphProviderImpl::declarations_for(&info, conventions().unwrap());
        assert_eq!(declarations.len(), 1);
        assert_eq!(declarations[0].entity_type, "service");

        let info = table_info(
            &["service_name"],
            &[
                ("greptime.semantic.entity.service.id", "service_name"),
                ("greptime.semantic.entity.service.descriptive", "gone"),
            ],
        );
        assert!(
            EntityGraphProviderImpl::declarations_for(&info, conventions().unwrap()).is_empty()
        );
    }

    const PROM_STAMPS: &[(&str, &str)] = &[
        (SEMANTIC_SIGNAL_TYPE, SIGNAL_TYPE_METRIC),
        (SEMANTIC_SOURCE, SOURCE_PROMETHEUS),
    ];

    fn sorted_declarations(info: &TableInfo) -> Vec<EntityDeclaration> {
        let mut declarations =
            EntityGraphProviderImpl::declarations_for(info, conventions().unwrap());
        declarations.sort_by(|a, b| a.entity_type.cmp(&b.entity_type));
        declarations
    }

    #[test]
    fn prometheus_info_metric_gets_implicit_declarations() {
        let info = prom_table_info(
            "kube_pod_info",
            &["namespace", "pod", "uid", "node", "job", "instance"],
            PROM_STAMPS,
        );
        let declarations = sorted_declarations(&info);
        assert_eq!(declarations.len(), 2);
        assert_eq!(declarations[0].entity_type, "k8s.node");
        assert_eq!(declarations[0].id_columns, vec!["node"]);
        assert_eq!(declarations[1].entity_type, "k8s.pod");
        assert_eq!(declarations[1].id_columns, vec!["uid"]);
        // host_ip/pod_ip/created_by_* are absent from this table; descriptive
        // shrinks to the present columns.
        assert_eq!(
            declarations[1].descriptive_columns,
            vec!["namespace", "pod", "node"]
        );
        assert_eq!(declarations[1].time_index, "greptime_timestamp");
    }

    #[test]
    fn trace_table_gets_implicit_resource_entities() {
        let full = table_info(
            &[
                "service_name",
                "resource_attributes.service.instance.id",
                "resource_attributes.k8s.pod.uid",
                "resource_attributes.k8s.pod.name",
                "resource_attributes.k8s.node.name",
            ],
            &[(TABLE_DATA_MODEL, TABLE_DATA_MODEL_TRACE_V1)],
        );
        let declarations = sorted_declarations(&full);
        let types: Vec<&str> = declarations
            .iter()
            .map(|d| d.entity_type.as_str())
            .collect();
        assert_eq!(
            types,
            vec!["k8s.node", "k8s.pod", "service", "service.instance"]
        );
        assert_eq!(
            declarations[0].id_columns,
            vec!["resource_attributes.k8s.node.name"]
        );
        assert_eq!(
            declarations[1].id_columns,
            vec!["resource_attributes.k8s.pod.uid"]
        );
        assert_eq!(
            declarations[1].descriptive_columns,
            vec!["resource_attributes.k8s.pod.name"]
        );
        assert_eq!(
            declarations[3].id_columns,
            vec!["service_name", "resource_attributes.service.instance.id"]
        );
        assert_eq!(declarations[2].id_qualifier, None);

        let namespaced = table_info(
            &[
                "service_name",
                "resource_attributes.service.namespace",
                "resource_attributes.service.instance.id",
            ],
            &[(TABLE_DATA_MODEL, TABLE_DATA_MODEL_TRACE_V1)],
        );
        for declaration in sorted_declarations(&namespaced) {
            assert_eq!(
                declaration.id_qualifier.as_deref(),
                Some("resource_attributes.service.namespace"),
                "{} must qualify its identity like the metric side's job",
                declaration.entity_type
            );
        }

        // Missing uid column: no pod entity synthesized, no name-based guess.
        let no_uid = table_info(
            &["service_name", "resource_attributes.k8s.pod.name"],
            &[(TABLE_DATA_MODEL, TABLE_DATA_MODEL_TRACE_V1)],
        );
        let types: Vec<String> = sorted_declarations(&no_uid)
            .into_iter()
            .map(|d| d.entity_type)
            .collect();
        assert_eq!(types, vec!["service"]);

        // An explicit declaration suppresses the implicit one even when it is
        // invalid and skipped: identity must not change behind the user's back.
        let invalid_explicit = table_info(
            &["service_name"],
            &[
                (TABLE_DATA_MODEL, TABLE_DATA_MODEL_TRACE_V1),
                ("greptime.semantic.entity.service.id", "gone"),
            ],
        );
        assert!(sorted_declarations(&invalid_explicit).is_empty());
    }

    #[test]
    fn trace_table_host_and_container_require_stable_ids() {
        let with_ids = table_info(
            &[
                "service_name",
                "resource_attributes.host.id",
                "resource_attributes.host.name",
                "resource_attributes.container.id",
            ],
            &[(TABLE_DATA_MODEL, TABLE_DATA_MODEL_TRACE_V1)],
        );
        let declarations = sorted_declarations(&with_ids);
        let types: Vec<&str> = declarations
            .iter()
            .map(|d| d.entity_type.as_str())
            .collect();
        assert_eq!(types, vec!["container", "host", "service"]);
        assert_eq!(
            declarations[1].id_columns,
            vec!["resource_attributes.host.id"]
        );
        assert_eq!(
            declarations[1].descriptive_columns,
            vec!["resource_attributes.host.name"]
        );

        // A wrong `resource_attributes.` prefix would leave the generic
        // container standing beside the k8s one, which the descriptor-table
        // case cannot catch.
        let pod_container = table_info(
            &[
                "service_name",
                "resource_attributes.container.id",
                "resource_attributes.container.name",
                "resource_attributes.k8s.pod.uid",
                "resource_attributes.k8s.container.name",
            ],
            &[(TABLE_DATA_MODEL, TABLE_DATA_MODEL_TRACE_V1)],
        );
        let declarations = sorted_declarations(&pod_container);
        let types: Vec<&str> = declarations
            .iter()
            .map(|d| d.entity_type.as_str())
            .collect();
        assert_eq!(
            types,
            vec!["container", "k8s.container", "k8s.pod", "service"]
        );
        assert_eq!(
            declarations[0].superseded_by_columns,
            vec![
                "resource_attributes.k8s.pod.uid",
                "resource_attributes.k8s.container.name"
            ]
        );
        assert_eq!(
            declarations[1].descriptive_columns,
            vec![
                "resource_attributes.container.id",
                "resource_attributes.container.name"
            ]
        );

        let names_only = table_info(
            &[
                "service_name",
                "resource_attributes.host.name",
                "resource_attributes.container.name",
            ],
            &[(TABLE_DATA_MODEL, TABLE_DATA_MODEL_TRACE_V1)],
        );
        let types: Vec<String> = sorted_declarations(&names_only)
            .into_iter()
            .map(|d| d.entity_type)
            .collect();
        assert_eq!(types, vec!["service"]);
    }

    #[test]
    fn prometheus_implicit_declarations_are_gated() {
        let labels: &[&str] = &["namespace", "pod", "node"];
        // A non-Prometheus source.
        assert!(
            sorted_declarations(&prom_table_info(
                "kube_pod_info",
                labels,
                &[
                    (SEMANTIC_SIGNAL_TYPE, SIGNAL_TYPE_METRIC),
                    (SEMANTIC_SOURCE, "opentelemetry"),
                ],
            ))
            .is_empty()
        );
        // Not a whitelisted metric name.
        assert!(
            sorted_declarations(&prom_table_info("http_requests_total", labels, PROM_STAMPS))
                .is_empty()
        );
        let mut stamps = PROM_STAMPS.to_vec();
        stamps.push((PHYSICAL_TABLE_METADATA_KEY, "true"));
        assert!(sorted_declarations(&prom_table_info("kube_pod_info", labels, &stamps)).is_empty());

        // A missing id column (uid) drops that entity, not the whole table.
        let info = prom_table_info("kube_pod_info", &["namespace", "pod", "node"], PROM_STAMPS);
        let declarations = sorted_declarations(&info);
        assert_eq!(declarations.len(), 1);
        assert_eq!(declarations[0].entity_type, "k8s.node");
    }

    #[test]
    fn explicit_declaration_suppresses_the_implicit_one() {
        let mut stamps = PROM_STAMPS.to_vec();
        stamps.push(("greptime.semantic.entity.k8s.pod.id", "pod"));
        let info = prom_table_info("kube_pod_info", &["namespace", "pod", "node"], &stamps);
        let declarations = sorted_declarations(&info);
        assert_eq!(declarations.len(), 2);
        assert_eq!(declarations[0].entity_type, "k8s.node");
        assert_eq!(declarations[1].entity_type, "k8s.pod");
        // The explicit identity wins over the conventional [namespace, pod].
        assert_eq!(declarations[1].id_columns, vec!["pod"]);
    }

    const OTEL_STAMPS: &[(&str, &str)] = &[
        (SEMANTIC_SIGNAL_TYPE, SIGNAL_TYPE_METRIC),
        (SEMANTIC_SOURCE, SOURCE_OPENTELEMETRY),
        (SEMANTIC_METRIC_TYPE, servers::semantic::METRIC_TYPE_INFO),
    ];

    #[test]
    fn greptime_otel_resource_info_gets_implicit_declarations() {
        let info = prom_table_info(
            "greptime_otel_resource_info",
            &[
                "job",
                "instance",
                "service.name",
                "service.namespace",
                "host.id",
                "host.name",
                "container.id",
                "container.name",
                "k8s.pod.uid",
                "k8s.pod.name",
                "k8s.container.name",
                "k8s.namespace.name",
                "k8s.node.name",
            ],
            OTEL_STAMPS,
        );
        let declarations = sorted_declarations(&info);
        let types: Vec<&str> = declarations
            .iter()
            .map(|d| d.entity_type.as_str())
            .collect();
        assert_eq!(
            types,
            vec![
                "container",
                "host",
                "k8s.container",
                "k8s.node",
                "k8s.pod",
                "service",
                "service.instance"
            ]
        );
        // A pod's container is the k8s.container, under the identity
        // kube-state-metrics gives it; the generic type yields to it.
        assert_eq!(
            declarations[2].id_columns,
            vec!["k8s.pod.uid", "k8s.container.name"]
        );
        assert_eq!(
            declarations[0].superseded_by_columns,
            vec!["k8s.pod.uid", "k8s.container.name"],
            "the generic container must yield to the k8s.container identity itself"
        );
        assert_eq!(declarations[1].id_columns, vec!["host.id"]);
        assert_eq!(declarations[1].descriptive_columns, vec!["host.name"]);
        assert_eq!(declarations[3].id_columns, vec!["k8s.node.name"]);
        assert_eq!(declarations[5].id_columns, vec!["job"]);
        assert_eq!(
            declarations[5].descriptive_columns,
            vec!["service.name", "service.namespace"]
        );
        assert_eq!(declarations[6].id_columns, vec!["job", "instance"]);
        assert!(declarations[6].descriptive_columns.is_empty());

        // Nothing here can produce a k8s.container, so the generic one must
        // stand or the container disappears instead of changing type.
        let no_k8s = prom_table_info(
            "greptime_otel_resource_info",
            &["job", "container.id", "k8s.pod.uid"],
            OTEL_STAMPS,
        );
        let declarations = sorted_declarations(&no_k8s);
        assert_eq!(declarations[0].entity_type, "container");
        assert!(declarations[0].superseded_by_columns.is_empty());

        // Same rule when a skipped explicit declaration blocks the implicit
        // one: nothing declares the type, so nothing may yield to it.
        let mut stamps = OTEL_STAMPS.to_vec();
        stamps.push(("greptime.semantic.entity.k8s.container.id", "gone"));
        let broken_explicit = prom_table_info(
            "greptime_otel_resource_info",
            &["job", "container.id", "k8s.pod.uid", "k8s.container.name"],
            &stamps,
        );
        let declarations = sorted_declarations(&broken_explicit);
        let types: Vec<&str> = declarations
            .iter()
            .map(|d| d.entity_type.as_str())
            .collect();
        assert_eq!(types, vec!["container", "k8s.pod", "service"]);
        assert!(declarations[0].superseded_by_columns.is_empty());

        let partial = prom_table_info(
            "greptime_otel_resource_info",
            &["job", "service.name", "host.id"],
            OTEL_STAMPS,
        );
        let types: Vec<String> = sorted_declarations(&partial)
            .into_iter()
            .map(|d| d.entity_type)
            .collect();
        assert_eq!(types, vec!["host", "service"]);
    }

    #[test]
    fn otel_implicit_declarations_are_gated() {
        let labels: &[&str] = &["job", "instance", "host.id"];
        assert!(
            sorted_declarations(&prom_table_info(
                "greptime_otel_resource_info",
                labels,
                PROM_STAMPS
            ))
            .is_empty()
        );
        let mut stamps = OTEL_STAMPS.to_vec();
        stamps.push((PHYSICAL_TABLE_METADATA_KEY, "true"));
        assert!(
            sorted_declarations(&prom_table_info(
                "greptime_otel_resource_info",
                labels,
                &stamps
            ))
            .is_empty()
        );
        assert!(
            conventions()
                .unwrap()
                .otel_info_metrics
                .contains_key(servers::otlp::metrics::OTEL_RESOURCE_INFO_TABLE_NAME)
        );

        let wrong_type = [
            (SEMANTIC_SIGNAL_TYPE, SIGNAL_TYPE_METRIC),
            (SEMANTIC_SOURCE, SOURCE_OPENTELEMETRY),
            (SEMANTIC_METRIC_TYPE, servers::semantic::METRIC_TYPE_GAUGE),
        ];
        assert!(
            sorted_declarations(&prom_table_info(
                "greptime_otel_resource_info",
                labels,
                &wrong_type
            ))
            .is_empty()
        );
    }

    #[test]
    fn target_info_descriptive_rest_covers_remaining_tags() {
        let marker = greptime_temporality_label();
        let info = prom_table_info(
            "target_info",
            &[
                "job",
                "instance",
                "k8s_cluster_name",
                "service_version",
                marker,
            ],
            PROM_STAMPS,
        );
        let declarations = sorted_declarations(&info);
        assert_eq!(declarations.len(), 2);
        assert_eq!(declarations[0].entity_type, "service");
        assert_eq!(declarations[0].id_columns, vec!["job"]);
        assert!(declarations[0].descriptive_columns.is_empty());
        // The remaining labels are the target's resource attributes: they
        // describe the instance, not the logical service.
        assert_eq!(declarations[1].entity_type, "service.instance");
        assert_eq!(declarations[1].id_columns, vec!["job", "instance"]);
        assert_eq!(
            declarations[1].descriptive_columns,
            vec!["k8s_cluster_name", "service_version"]
        );
    }
}
