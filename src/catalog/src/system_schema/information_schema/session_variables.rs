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

//! `information_schema.session_variables`.
//!
//! Unlike every other `information_schema` table, this one reports *per-session*
//! state, which lives on the `QueryContext`. The provider that owns the other
//! tables is cached per catalog, so it cannot hold a context; this table is
//! therefore rebuilt on demand for each resolution, the same way
//! [`SemanticGraphTableProvider`](crate::system_schema::semantic_graph) is.
//! The static registration stays in place so that `SHOW TABLES`,
//! `information_schema.columns` and existence checks keep working without a
//! query context.

use std::collections::HashMap;
use std::sync::Arc;

use common_catalog::consts::{
    INFORMATION_SCHEMA_NAME, INFORMATION_SCHEMA_SESSION_VARIABLES_TABLE_ID,
};
use common_error::ext::BoxedError;
use common_recordbatch::adapter::RecordBatchStreamAdapter;
use common_recordbatch::{RecordBatch, SendableRecordBatchStream};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter as DfRecordBatchStreamAdapter;
use datatypes::schema::SchemaRef;
use datatypes::value::Value;
use datatypes::vectors::{StringVector, VectorRef};
use session::context::QueryContextRef;
use snafu::ResultExt;
use store_api::storage::{ScanRequest, TableId};
use table::TableRef;
use table::requests::{SEMANTIC_TABLE_SCOPE, TableOptions};

use super::information_memory_table::get_schema_columns;
use super::{InformationSchemaTableScope, InformationTable, SESSION_VARIABLES};
use crate::error::{self, InternalSnafu};
use crate::system_schema::predicate::Predicates;
use crate::system_schema::{SystemTable, build_system_table};

/// Column names of `information_schema.session_variables`.
pub const VARIABLE_NAME: &str = "variable_name";
pub const VARIABLE_VALUE: &str = "variable_value";

/// Resolves a `query.*` option to the value the engine would use for it.
///
/// The effective value lives in the query engine's configuration, which this
/// crate cannot reach: `query` depends on `catalog`, so the dependency would
/// cycle. Implemented in the frontend and injected into the catalog manager
/// after the engine is built, the same late binding
/// [`EntityGraphProvider`](crate::system_schema::semantic_graph::EntityGraphProvider)
/// uses.
///
/// `None` means the option is unknown, which the table reports by omitting the
/// row rather than by inventing a value.
pub trait QueryOptionValueProvider: Send + Sync {
    fn effective_value(&self, name: &str, query_ctx: QueryContextRef) -> Option<String>;
}

pub type QueryOptionValueProviderRef = Arc<dyn QueryOptionValueProvider>;

/// Query options reported by the table, in the order they appear.
///
/// Each is a key `SHOW <name>` already answers; the table reports the engine's
/// effective value, so an option the session never set shows its default.
pub const QUERY_OPTIONS: &[&str] = &[
    "query.parallelism",
    "query.allow_query_fallback",
    "query.enable_remote_dynamic_filter_pushdown",
    "query.experimental_native_promql",
];

/// Variables reported by the table.
///
/// Each name mirrors a `@@` lookup that [`QueryContext`](session::context::QueryContext)
/// already answers, so a client reading the table and a client reading `@@` see
/// the same value for the same variable.
pub const MAX_EXECUTION_TIME: &str = "max_execution_time";
pub const TIME_ZONE: &str = "time_zone";
pub const READ_PREFERENCE: &str = "read_preference";
pub const SKIP_WAL: &str = "skip_wal";

/// `information_schema.session_variables`, bound to the resolving query's
/// session.
pub struct InformationSchemaSessionVariables {
    schema: SchemaRef,
    /// The resolving query's context, captured per resolution. The table is
    /// built on demand and never cached, so it cannot leak across sessions.
    query_ctx: Option<QueryContextRef>,
    /// Resolves `query.*` options to their effective values. `None` until the
    /// frontend installs it, in which case those options are simply omitted.
    query_options: Option<QueryOptionValueProviderRef>,
}

impl InformationSchemaSessionVariables {
    pub fn new(
        query_ctx: Option<QueryContextRef>,
        query_options: Option<QueryOptionValueProviderRef>,
    ) -> Self {
        // Reuse the statically registered schema so the context-bound instance
        // and the catalog-cached entry report identical columns.
        let (schema, _) = get_schema_columns(SESSION_VARIABLES);
        Self {
            schema,
            query_ctx,
            query_options,
        }
    }

    /// Builds the table as the catalog exposes it, with the same table id and
    /// scope option as the statically registered entry.
    pub fn build_table(
        catalog_name: &str,
        query_ctx: Option<QueryContextRef>,
        query_options: Option<QueryOptionValueProviderRef>,
    ) -> TableRef {
        let table = Self::new(query_ctx, query_options);
        let options = TableOptions {
            extra_options: HashMap::from([(
                SEMANTIC_TABLE_SCOPE.to_string(),
                table.scope().as_str().to_string(),
            )]),
            ..Default::default()
        };
        build_system_table(
            catalog_name.to_string(),
            INFORMATION_SCHEMA_NAME,
            Arc::new(table),
            options,
        )
    }

    /// Returns `(variable_name, variable_value)` rows for the bound session.
    ///
    /// An absent context (context-less internal resolution) yields no rows: the
    /// table describes a session, and there is none to describe.
    fn rows(&self) -> Vec<(String, String)> {
        let Some(query_ctx) = self.query_ctx.as_ref() else {
            return Vec::new();
        };

        // Session-scoped values come from the context; the MySQL compatibility
        // constants come from `session::mysql_variables`, which is also what the
        // MySQL protocol layer answers `@@name` from. Sharing the definition is
        // what keeps `SELECT * FROM session_variables` and `SELECT @@name`
        // agreeing on the same variable.
        let mut rows = vec![
            (
                MAX_EXECUTION_TIME.to_string(),
                query_ctx.query_timeout_as_millis().to_string(),
            ),
            (TIME_ZONE.to_string(), query_ctx.timezone().to_string()),
            (
                READ_PREFERENCE.to_string(),
                query_ctx.read_preference().to_string(),
            ),
            (SKIP_WAL.to_string(), query_ctx.skip_wal().to_string()),
        ];
        rows.extend(
            session::mysql_variables::compatibility_variables()
                .into_iter()
                .map(|(name, value)| (name.to_string(), value.to_string())),
        );

        // `query.*` options report the engine's effective value, so an option
        // the session never set shows its default rather than going missing.
        // The resolver is injected by the frontend because the effective values
        // live above this crate in the query engine.
        if let Some(query_options) = self.query_options.as_ref() {
            rows.extend(QUERY_OPTIONS.iter().filter_map(|name| {
                query_options
                    .effective_value(name, query_ctx.clone())
                    .map(|value| (name.to_string(), value))
            }));
        }

        rows
    }

    fn make_record_batch(&self, request: ScanRequest) -> error::Result<RecordBatch> {
        let predicates = Predicates::from_scan_request(&Some(request));

        let mut names = Vec::new();
        let mut values = Vec::new();
        for (name, value) in self.rows() {
            let name_value = Value::from(name.clone());
            let value_value = Value::from(value.clone());
            let row = [(VARIABLE_NAME, &name_value), (VARIABLE_VALUE, &value_value)];
            if predicates.eval(&row) {
                names.push(name);
                values.push(value);
            }
        }

        RecordBatch::new(
            self.schema.clone(),
            vec![
                Arc::new(StringVector::from(names)) as VectorRef,
                Arc::new(StringVector::from(values)) as VectorRef,
            ],
        )
        .context(error::CreateRecordBatchSnafu)
    }
}

impl InformationTable for InformationSchemaSessionVariables {
    fn scope(&self) -> InformationSchemaTableScope {
        InformationSchemaTableScope::Global
    }
}

impl SystemTable for InformationSchemaSessionVariables {
    fn table_id(&self) -> TableId {
        INFORMATION_SCHEMA_SESSION_VARIABLES_TABLE_ID
    }

    fn table_name(&self) -> &'static str {
        SESSION_VARIABLES
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn to_stream(&self, request: ScanRequest) -> error::Result<SendableRecordBatchStream> {
        let schema = self.schema.clone();
        let batch = self.make_record_batch(request);
        let stream = Box::pin(DfRecordBatchStreamAdapter::new(
            schema.arrow_schema().clone(),
            futures::stream::once(async move {
                batch
                    .map(RecordBatch::into_df_record_batch)
                    .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))
            }),
        ));

        Ok(Box::pin(
            RecordBatchStreamAdapter::try_new(stream)
                .map_err(BoxedError::new)
                .context(InternalSnafu)?,
        ))
    }
}

#[cfg(test)]
mod tests {
    use common_catalog::consts::DEFAULT_CATALOG_NAME;
    use common_recordbatch::RecordBatches;
    use common_time::timezone::parse_timezone;
    use session::context::QueryContext;

    use super::*;

    async fn collect(table: &InformationSchemaSessionVariables) -> RecordBatches {
        let stream = table.to_stream(ScanRequest::default()).unwrap();
        RecordBatches::try_collect(stream).await.unwrap()
    }

    #[tokio::test]
    async fn test_reports_the_bound_session_values() {
        let query_ctx = QueryContext::with(DEFAULT_CATALOG_NAME, "public");
        query_ctx.set_query_timeout(std::time::Duration::from_millis(2000));
        query_ctx.set_timezone(parse_timezone(Some("Asia/Shanghai")));
        query_ctx.set_skip_wal(true);

        let table = InformationSchemaSessionVariables::new(Some(Arc::new(query_ctx)), None);
        let batches = collect(&table).await;

        assert_eq!(
            "\
+-----------------------+-----------------------------------------------------------------------------------------------------------------------+
| variable_name         | variable_value                                                                                                        |
+-----------------------+-----------------------------------------------------------------------------------------------------------------------+
| max_execution_time    | 2000                                                                                                                  |
| time_zone             | Asia/Shanghai                                                                                                         |
| read_preference       | LEADER                                                                                                                |
| skip_wal              | true                                                                                                                  |
| transaction_isolation | REPEATABLE-READ                                                                                                       |
| transaction_read_only | 0                                                                                                                     |
| max_allowed_packet    | 134217728                                                                                                             |
| interactive_timeout   | 31536000                                                                                                              |
| wait_timeout          | 31536000                                                                                                              |
| net_write_timeout     | 31536000                                                                                                              |
| sql_mode              | ONLY_FULL_GROUP_BY STRICT_TRANS_TABLES NO_ZERO_IN_DATE NO_ZERO_DATE ERROR_FOR_DIVISION_BY_ZERO NO_ENGINE_SUBSTITUTION |
| version_comment       | GreptimeDB                                                                                                            |
+-----------------------+-----------------------------------------------------------------------------------------------------------------------+",
            batches.pretty_print().unwrap()
        );
    }

    #[tokio::test]
    async fn test_reports_defaults_without_any_set() {
        let query_ctx = QueryContext::with(DEFAULT_CATALOG_NAME, "public");
        let table = InformationSchemaSessionVariables::new(Some(Arc::new(query_ctx)), None);
        let batches = collect(&table).await;

        let text = batches.pretty_print().unwrap();
        assert!(text.contains("| max_execution_time    | 0 "), "{text}");
        assert!(text.contains("| skip_wal              | false "), "{text}");
        assert!(text.contains("| read_preference       | LEADER "), "{text}");
        // The MySQL compatibility constants are static, so they are reported
        // even when the session set nothing.
        assert!(
            text.contains("| transaction_isolation | REPEATABLE-READ "),
            "{text}"
        );
        assert!(
            text.contains("| sql_mode              | ONLY_FULL_GROUP_BY "),
            "{text}"
        );
    }

    #[tokio::test]
    async fn test_no_rows_without_a_context() {
        let table = InformationSchemaSessionVariables::new(None, None);
        let batches = collect(&table).await;

        assert_eq!(
            "\
+---------------+----------------+
| variable_name | variable_value |
+---------------+----------------+
+---------------+----------------+",
            batches.pretty_print().unwrap()
        );
    }

    /// Stands in for the frontend-installed resolver.
    struct FixedQueryOptions(&'static [(&'static str, &'static str)]);

    impl QueryOptionValueProvider for FixedQueryOptions {
        fn effective_value(&self, name: &str, _query_ctx: QueryContextRef) -> Option<String> {
            self.0
                .iter()
                .find(|(key, _)| *key == name)
                .map(|(_, value)| value.to_string())
        }
    }

    #[tokio::test]
    async fn test_reports_query_options_through_the_resolver() {
        let query_ctx = QueryContext::with(DEFAULT_CATALOG_NAME, "public");
        let resolver = Arc::new(FixedQueryOptions(&[("query.parallelism", "8")]));

        let table =
            InformationSchemaSessionVariables::new(Some(Arc::new(query_ctx)), Some(resolver));
        let text = collect(&table).await.pretty_print().unwrap();

        assert!(text.contains("| query.parallelism"), "{text}");
        assert!(text.contains("| 8 "), "{text}");
        // Options the resolver does not know are omitted, not invented.
        assert!(!text.contains("query.allow_query_fallback"), "{text}");
    }

    #[tokio::test]
    async fn test_omits_query_options_without_a_resolver() {
        let query_ctx = QueryContext::with(DEFAULT_CATALOG_NAME, "public");
        let table = InformationSchemaSessionVariables::new(Some(Arc::new(query_ctx)), None);
        let text = collect(&table).await.pretty_print().unwrap();

        assert!(!text.contains("query.parallelism"), "{text}");
    }

    #[tokio::test]
    async fn test_schema_matches_the_static_registration() {
        let (static_schema, _) = get_schema_columns(SESSION_VARIABLES);
        let table = InformationSchemaSessionVariables::new(None, None);

        assert_eq!(static_schema, table.schema());
        assert_eq!("variable_name", table.schema().column_schemas()[0].name);
        assert_eq!("variable_value", table.schema().column_schemas()[1].name);
    }
}
