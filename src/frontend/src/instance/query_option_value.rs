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

//! Frontend implementation of [`QueryOptionValueProvider`]: lets
//! `information_schema.session_variables` report the engine's *effective*
//! value for a `query.*` option, so an option the session never set shows its
//! default instead of going missing.
//!
//! Injected into the catalog manager after the query engine is built, breaking
//! the `catalog -> query` dependency cycle the same way the entity-graph
//! provider does.

use std::sync::Arc;

use catalog::information_schema::QueryOptionValueProvider;
use common_telemetry::warn;
use query::QueryEngineRef;
use session::context::QueryContextRef;

/// Resolves `query.*` options through the query engine's own `SHOW` path.
pub struct QueryOptionValueProviderImpl {
    query_engine: QueryEngineRef,
}

impl QueryOptionValueProviderImpl {
    pub fn new(query_engine: QueryEngineRef) -> Self {
        Self { query_engine }
    }
}

impl QueryOptionValueProvider for QueryOptionValueProviderImpl {
    fn effective_value(&self, name: &str, query_ctx: QueryContextRef) -> Option<String> {
        // A resolution failure means the option cannot be reported for this
        // session; omitting the row is better than failing the whole scan of a
        // diagnostic table.
        match query::sql::effective_query_option_value(name, query_ctx, self.query_engine.as_ref())
        {
            Ok(value) => value,
            Err(error) => {
                warn!(error; "Failed to resolve query option '{name}' for session_variables");
                None
            }
        }
    }
}

/// Builds the provider as the catalog manager stores it.
pub fn build_query_option_value_provider(
    query_engine: QueryEngineRef,
) -> Arc<dyn QueryOptionValueProvider> {
    Arc::new(QueryOptionValueProviderImpl::new(query_engine))
}
