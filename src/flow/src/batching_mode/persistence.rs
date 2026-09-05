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

//! Optional collaborator boundary for batching state.
//!
//! The batching engine only deals in the small, typed values described here. It
//! does not interpret or persist any of them.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use common_error::ext::BoxedError;
use common_query::Output;
use datafusion_common::ScalarValue;
use datatypes::prelude::ConcreteDataType;
use store_api::storage::TableId;

use crate::Result;
use crate::batching_mode::IncrementalMode;

/// SQL execution exposed to a persistence implementation for reading and
/// writing internal state. The batching engine does not interpret this SQL;
/// the persistence implementation owns the SQL and its schema.
#[async_trait::async_trait]
pub trait BatchingQueryExecutor: Send + Sync + 'static {
    async fn execute_sql(&self, catalog: &str, schema: &str, sql: &str) -> Result<Output>;
}

/// Describes the validated sink table available to a persistence collaborator.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SinkLayout {
    pub table_id: TableId,
    pub table_name: [String; 3],
    pub engine: String,
    pub append: bool,
    pub merge_mode: Option<String>,
    pub columns: Vec<BatchingMetadataColumn>,
    pub ordered_primary_key_indices: Vec<usize>,
    pub time_index: Option<usize>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BatchingMetadataColumn {
    pub name: String,
    pub data_type: ConcreteDataType,
    pub nullable: bool,
}

/// Result of restoring a task's opaque state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RestoreOutcome {
    TrustedCheckpoint(BTreeMap<u64, u64>),
    FullRepair,
}

/// One serialized execution attempt.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct BatchingAttempt {
    pub ordinary_values: BTreeMap<String, ScalarValue>,
}

#[async_trait::async_trait]
pub trait BatchingPersistence: Send + Sync + 'static {
    async fn restore(&self) -> Result<RestoreOutcome>;
    async fn begin_attempt(&self) -> Result<BatchingAttempt>;
    async fn persist(
        &self,
        attempt: BatchingAttempt,
        validated_checkpoints: BTreeMap<u64, u64>,
    ) -> Result<()>;
}

/// Context supplied to the typed persistence factory for one batching flow.
#[derive(Clone)]
pub struct PersistenceContext {
    pub flow_id: crate::FlowId,
    pub incremental_mode: IncrementalMode,
    pub sink: SinkLayout,
    pub executor: Arc<dyn BatchingQueryExecutor>,
}

/// Typed factory implementation used by the batching engine.
#[async_trait::async_trait]
pub trait Factory: Send + Sync + 'static {
    async fn create(
        &self,
        context: PersistenceContext,
    ) -> Result<Option<Arc<dyn BatchingPersistence>>>;
}

/// Typed plugin wrapper around a persistence factory.
#[derive(Clone)]
pub struct FactoryPlugin(pub Arc<dyn Factory>);

impl std::ops::Deref for FactoryPlugin {
    type Target = dyn Factory;

    fn deref(&self) -> &Self::Target {
        self.0.as_ref()
    }
}

/// Query executor backed by the existing flownode frontend client.
pub(crate) struct FrontendBatchingQueryExecutor {
    client: Arc<crate::FrontendClient>,
}

impl FrontendBatchingQueryExecutor {
    pub(crate) fn new(client: Arc<crate::FrontendClient>) -> Self {
        Self { client }
    }
}

#[async_trait::async_trait]
impl BatchingQueryExecutor for FrontendBatchingQueryExecutor {
    async fn execute_sql(&self, catalog: &str, schema: &str, sql: &str) -> Result<Output> {
        let mut peer = None;
        let request = api::v1::QueryRequest {
            query: Some(api::v1::query_request::Query::Sql(sql.to_string())),
        };
        self.client
            .query_with_terminal_metrics(catalog, schema, request, &[], &HashMap::new(), &mut peer)
            .await
            .map(|output| output.into_output())
            .map_err(|err| crate::Error::External {
                source: BoxedError::new(err),
                location: snafu::location!(),
            })
    }
}
