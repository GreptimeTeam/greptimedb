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

use std::sync::{Arc, Weak};

use api::v1::ColumnSchema;
use async_trait::async_trait;
use servers::batcher::logical_table::{LogicalTablePendingRowsBatcher, PendingRowsSchemaAlterer};
use servers::error::{BatcherChannelClosedSnafu, Result};
use servers::http::BatchingProtocol;
use session::context::QueryContextRef;
use snafu::OptionExt;

use crate::frontend::FrontendOptions;
use crate::instance::Instance;

impl Instance {
    pub(crate) fn init_logical_batcher(self: &Arc<Self>, options: &FrontendOptions) {
        self.logical_batcher.get_or_init(|| {
            let options_batcher = options.logical_batcher_options();
            let enabled = options_batcher
                .protocols
                .iter()
                .any(|protocol| match protocol {
                    BatchingProtocol::Prom => options.prom_store.enable,
                    BatchingProtocol::Otlp => options.otlp.enable,
                    _ => false,
                });
            if !options.prom_store.with_metric_engine
                || !enabled
                || !options_batcher.pending_rows_batching_enabled()
            {
                return None;
            }
            LogicalTablePendingRowsBatcher::try_new(
                self.partition_manager().clone(),
                self.node_manager().clone(),
                self.catalog_manager().clone(),
                self.table_flownode_set_cache().clone(),
                true,
                Arc::new(LogicalTables(Arc::downgrade(self))),
                options_batcher.pending_rows_flush_interval,
                options_batcher.max_batch_rows,
                options_batcher.max_concurrent_flushes,
                options_batcher.worker_channel_capacity,
                options_batcher.max_inflight_requests,
                options_batcher.flow_notification_queue_capacity,
            )
        });
    }

    pub(crate) fn logical_batcher(&self) -> Option<&Arc<LogicalTablePendingRowsBatcher>> {
        self.logical_batcher.get().and_then(Option::as_ref)
    }
}

// The instance owns the shared batcher; schema preparation must not keep that
// owner alive through a reference cycle.
struct LogicalTables(Weak<Instance>);

#[async_trait]
impl PendingRowsSchemaAlterer for LogicalTables {
    async fn create_tables_if_missing_batch(
        &self,
        catalog: &str,
        schema: &str,
        tables: &[(&str, &[ColumnSchema])],
        with_metric_engine: bool,
        ctx: QueryContextRef,
    ) -> Result<()> {
        self.0
            .upgrade()
            .context(BatcherChannelClosedSnafu)?
            .create_tables_if_missing_batch(catalog, schema, tables, with_metric_engine, ctx)
            .await
    }

    async fn add_missing_prom_tag_columns_batch(
        &self,
        catalog: &str,
        schema: &str,
        tables: &[(&str, &[String])],
        ctx: QueryContextRef,
    ) -> Result<()> {
        self.0
            .upgrade()
            .context(BatcherChannelClosedSnafu)?
            .add_missing_prom_tag_columns_batch(catalog, schema, tables, ctx)
            .await
    }
}
