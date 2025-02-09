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

use async_trait::async_trait;
use common_query::Output;
use common_recordbatch::adapter::RecordBatchStreamAdapter;
use datafusion_expr::col;
use query::dataframe::DataFrame;
use servers::error::{
    CatalogSnafu, CollectRecordbatchSnafu, DataFusionSnafu, ReadTableSnafu, Result as ServerResult,
    TableNotFoundSnafu,
};
use servers::otlp::trace::TRACE_TABLE_NAME;
use servers::query_handler::JaegerQueryHandler;
use session::context::QueryContextRef;
use snafu::{OptionExt, ResultExt};

use super::Instance;

const SERVICE_NAME_COLUMN: &str = "service_name";

#[async_trait]
impl JaegerQueryHandler for Instance {
    async fn get_services(&self, ctx: QueryContextRef) -> ServerResult<Output> {
        let catalog_manager = self.catalog_manager();
        let query_engine = self.query_engine();
        let db = ctx.get_db_string();

        let table = catalog_manager
            .table(ctx.current_catalog(), &db, TRACE_TABLE_NAME, Some(&ctx))
            .await
            .context(CatalogSnafu)?
            .with_context(|| TableNotFoundSnafu {
                table: TRACE_TABLE_NAME,
                catalog: ctx.current_catalog().to_string(),
                schema: db.to_string(),
            })?;

        let DataFrame::DataFusion(dataframe) =
            query_engine.read_table(table).context(ReadTableSnafu)?;

        // It's equivalent to `SELECT DISTINCT(service_name) FROM ${db}.${trace_table_name}`.
        let select = vec![col(SERVICE_NAME_COLUMN)];
        let dataframe = dataframe.select(select).context(DataFusionSnafu)?;
        let dataframe = dataframe.distinct().context(DataFusionSnafu)?;
        let stream = dataframe.execute_stream().await.context(DataFusionSnafu)?;

        let output = Output::new_with_stream(Box::pin(
            RecordBatchStreamAdapter::try_new(stream).context(CollectRecordbatchSnafu)?,
        ));

        Ok(output)
    }

    async fn get_trace(&self, ctx: QueryContextRef, trace_id: String) -> ServerResult<Output> {
        todo!()
    }
}
