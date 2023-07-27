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

use common_procedure::{watcher, ProcedureWithId};
use common_query::Output;
use common_telemetry::logging::info;
use snafu::ResultExt;
use table::engine::TableReference;
use table::requests::TruncateTableRequest;
use table_procedure::TruncateTableProcedure;

use crate::error::{self, Result};
use crate::sql::SqlHandler;

impl SqlHandler {
    pub(crate) async fn truncate_table(&self, req: TruncateTableRequest) -> Result<Output> {
        let table_name = req.table_name.clone();
        let table_ref = TableReference {
            catalog: &req.catalog_name,
            schema: &req.schema_name,
            table: &table_name,
        };

        let table = self.get_table(&table_ref).await?;
        let engine_procedure = self.engine_procedure(table)?;

        let procedure =
            TruncateTableProcedure::new(req, self.catalog_manager.clone(), engine_procedure);

        let procedure_with_id = ProcedureWithId::with_random_id(Box::new(procedure));
        let procedure_id = procedure_with_id.id;

        info!(
            "Truncate table {} by procedure {}",
            table_name, procedure_id
        );

        let mut watcher = self
            .procedure_manager
            .submit(procedure_with_id)
            .await
            .context(error::SubmitProcedureSnafu { procedure_id })?;

        watcher::wait(&mut watcher)
            .await
            .context(error::WaitProcedureSnafu { procedure_id })?;
        Ok(Output::AffectedRows(0))
    }
}

#[cfg(test)]
mod tests {
    use api::v1::greptime_request::Request;
    use api::v1::query_request::Query;
    use api::v1::QueryRequest;
    use common_recordbatch::RecordBatches;
    use datatypes::prelude::ConcreteDataType;
    use query::parser::{QueryLanguageParser, QueryStatement};
    use query::query_engine::SqlStatementExecutor;
    use servers::query_handler::grpc::GrpcQueryHandler;
    use session::context::QueryContext;

    use super::*;
    use crate::tests::test_util::{create_test_table, MockInstance};

    #[tokio::test(flavor = "multi_thread")]
    async fn test_truncate_table_by_procedure() {
        common_telemetry::init_default_ut_logging();
        let instance = MockInstance::new("truncate_table_by_procedure").await;

        // Create table first.
        let _table = create_test_table(
            instance.inner(),
            ConcreteDataType::timestamp_millisecond_datatype(),
        )
        .await
        .unwrap();

        // Insert data.
        let query = Request::Query(QueryRequest {
            query: Some(Query::Sql(
                "INSERT INTO demo(host, cpu, memory, ts) VALUES \
                            ('host1', 66.6, 1024, 1672201025000),\
                            ('host2', 88.8, 333.3, 1672201026000),\
                            ('host3', 88.8, 333.3, 1672201026000)"
                    .to_string(),
            )),
        });

        let output = instance
            .inner()
            .do_query(query, QueryContext::arc())
            .await
            .unwrap();
        assert!(matches!(output, Output::AffectedRows(3)));

        // Truncate table.
        let sql = r#"truncate table demo"#;
        let stmt = match QueryLanguageParser::parse_sql(sql).unwrap() {
            QueryStatement::Sql(sql) => sql,
            _ => unreachable!(),
        };
        let output = instance
            .inner()
            .execute_sql(stmt, QueryContext::arc())
            .await
            .unwrap();
        assert!(matches!(output, Output::AffectedRows(0)));

        // Verify table is empty.
        let query = Request::Query(QueryRequest {
            query: Some(Query::Sql("SELECT * FROM demo".to_string())),
        });

        let output = instance
            .inner()
            .do_query(query, QueryContext::arc())
            .await
            .unwrap();
        if let Output::Stream(stream) = output {
            let output = RecordBatches::try_collect(stream)
                .await
                .unwrap()
                .pretty_print()
                .unwrap();
            assert_eq!("++\n++", output)
        }
    }
}
