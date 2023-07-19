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

use common_query::Output;
use table::engine::TableReference;
use table::requests::TruncateTableRequest;

use crate::error::Result;
use crate::sql::SqlHandler;

impl SqlHandler {
    pub(crate) async fn truncate_table(&self, req: TruncateTableRequest) -> Result<Output> {
        let table_name = req.table_name.clone();
        let table_ref = TableReference {
            catalog: &req.catalog_name,
            schema: &req.schema_name,
            table: &table_name,
        };

        let _table = self.get_table(&table_ref).await?;
        // TODO(DevilExileSu): implement truncate table-procedure.
        Ok(Output::AffectedRows(0))
    }
}

#[cfg(test)]
mod tests {
    use api::v1::greptime_request::Request;
    use api::v1::query_request::Query;
    use api::v1::QueryRequest;
    use datatypes::prelude::ConcreteDataType;
    use query::parser::{QueryLanguageParser, QueryStatement};
    use query::query_engine::SqlStatementExecutor;
    use servers::query_handler::grpc::GrpcQueryHandler;
    use session::context::QueryContext;

    use super::*;
    use crate::tests::test_util::{create_test_table, MockInstance};

    #[tokio::test(flavor = "multi_thread")]
    async fn test_truncate_table_by_procedure() {
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
    }
}
