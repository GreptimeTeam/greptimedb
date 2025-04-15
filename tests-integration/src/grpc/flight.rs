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

#[cfg(test)]
mod test {
    use std::net::SocketAddr;
    use std::sync::Arc;
    use std::time::Duration;

    use api::v1::auth_header::AuthScheme;
    use api::v1::greptime_response::Response;
    use api::v1::{Basic, ColumnDataType, ColumnDef, CreateTableExpr, SemanticType};
    use auth::user_provider_from_option;
    use client::{Client, Database};
    use common_query::OutputData;
    use common_recordbatch::RecordBatch;
    use datatypes::prelude::{ConcreteDataType, ScalarVector, VectorRef};
    use datatypes::schema::{ColumnSchema, Schema};
    use datatypes::vectors::{Int32Vector, StringVector, TimestampMillisecondVector};
    use futures_util::StreamExt;
    use itertools::Itertools;
    use servers::grpc::builder::GrpcServerBuilder;
    use servers::grpc::greptime_handler::GreptimeRequestHandler;
    use servers::grpc::GrpcServerConfig;
    use servers::query_handler::grpc::ServerGrpcQueryHandlerAdapter;
    use servers::server::Server;

    use crate::cluster::GreptimeDbClusterBuilder;
    use crate::grpc::query_and_expect;
    use crate::test_util::{setup_grpc_server_with_user_provider, StorageType};
    use crate::tests::test_util::MockInstance;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_standalone_flight_do_put() {
        common_telemetry::init_default_ut_logging();

        let (addr, db, _server) = setup_grpc_server_with_user_provider(
            StorageType::File,
            "test_standalone_flight_do_put",
            user_provider_from_option(
                &"static_user_provider:cmd:greptime_user=greptime_pwd".to_string(),
            )
            .ok(),
        )
        .await;

        let client = Client::with_urls(vec![addr]);
        let mut client = Database::new_with_dbname("public", client);
        client.set_auth(AuthScheme::Basic(Basic {
            username: "greptime_user".to_string(),
            password: "greptime_pwd".to_string(),
        }));

        create_table(&client).await;

        let record_batches = create_record_batches(1);
        test_put_record_batches(&client, record_batches).await;

        let sql = "select ts, a, b from foo order by ts";
        let expected = "\
++
++";
        query_and_expect(db.frontend().as_ref(), sql, expected).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_distributed_flight_do_put() {
        common_telemetry::init_default_ut_logging();

        let db = GreptimeDbClusterBuilder::new("test_distributed_flight_do_put")
            .await
            .build()
            .await;

        let runtime = common_runtime::global_runtime().clone();
        let greptime_request_handler = GreptimeRequestHandler::new(
            ServerGrpcQueryHandlerAdapter::arc(db.frontend.instance.clone()),
            user_provider_from_option(
                &"static_user_provider:cmd:greptime_user=greptime_pwd".to_string(),
            )
            .ok(),
            Some(runtime.clone()),
        );
        let grpc_server = GrpcServerBuilder::new(GrpcServerConfig::default(), runtime)
            .flight_handler(Arc::new(greptime_request_handler))
            .build();
        let addr = grpc_server
            .start("127.0.0.1:0".parse::<SocketAddr>().unwrap())
            .await
            .unwrap()
            .to_string();

        // wait for GRPC server to start
        tokio::time::sleep(Duration::from_secs(1)).await;

        let client = Client::with_urls(vec![addr]);
        let mut client = Database::new_with_dbname("public", client);
        client.set_auth(AuthScheme::Basic(Basic {
            username: "greptime_user".to_string(),
            password: "greptime_pwd".to_string(),
        }));

        create_table(&client).await;

        let record_batches = create_record_batches(1);
        test_put_record_batches(&client, record_batches).await;

        let sql = "select ts, a, b from foo order by ts";
        let expected = "\
++
++";
        query_and_expect(db.fe_instance().as_ref(), sql, expected).await;
    }

    async fn test_put_record_batches(client: &Database, record_batches: Vec<RecordBatch>) {
        let request_stream = tokio_stream::iter(record_batches).boxed();
        let response_stream = client
            .do_put("foo".to_string(), request_stream)
            .await
            .unwrap();
        let responses = response_stream.collect::<Vec<_>>().await;
        assert_eq!(responses.len(), 3);
        for response in responses {
            let Response::AffectedRows(affected_rows) = response.response.unwrap();
            assert_eq!(affected_rows.value, 448);
        }
    }

    fn create_record_batches(start: i64) -> Vec<RecordBatch> {
        let schema = Arc::new(Schema::new(vec![
            ColumnSchema::new(
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            )
            .with_time_index(true),
            ColumnSchema::new("a", ConcreteDataType::int32_datatype(), false),
            ColumnSchema::new("b", ConcreteDataType::string_datatype(), true),
        ]));

        let mut record_batches = Vec::with_capacity(3);
        for chunk in &(start..start + 9).chunks(3) {
            let vs = chunk.collect_vec();
            let x1 = vs[0];
            let x2 = vs[1];
            let x3 = vs[2];

            record_batches.push(
                RecordBatch::new(
                    schema.clone(),
                    vec![
                        Arc::new(TimestampMillisecondVector::from_vec(vec![x1, x2, x3]))
                            as VectorRef,
                        Arc::new(Int32Vector::from_vec(vec![
                            -x1 as i32, -x2 as i32, -x3 as i32,
                        ])),
                        Arc::new(StringVector::from_vec(vec![
                            format!("s{x1}"),
                            format!("s{x2}"),
                            format!("s{x3}"),
                        ])),
                    ],
                )
                .unwrap(),
            );
        }
        record_batches
    }

    async fn create_table(client: &Database) {
        // create table foo (
        //   ts timestamp time index,
        //   a int primary key,
        //   b string,
        // )
        let output = client
            .create(CreateTableExpr {
                schema_name: "public".to_string(),
                table_name: "foo".to_string(),
                column_defs: vec![
                    ColumnDef {
                        name: "ts".to_string(),
                        data_type: ColumnDataType::TimestampMillisecond as i32,
                        semantic_type: SemanticType::Timestamp as i32,
                        is_nullable: false,
                        ..Default::default()
                    },
                    ColumnDef {
                        name: "a".to_string(),
                        data_type: ColumnDataType::Int32 as i32,
                        semantic_type: SemanticType::Tag as i32,
                        is_nullable: false,
                        ..Default::default()
                    },
                    ColumnDef {
                        name: "b".to_string(),
                        data_type: ColumnDataType::String as i32,
                        semantic_type: SemanticType::Field as i32,
                        is_nullable: true,
                        ..Default::default()
                    },
                ],
                time_index: "ts".to_string(),
                primary_keys: vec!["a".to_string()],
                engine: "mito".to_string(),
                ..Default::default()
            })
            .await
            .unwrap();
        let OutputData::AffectedRows(affected_rows) = output.data else {
            unreachable!()
        };
        assert_eq!(affected_rows, 0);
    }
}
