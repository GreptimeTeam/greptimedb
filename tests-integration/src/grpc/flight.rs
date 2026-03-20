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

    use api::v1::auth_header::AuthScheme;
    use api::v1::{Basic, ColumnDataType, ColumnDef, CreateTableExpr, SemanticType};
    use arrow_flight::FlightDescriptor;
    use auth::user_provider_from_option;
    use client::{Client, Database};
    use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
    use common_error::ext::ErrorExt;
    use common_error::status_code::StatusCode;
    use common_grpc::flight::do_put::DoPutMetadata;
    use common_grpc::flight::{FlightEncoder, FlightMessage};
    use common_query::OutputData;
    use common_recordbatch::RecordBatch;
    use common_recordbatch::adapter::{RecordBatchMetrics, RegionWatermarkEntry};
    use datatypes::prelude::{ConcreteDataType, ScalarVector, VectorRef};
    use datatypes::schema::{ColumnSchema, Schema};
    use datatypes::vectors::{Int32Vector, StringVector, TimestampMillisecondVector};
    use futures_util::StreamExt;
    use itertools::Itertools;
    use servers::grpc::builder::GrpcServerBuilder;
    use servers::grpc::greptime_handler::GreptimeRequestHandler;
    use servers::grpc::{FlightCompression, GrpcServerConfig};
    use servers::server::Server;
    use store_api::storage::RegionId;

    use crate::cluster::GreptimeDbClusterBuilder;
    use crate::grpc::query_and_expect;
    use crate::test_util::{StorageType, setup_grpc_server};
    use crate::tests::test_util::MockInstance;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_standalone_flight_do_put() {
        common_telemetry::init_default_ut_logging();

        let (db, server) =
            setup_grpc_server(StorageType::File, "test_standalone_flight_do_put").await;
        let addr = server.bind_addr().unwrap().to_string();

        let client = Client::with_urls(vec![addr]);
        let client = Database::new_with_dbname("greptime-public", client);

        create_table(&client).await;

        let record_batches = create_record_batches(1);
        test_put_record_batches(&client, record_batches).await;

        let sql = "select ts, a, `B` from foo order by ts";
        let expected = "\
+-------------------------+----+----+
| ts                      | a  | B  |
+-------------------------+----+----+
| 1970-01-01T00:00:00.001 | -1 | s1 |
| 1970-01-01T00:00:00.002 | -2 | s2 |
| 1970-01-01T00:00:00.003 | -3 | s3 |
| 1970-01-01T00:00:00.004 | -4 | s4 |
| 1970-01-01T00:00:00.005 | -5 | s5 |
| 1970-01-01T00:00:00.006 | -6 | s6 |
| 1970-01-01T00:00:00.007 | -7 | s7 |
| 1970-01-01T00:00:00.008 | -8 | s8 |
| 1970-01-01T00:00:00.009 | -9 | s9 |
+-------------------------+----+----+";
        query_and_expect(db.frontend().as_ref(), sql, expected).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_distributed_flight_do_put() {
        common_telemetry::init_default_ut_logging();

        let db = GreptimeDbClusterBuilder::new("test_distributed_flight_do_put")
            .await
            .build(false)
            .await;

        let runtime = common_runtime::global_runtime().clone();
        let greptime_request_handler = GreptimeRequestHandler::new(
            db.frontend.instance.clone(),
            user_provider_from_option("static_user_provider:cmd:greptime_user=greptime_pwd").ok(),
            Some(runtime.clone()),
            FlightCompression::default(),
        );
        let mut grpc_server = GrpcServerBuilder::new(GrpcServerConfig::default(), runtime)
            .flight_handler(Arc::new(greptime_request_handler))
            .build();
        grpc_server
            .start("127.0.0.1:0".parse::<SocketAddr>().unwrap())
            .await
            .unwrap();
        let addr = grpc_server.bind_addr().unwrap().to_string();

        let client = Client::with_urls(vec![addr]);
        let mut client = Database::new(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, client);
        client.set_auth(AuthScheme::Basic(Basic {
            username: "greptime_user".to_string(),
            password: "greptime_pwd".to_string(),
        }));

        create_table(&client).await;

        let record_batches = create_record_batches(1);
        test_put_record_batches(&client, record_batches).await;

        let sql = "select ts, a, `B` from foo order by ts";
        let expected = "\
+-------------------------+----+----+
| ts                      | a  | B  |
+-------------------------+----+----+
| 1970-01-01T00:00:00.001 | -1 | s1 |
| 1970-01-01T00:00:00.002 | -2 | s2 |
| 1970-01-01T00:00:00.003 | -3 | s3 |
| 1970-01-01T00:00:00.004 | -4 | s4 |
| 1970-01-01T00:00:00.005 | -5 | s5 |
| 1970-01-01T00:00:00.006 | -6 | s6 |
| 1970-01-01T00:00:00.007 | -7 | s7 |
| 1970-01-01T00:00:00.008 | -8 | s8 |
| 1970-01-01T00:00:00.009 | -9 | s9 |
+-------------------------+----+----+";
        query_and_expect(db.fe_instance().as_ref(), sql, expected).await;

        let output = client.sql(sql).await.unwrap();
        let OutputData::Stream(mut stream) = output.data else {
            panic!("expected stream output");
        };
        while let Some(batch) = stream.next().await {
            batch.unwrap();
        }
        let metrics = stream.metrics().expect("expected terminal metrics");
        assert!(metrics.region_watermarks.is_empty());

        let result = client
            .sql_with_terminal_metrics(sql, &[("flow.return_region_seq", "true")])
            .await
            .unwrap();
        let terminal_metrics = result.metrics.clone();
        let OutputData::Stream(mut stream) = result.output.data else {
            panic!("expected stream output");
        };
        while let Some(batch) = stream.next().await {
            batch.unwrap();
        }
        let regions = db.list_all_regions().await;
        assert_eq!(regions.len(), 1);
        let (region_id, region) = regions.into_iter().next().unwrap();
        let expected_watermark = (region_id.as_u64(), region.find_committed_sequence());
        assert_eq!(
            terminal_metrics.region_watermark_map(),
            Some(std::collections::HashMap::from([expected_watermark]))
        );

        let output = client
            .sql_with_hint(sql, &[("flow.return_region_seq", "true")])
            .await
            .unwrap();
        let OutputData::Stream(mut stream) = output.data else {
            panic!("expected stream output");
        };

        let mut row_count = 0;
        while let Some(batch) = stream.next().await {
            let batch = batch.unwrap();
            row_count += batch.num_rows();
        }
        assert_eq!(row_count, 9);

        let metrics = stream.metrics().expect("expected terminal metrics");
        let region_watermarks = metrics.region_watermarks;
        assert_eq!(
            region_watermarks,
            vec![RegionWatermarkEntry {
                region_id: expected_watermark.0,
                watermark: Some(expected_watermark.1),
            }]
        );

        let previous_watermark = expected_watermark;

        create_table_named(&client, "bar").await;
        let result = client
            .sql_with_terminal_metrics("insert into bar select ts, a, `B` from foo", &[])
            .await
            .unwrap();
        let OutputData::AffectedRows(affected_rows) = result.output.data else {
            panic!("expected affected rows output");
        };
        assert_eq!(affected_rows, 9);
        assert!(result.metrics.is_ready());
        assert!(result.region_watermark_map().is_none());

        let err = client
            .sql_with_terminal_metrics(
                "insert into bar select ts, a, `B` from foo",
                &[("flow.return_region_seq", "not-a-bool")],
            )
            .await
            .unwrap_err();
        let err_msg = format!("{err:?}");
        assert!(err_msg.contains("Invalid value for flow.return_region_seq"));

        client.sql("truncate table bar").await.unwrap();

        let result = client
            .sql_with_terminal_metrics(
                "insert into bar select ts, a, `B` from foo",
                &[("flow.return_region_seq", "true")],
            )
            .await
            .unwrap();
        let OutputData::AffectedRows(affected_rows) = result.output.data else {
            panic!("expected affected rows output");
        };
        assert_eq!(affected_rows, 9);
        assert_eq!(
            result.region_watermark_map(),
            Some(std::collections::HashMap::from([previous_watermark]))
        );

        let incremental_batches = create_record_batches(10);
        test_put_record_batches(&client, incremental_batches).await;

        let output = client
            .sql_with_hint(
                sql,
                &[
                    ("flow.incremental_mode", "memtable_only"),
                    (
                        "flow.incremental_after_seqs",
                        &format!(
                            r#"{{"{}":"{}"}}"#,
                            previous_watermark.0, previous_watermark.1
                        ),
                    ),
                    ("flow.return_region_seq", "true"),
                ],
            )
            .await
            .unwrap();
        let OutputData::Stream(mut stream) = output.data else {
            panic!("expected stream output");
        };

        let schema = stream.schema();
        let mut rows = Vec::new();
        while let Some(batch) = stream.next().await {
            rows.push(batch.unwrap());
        }
        let pretty = common_recordbatch::RecordBatches::try_new(schema, rows)
            .unwrap()
            .pretty_print()
            .unwrap();
        assert_eq!(
            pretty,
            "\
+-------------------------+-----+-----+
| ts                      | a   | B   |
+-------------------------+-----+-----+
| 1970-01-01T00:00:00.010 | -10 | s10 |
| 1970-01-01T00:00:00.011 | -11 | s11 |
| 1970-01-01T00:00:00.012 | -12 | s12 |
| 1970-01-01T00:00:00.013 | -13 | s13 |
| 1970-01-01T00:00:00.014 | -14 | s14 |
| 1970-01-01T00:00:00.015 | -15 | s15 |
| 1970-01-01T00:00:00.016 | -16 | s16 |
| 1970-01-01T00:00:00.017 | -17 | s17 |
| 1970-01-01T00:00:00.018 | -18 | s18 |
+-------------------------+-----+-----+"
        );

        let RecordBatchMetrics {
            region_watermarks, ..
        } = stream
            .metrics()
            .expect("expected terminal incremental metrics");
        let regions = db.list_all_regions().await;
        let region = regions
            .get(&RegionId::from_u64(previous_watermark.0))
            .expect("expected source region to exist");
        let expected_incremental_watermark =
            (previous_watermark.0, region.find_committed_sequence());
        assert_eq!(
            region_watermarks,
            vec![RegionWatermarkEntry {
                region_id: expected_incremental_watermark.0,
                watermark: Some(expected_incremental_watermark.1),
            }]
        );

        client.sql("admin flush_table('foo')").await.unwrap();

        let output = client
            .sql_with_hint(
                sql,
                &[
                    ("flow.incremental_mode", "memtable_only"),
                    (
                        "flow.incremental_after_seqs",
                        &format!(
                            r#"{{"{}":"{}"}}"#,
                            previous_watermark.0, previous_watermark.1
                        ),
                    ),
                    ("flow.return_region_seq", "true"),
                ],
            )
            .await
            .unwrap();
        let OutputData::Stream(mut stream) = output.data else {
            panic!("expected stream output");
        };
        let err = loop {
            match stream.next().await {
                Some(Err(err)) => break err,
                Some(Ok(_)) => continue,
                None => panic!("expected stale incremental query to fail while consuming stream"),
            }
        };
        assert_eq!(err.status_code(), StatusCode::EngineExecuteQuery);
        let err_msg = format!("{err:?}");
        assert!(err_msg.contains("STALE_CURSOR"));
        assert!(err_msg.contains(&previous_watermark.0.to_string()));
        assert!(err_msg.contains(&previous_watermark.1.to_string()));
    }

    async fn test_put_record_batches(client: &Database, record_batches: Vec<RecordBatch>) {
        let requests_count = record_batches.len();
        let schema = record_batches[0].schema.arrow_schema().clone();

        let stream = futures::stream::once(async move {
            let mut schema_data = FlightEncoder::default().encode_schema(schema.as_ref());
            let metadata = DoPutMetadata::new(0);
            schema_data.app_metadata = serde_json::to_vec(&metadata).unwrap().into();
            // first message in "DoPut" stream should carry table name in flight descriptor
            schema_data.flight_descriptor = Some(FlightDescriptor {
                r#type: arrow_flight::flight_descriptor::DescriptorType::Path as i32,
                path: vec!["foo".to_string()],
                ..Default::default()
            });
            schema_data
        })
        .chain(
            tokio_stream::iter(record_batches)
                .enumerate()
                .flat_map(|(i, x)| {
                    let mut encoder = FlightEncoder::default();
                    let message = FlightMessage::RecordBatch(x.into_df_record_batch());
                    let mut data = encoder.encode(message);
                    let metadata = DoPutMetadata::new((i + 1) as i64);
                    data.iter_mut().for_each(|x| {
                        x.app_metadata = serde_json::to_vec(&metadata).unwrap().into()
                    });
                    tokio_stream::iter(data)
                })
                .boxed(),
        )
        .boxed();

        let response_stream = client.do_put(stream).await.unwrap();

        let responses = response_stream.collect::<Vec<_>>().await;
        let responses_count = responses.len();
        for (i, response) in responses.into_iter().enumerate() {
            assert!(response.is_ok(), "{}", response.err().unwrap());
            let response = response.unwrap();
            assert_eq!(response.request_id(), i as i64);
            if i == 0 {
                // the first is schema
                assert_eq!(response.affected_rows(), 0);
            } else {
                assert_eq!(response.affected_rows(), 3);
            }
        }
        assert_eq!(requests_count + 1, responses_count);
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
            ColumnSchema::new("B", ConcreteDataType::string_datatype(), true),
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
        create_table_named(client, "foo").await;
    }

    async fn create_table_named(client: &Database, table_name: &str) {
        // create table foo (
        //   ts timestamp time index,
        //   a int primary key,
        //   b string,
        // )
        let output = client
            .create(CreateTableExpr {
                schema_name: "public".to_string(),
                table_name: table_name.to_string(),
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
                        name: "B".to_string(),
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
