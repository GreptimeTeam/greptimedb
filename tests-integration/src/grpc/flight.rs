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
    use std::collections::HashMap;
    use std::net::SocketAddr;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{Duration, Instant};

    use api::v1::auth_header::AuthScheme;
    use api::v1::greptime_request::Request as GreptimeQueryRequest;
    use api::v1::health_check_server::HealthCheckServer;
    use api::v1::query_request::Query;
    use api::v1::{Basic, ColumnDataType, ColumnDef, CreateTableExpr, QueryRequest, SemanticType};
    use arrow_flight::flight_service_server::FlightServiceServer;
    use arrow_flight::{FlightData, FlightDescriptor, Ticket};
    use auth::user_provider_from_option;
    use client::client_manager::NodeClients;
    use client::region::RegionRequester;
    use client::{Client, Database};
    use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
    use common_grpc::channel_manager::{ChannelConfig, ChannelManager};
    use common_grpc::flight::do_put::{DoPutMetadata, DoPutResponse};
    use common_grpc::flight::{FlightDecoder, FlightEncoder, FlightMessage};
    use common_meta::peer::Peer;
    use common_query::{Output, OutputData};
    use common_recordbatch::adapter::RegionWatermarkEntry;
    use common_recordbatch::{
        RecordBatch, RecordBatchStreamWrapper, RecordBatches, SendableRecordBatchStream,
    };
    use common_telemetry::tracing_context::TracingContext;
    use datatypes::prelude::{ConcreteDataType, ScalarVector, VectorRef};
    use datatypes::schema::{ColumnSchema, Schema};
    use datatypes::vectors::{Int32Vector, StringVector, TimestampMillisecondVector};
    use futures_util::{Stream, StreamExt};
    use hyper_util::rt::TokioIo;
    use itertools::Itertools;
    use servers::grpc::builder::GrpcServerBuilder;
    use servers::grpc::flight::{
        FlightCraft, FlightCraftWrapper, FlightRecordBatchSource, FlightRecordBatchStream,
        FlightRecordBatchStreamInput, PutRecordBatchRequestStream, TonicStream,
    };
    use servers::grpc::greptime_handler::GreptimeRequestHandler;
    use servers::grpc::{FlightCompression, GrpcServerConfig};
    use servers::query_handler::grpc::GrpcQueryHandler;
    use servers::server::Server;
    use session::context::QueryContextRef;
    use tokio::net::TcpListener;
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::transport::Server as TonicServer;
    use tonic::{Response, Status};
    use tower::service_fn;

    use crate::cluster::GreptimeDbClusterBuilder;
    use crate::grpc::query_and_expect;
    use crate::test_util::{StorageType, setup_grpc_server};
    use crate::tests::test_util::MockInstance;

    struct SlowFlightCraft;

    struct RetainedFlightCraft;

    struct ErrorFlightCraft;

    struct SlowRemoteQueryHandler {
        region_requester: RegionRequester,
    }

    fn slow_recordbatch_stream() -> SendableRecordBatchStream {
        let schema = Arc::new(Schema::new(vec![ColumnSchema::new(
            "value",
            ConcreteDataType::int32_datatype(),
            false,
        )]));
        let recordbatch = RecordBatch::new(
            schema.clone(),
            vec![Arc::new(Int32Vector::from_vec(vec![1])) as VectorRef],
        )
        .unwrap();

        RecordBatches::try_new(schema, vec![recordbatch])
            .unwrap()
            .as_stream()
    }

    #[async_trait::async_trait]
    impl FlightCraft for SlowFlightCraft {
        async fn do_get(
            &self,
            _: tonic::Request<Ticket>,
        ) -> std::result::Result<Response<TonicStream<FlightData>>, tonic::Status> {
            let stream = FlightRecordBatchStream::new(
                FlightRecordBatchStreamInput::initializer(async {
                    tokio::time::sleep(Duration::from_secs(2)).await;
                    Ok(FlightRecordBatchSource::RecordBatches(
                        slow_recordbatch_stream(),
                    ))
                }),
                TracingContext::default(),
                FlightCompression::default(),
                session::context::QueryContext::arc(),
            );

            Ok(Response::new(Box::pin(stream)))
        }
    }

    #[async_trait::async_trait]
    impl FlightCraft for RetainedFlightCraft {
        async fn do_get(
            &self,
            _: tonic::Request<Ticket>,
        ) -> std::result::Result<Response<TonicStream<FlightData>>, tonic::Status> {
            let schema = Arc::new(Schema::new(vec![ColumnSchema::new(
                "value",
                ConcreteDataType::int32_datatype(),
                false,
            )]));
            let stream =
                futures_util::stream::pending::<common_recordbatch::error::Result<RecordBatch>>();
            let recordbatches = RecordBatchStreamWrapper::new(schema, stream);
            let stream = FlightRecordBatchStream::new(
                FlightRecordBatchStreamInput::ready(FlightRecordBatchSource::RecordBatches(
                    Box::pin(recordbatches),
                )),
                TracingContext::default(),
                FlightCompression::default(),
                session::context::QueryContext::arc(),
            );
            Ok(Response::new(Box::pin(stream)))
        }
    }

    #[async_trait::async_trait]
    impl FlightCraft for ErrorFlightCraft {
        async fn do_get(
            &self,
            _: tonic::Request<Ticket>,
        ) -> std::result::Result<Response<TonicStream<FlightData>>, tonic::Status> {
            let stream = FlightRecordBatchStream::new(
                FlightRecordBatchStreamInput::initializer(async {
                    Err(Status::internal("deferred initializer detail"))
                }),
                TracingContext::default(),
                FlightCompression::default(),
                session::context::QueryContext::arc(),
            );
            Ok(Response::new(Box::pin(stream)))
        }
    }

    #[async_trait::async_trait]
    impl GrpcQueryHandler for SlowRemoteQueryHandler {
        async fn do_query(
            &self,
            _: GreptimeQueryRequest,
            _: QueryContextRef,
        ) -> servers::error::Result<Output> {
            let stream = self
                .region_requester
                .do_get_inner(Ticket::default())
                .await
                .unwrap();
            Ok(Output::new_with_stream(stream))
        }

        fn handle_put_record_batch_stream(
            &self,
            _: PutRecordBatchRequestStream,
            _: QueryContextRef,
        ) -> Pin<Box<dyn Stream<Item = servers::error::Result<DoPutResponse>> + Send>> {
            Box::pin(futures::stream::empty())
        }
    }

    fn client_for_flight_craft<T>(addr: &'static str, craft: T) -> Client
    where
        T: FlightCraft,
    {
        client_for_flight_craft_with_max_encoding(addr, craft, None)
    }

    fn client_for_flight_craft_with_max_encoding<T>(
        addr: &'static str,
        craft: T,
        max_encoding_message_size: Option<usize>,
    ) -> Client
    where
        T: FlightCraft,
    {
        let (client_io, server_io) = tokio::io::duplex(1024);
        tokio::spawn(async move {
            let flight_service = FlightServiceServer::new(FlightCraftWrapper(craft));
            let flight_service = match max_encoding_message_size {
                Some(size) => flight_service.max_encoding_message_size(size),
                None => flight_service,
            };
            TonicServer::builder()
                .add_service(flight_service)
                .serve_with_incoming(futures::stream::iter(vec![Ok::<_, std::io::Error>(
                    server_io,
                )]))
                .await
                .unwrap();
        });

        let channel_manager = ChannelManager::with_config(ChannelConfig::new().timeout(None), None);
        let mut client_io = Some(client_io);
        channel_manager
            .reset_with_connector(
                addr,
                service_fn(move |_| {
                    let client_io = client_io.take();

                    async move {
                        client_io
                            .map(TokioIo::new)
                            .ok_or_else(|| std::io::Error::other("Client already taken"))
                    }
                }),
            )
            .unwrap();
        Client::with_manager_and_urls(channel_manager, [addr])
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_retained_flight_stream_uses_separate_control_connection() {
        // This models a cursor-like transport condition: a DoGet response whose first Flight
        // message is available while the rest remains active. The control lane must stay live
        // without relying on an unconditional production deadlock to reproduce the transport risk.
        let accepted_connections = Arc::new(AtomicUsize::new(0));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let server_accepted_connections = accepted_connections.clone();
        let incoming = TcpListenerStream::new(listener).map(move |result| {
            result.inspect(|_| {
                server_accepted_connections.fetch_add(1, Ordering::SeqCst);
            })
        });
        let mut server = tokio::spawn(async move {
            TonicServer::builder()
                .add_service(FlightServiceServer::new(FlightCraftWrapper(
                    RetainedFlightCraft,
                )))
                .add_service(HealthCheckServer::new(servers::grpc::HealthCheckHandler))
                .serve_with_incoming_shutdown(incoming, async {
                    let _ = shutdown_rx.await;
                })
                .await
                .unwrap();
        });

        let peer = Peer::new(1, addr.to_string());
        let node_clients = NodeClients::new(ChannelConfig::new().timeout(None));
        let client = node_clients.get_client(&peer).await;

        let mut flight_client = client.make_flight_client(false, false).unwrap();
        let mut retained_stream = flight_client
            .mut_inner()
            .do_get(tonic::Request::new(Ticket::default()))
            .await
            .unwrap()
            .into_inner();
        let first_data = retained_stream.message().await.unwrap().unwrap();
        let mut decoder = FlightDecoder::default();
        assert!(matches!(
            decoder.try_decode(&first_data).unwrap(),
            Some(FlightMessage::Schema(_))
        ));

        tokio::time::timeout(Duration::from_secs(2), client.health_check())
            .await
            .expect("control RPC should not wait for retained DoGet")
            .unwrap();
        assert_eq!(2, accepted_connections.load(Ordering::SeqCst));

        // A second DoGet and control RPC must reuse their respective physical connections.
        let mut second_flight_client = client.make_flight_client(false, false).unwrap();
        let mut second_stream = second_flight_client
            .mut_inner()
            .do_get(tonic::Request::new(Ticket::default()))
            .await
            .unwrap()
            .into_inner();
        let second_data = second_stream.message().await.unwrap().unwrap();
        let mut second_decoder = FlightDecoder::default();
        assert!(matches!(
            second_decoder.try_decode(&second_data).unwrap(),
            Some(FlightMessage::Schema(_))
        ));
        client.health_check().await.unwrap();
        assert_eq!(2, accepted_connections.load(Ordering::SeqCst));

        // Release the retained and secondary responses before gracefully stopping the real server.
        drop(retained_stream);
        drop(second_stream);
        drop(flight_client);
        drop(second_flight_client);
        drop(client);
        drop(node_clients);
        shutdown_tx.send(()).unwrap();
        let server_result = tokio::time::timeout(Duration::from_secs(2), &mut server).await;
        if server_result.is_err() {
            server.abort();
            let _ = server.await;
            panic!("Flight test server did not stop after stream release");
        }
        server_result.unwrap().unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_do_get_timeout_does_not_cancel_slow_flight_stream() {
        let client = client_for_flight_craft("slow-flight", SlowFlightCraft);
        let mut flight_client = client.make_flight_client(false, false).unwrap();

        let start = Instant::now();
        let mut request = tonic::Request::new(Ticket::default());
        request.set_timeout(Duration::from_secs(1));
        let response = flight_client.mut_inner().do_get(request).await.unwrap();
        assert!(start.elapsed() < Duration::from_secs(1));
        let mut stream = response.into_inner();

        let start = Instant::now();
        assert!(stream.message().await.unwrap().is_some());
        assert!(start.elapsed() >= Duration::from_secs(1));

        assert!(stream.message().await.unwrap().is_some());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_deferred_flight_initializer_error_preserves_message() {
        let client = client_for_flight_craft("error-flight", ErrorFlightCraft);
        let mut flight_client = client.make_flight_client(false, false).unwrap();
        let mut stream = flight_client
            .mut_inner()
            .do_get(tonic::Request::new(Ticket::default()))
            .await
            .unwrap()
            .into_inner();
        let error = stream.message().await.unwrap_err();
        assert_eq!(tonic::Code::Internal, error.code());
        assert_eq!("deferred initializer detail", error.message());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_deferred_flight_encode_error_preserves_message() {
        let client =
            client_for_flight_craft_with_max_encoding("limited-flight", SlowFlightCraft, Some(1));
        let mut flight_client = client.make_flight_client(false, false).unwrap();
        let mut stream = flight_client
            .mut_inner()
            .do_get(tonic::Request::new(Ticket::default()))
            .await
            .unwrap()
            .into_inner();
        let error = stream.message().await.unwrap_err();
        assert_eq!(tonic::Code::OutOfRange, error.code());
        assert!(error.message().contains("message length too large"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_flight_request_timeout_does_not_cancel_slow_datanode_stream() {
        let datanode_client = client_for_flight_craft("slow-datanode", SlowFlightCraft);
        let frontend_handler = GreptimeRequestHandler::new(
            Arc::new(SlowRemoteQueryHandler {
                region_requester: RegionRequester::new(datanode_client, false, false),
            }),
            None,
            None,
            FlightCompression::default(),
        );
        let frontend_client = client_for_flight_craft("slow-frontend", frontend_handler);
        let database = Database::new(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, frontend_client);

        let start = Instant::now();
        let output = database
            .flight_request()
            .with_timeout(Duration::from_secs(1))
            .sql("select 1")
            .await
            .unwrap();
        // `sql()` waits for the first Flight message. Succeeding after this delay
        // proves the request timeout applied only to the response header.
        assert!(start.elapsed() >= Duration::from_secs(1));
        assert!(matches!(output.data, OutputData::Stream(_)));
    }
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

        create_table_named(&client, "bar").await;
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
        assert!(result.metrics.is_ready());
        let region_watermark_map = result
            .region_watermark_map()
            .expect("standalone affected-rows output should carry terminal region watermarks");
        assert!(
            !region_watermark_map.is_empty(),
            "standalone affected-rows output should contain at least one region watermark"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_standalone_flight_do_put_missing_nullable_columns() {
        common_telemetry::init_default_ut_logging();

        let (db, server) = setup_grpc_server(
            StorageType::File,
            "test_standalone_flight_do_put_missing_nullable_columns",
        )
        .await;
        let addr = server.bind_addr().unwrap().to_string();

        let client = Client::with_urls(vec![addr]);
        let client = Database::new_with_dbname("greptime-public", client);

        create_table(&client).await;

        let record_batches = create_record_batches_without_nullable_column(1);
        test_put_record_batches(&client, record_batches).await;

        let sql = "select count(*) from foo where `B` is null";
        let expected = "\
+----------+
| count(*) |
+----------+
| 9        |
+----------+";
        query_and_expect(db.frontend().as_ref(), sql, expected).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_distributed_frontend_database_and_flight_health() {
        common_telemetry::init_default_ut_logging();

        let db =
            GreptimeDbClusterBuilder::new("test_distributed_frontend_database_and_flight_health")
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
            .database_handler(greptime_request_handler.clone())
            .flight_handler(Arc::new(greptime_request_handler))
            .build();
        grpc_server
            .start("127.0.0.1:0".parse::<SocketAddr>().unwrap())
            .await
            .unwrap();
        let addr = grpc_server.bind_addr().unwrap().to_string();

        let client = Client::with_urls(vec![addr]);
        wait_for_client_health(&client).await;
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
        assert!(terminal_metrics.is_ready());
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
        result.metrics.wait_ready().await;
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
        result.metrics.wait_ready().await;
        assert_eq!(
            result.region_watermark_map(),
            Some(std::collections::HashMap::from([previous_watermark]))
        );
    }

    async fn wait_for_client_health(client: &Client) {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if client.health_check().await.is_ok() {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("cluster frontend did not become healthy within 10 seconds");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_distributed_flight_snapshot_seqs_rejects_stale_sst_fence() {
        common_telemetry::init_default_ut_logging();

        let db = GreptimeDbClusterBuilder::new(
            "test_distributed_flight_snapshot_seqs_rejects_stale_sst_fence",
        )
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

        let client = Client::with_urls(vec![grpc_server.bind_addr().unwrap().to_string()]);
        let mut client = Database::new(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, client);
        client.set_auth(AuthScheme::Basic(Basic {
            username: "greptime_user".to_string(),
            password: "greptime_pwd".to_string(),
        }));

        create_table(&client).await;
        test_put_record_batches(&client, create_record_batches(1)).await;
        client.sql("admin flush_table('foo')").await.unwrap();

        let regions = db.list_all_regions().await;
        assert_eq!(regions.len(), 1);
        let (region_id, region) = regions.into_iter().next().unwrap();
        let stale_snapshot_seq = region.find_committed_sequence();

        test_put_record_batches(&client, create_record_batches(10)).await;
        client.sql("admin flush_table('foo')").await.unwrap();
        assert!(
            region.find_committed_sequence() > stale_snapshot_seq,
            "second flush should make the first snapshot sequence stale"
        );

        let result = client
            .flight_request()
            .with_flow_extensions(&[("flow.return_region_seq", "true")])
            .with_snapshot_seqs(&HashMap::from([(region_id.as_u64(), stale_snapshot_seq)]))
            .query_with_terminal_metrics(QueryRequest {
                query: Some(Query::Sql(
                    "select ts, a, `B` from foo order by ts".to_string(),
                )),
            })
            .await
            .unwrap();

        let OutputData::Stream(mut stream) = result.output.data else {
            panic!("expected stream output");
        };

        let mut stale_fence_error = None;
        while let Some(batch) = stream.next().await {
            if let Err(err) = batch {
                stale_fence_error = Some(format!("{err:?}"));
                break;
            }
        }

        let err_msg = stale_fence_error.expect("expected stale snapshot fence rejection");
        assert!(
            err_msg.contains("STALE_SNAPSHOT_FENCE")
                || err_msg.contains("RequestOutdated")
                || err_msg.contains("snapshot upper bound stale"),
            "expected stale snapshot fence rejection, got: {err_msg}"
        );
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

    fn create_record_batches_without_nullable_column(start: i64) -> Vec<RecordBatch> {
        let schema = Arc::new(Schema::new(vec![
            ColumnSchema::new(
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            )
            .with_time_index(true),
            ColumnSchema::new("a", ConcreteDataType::int32_datatype(), false),
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
                    ],
                )
                .unwrap(),
            );
        }
        record_batches
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
