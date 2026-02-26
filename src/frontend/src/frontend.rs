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

use std::sync::Arc;

use common_base::readable_size::ReadableSize;
use common_config::config::Configurable;
use common_event_recorder::EventRecorderOptions;
use common_memory_manager::OnExhaustedPolicy;
use common_options::datanode::DatanodeClientOptions;
use common_options::memory::MemoryOptions;
use common_telemetry::logging::{LoggingOptions, SlowQueryOptions, TracingOptions};
use meta_client::MetaClientOptions;
use query::options::QueryOptions;
use serde::{Deserialize, Serialize};
use servers::grpc::GrpcOptions;
use servers::http::HttpOptions;
use servers::server::ServerHandlers;
use snafu::ResultExt;

use crate::error;
use crate::error::Result;
use crate::heartbeat::HeartbeatTask;
use crate::instance::Instance;
use crate::service_config::{
    InfluxdbOptions, JaegerOptions, MysqlOptions, OpentsdbOptions, OtlpOptions, PostgresOptions,
    PromStoreOptions,
};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct FrontendOptions {
    pub node_id: Option<String>,
    pub default_timezone: Option<String>,
    pub default_column_prefix: Option<String>,
    /// Maximum total memory for all concurrent write request bodies and messages (HTTP, gRPC, Flight).
    /// Set to 0 to disable the limit. Default: "0" (unlimited)
    pub max_in_flight_write_bytes: ReadableSize,
    /// Policy when write bytes quota is exhausted.
    /// Options: "wait" (default, 10s), "wait(<duration>)", "fail"
    pub write_bytes_exhausted_policy: OnExhaustedPolicy,
    pub http: HttpOptions,
    pub grpc: GrpcOptions,
    /// The internal gRPC options for the frontend service.
    /// it provide the same service as the public gRPC service, just only for internal use.
    pub internal_grpc: Option<GrpcOptions>,
    pub mysql: MysqlOptions,
    pub postgres: PostgresOptions,
    pub opentsdb: OpentsdbOptions,
    pub influxdb: InfluxdbOptions,
    pub prom_store: PromStoreOptions,
    pub jaeger: JaegerOptions,
    pub otlp: OtlpOptions,
    pub meta_client: Option<MetaClientOptions>,
    pub logging: LoggingOptions,
    pub datanode: DatanodeClientOptions,
    pub user_provider: Option<String>,
    pub tracing: TracingOptions,
    pub query: QueryOptions,
    pub slow_query: SlowQueryOptions,
    pub memory: MemoryOptions,
    /// The event recorder options.
    pub event_recorder: EventRecorderOptions,
}

impl Default for FrontendOptions {
    fn default() -> Self {
        Self {
            node_id: None,
            default_timezone: None,
            default_column_prefix: None,
            max_in_flight_write_bytes: ReadableSize(0),
            write_bytes_exhausted_policy: OnExhaustedPolicy::default(),
            http: HttpOptions::default(),
            grpc: GrpcOptions::default(),
            internal_grpc: None,
            mysql: MysqlOptions::default(),
            postgres: PostgresOptions::default(),
            opentsdb: OpentsdbOptions::default(),
            influxdb: InfluxdbOptions::default(),
            jaeger: JaegerOptions::default(),
            prom_store: PromStoreOptions::default(),
            otlp: OtlpOptions::default(),
            meta_client: None,
            logging: LoggingOptions::default(),
            datanode: DatanodeClientOptions::default(),
            user_provider: None,
            tracing: TracingOptions::default(),
            query: QueryOptions::default(),
            slow_query: SlowQueryOptions::default(),
            memory: MemoryOptions::default(),
            event_recorder: EventRecorderOptions::default(),
        }
    }
}

impl Configurable for FrontendOptions {
    fn env_list_keys() -> Option<&'static [&'static str]> {
        Some(&["meta_client.metasrv_addrs"])
    }
}

/// The [`Frontend`] struct is the main entry point for the frontend service
/// which contains server handlers, frontend instance and some background tasks.
pub struct Frontend {
    pub instance: Arc<Instance>,
    pub servers: ServerHandlers,
    pub heartbeat_task: Option<HeartbeatTask>,
}

impl Frontend {
    pub async fn start(&mut self) -> Result<()> {
        if let Some(t) = &self.heartbeat_task {
            t.start().await?;
        }

        self.servers
            .start_all()
            .await
            .context(error::StartServerSnafu)
    }

    pub async fn shutdown(&mut self) -> Result<()> {
        self.servers
            .shutdown_all()
            .await
            .context(error::ShutdownServerSnafu)
    }

    pub fn server_handlers(&self) -> &ServerHandlers {
        &self.servers
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    use api::v1::meta::heartbeat_server::HeartbeatServer;
    use api::v1::meta::mailbox_message::Payload;
    use api::v1::meta::{
        AskLeaderRequest, AskLeaderResponse, HeartbeatRequest, HeartbeatResponse, MailboxMessage,
        Peer, PullMetaConfigRequest, PullMetaConfigResponse, ResponseHeader, Role,
        heartbeat_server,
    };
    use async_trait::async_trait;
    use client::{Client, Database};
    use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
    use common_error::ext::ErrorExt;
    use common_error::from_header_to_err_code_msg;
    use common_error::status_code::StatusCode;
    use common_grpc::channel_manager::ChannelManager;
    use common_meta::heartbeat::handler::HandlerGroupExecutor;
    use common_meta::heartbeat::handler::parse_mailbox_message::ParseMailboxMessageHandler;
    use common_meta::heartbeat::handler::suspend::SuspendHandler;
    use common_meta::instruction::Instruction;
    use common_stat::ResourceStatImpl;
    use meta_client::MetaClientRef;
    use meta_client::client::MetaClientBuilder;
    use meta_srv::service::GrpcStream;
    use servers::grpc::{FlightCompression, GRPC_SERVER};
    use servers::http::HTTP_SERVER;
    use servers::http::result::greptime_result_v1::GreptimedbV1Response;
    use tokio::sync::mpsc;
    use tonic::codec::CompressionEncoding;
    use tonic::codegen::tokio_stream::StreamExt;
    use tonic::codegen::tokio_stream::wrappers::ReceiverStream;
    use tonic::{Request, Response, Status, Streaming};

    use super::*;
    use crate::instance::builder::FrontendBuilder;
    use crate::server::Services;

    #[test]
    fn test_toml() {
        let opts = FrontendOptions::default();
        let toml_string = toml::to_string(&opts).unwrap();
        let _parsed: FrontendOptions = toml::from_str(&toml_string).unwrap();
    }

    struct SuspendableHeartbeatServer {
        suspend: Arc<AtomicBool>,
    }

    #[async_trait]
    impl heartbeat_server::Heartbeat for SuspendableHeartbeatServer {
        type HeartbeatStream = GrpcStream<HeartbeatResponse>;

        async fn heartbeat(
            &self,
            request: Request<Streaming<HeartbeatRequest>>,
        ) -> std::result::Result<Response<Self::HeartbeatStream>, Status> {
            let (tx, rx) = mpsc::channel(4);

            common_runtime::spawn_global({
                let mut requests = request.into_inner();
                let suspend = self.suspend.clone();
                async move {
                    // Make the heartbeat interval short in unit tests to reduce the waiting time.
                    // Only the handshake response needs to populate it (as metasrv does).
                    let heartbeat_interval_ms = Duration::from_millis(200).as_millis() as u64;
                    let mut is_handshake = true;
                    while let Some(request) = requests.next().await {
                        if let Err(e) = request {
                            let _ = tx.send(Err(e)).await;
                            return;
                        }

                        let mailbox_message =
                            suspend.load(Ordering::Relaxed).then(|| MailboxMessage {
                                payload: Some(Payload::Json(
                                    serde_json::to_string(&Instruction::Suspend).unwrap(),
                                )),
                                ..Default::default()
                            });
                        let heartbeat_config =
                            is_handshake.then_some(api::v1::meta::HeartbeatConfig {
                                heartbeat_interval_ms,
                                retry_interval_ms: heartbeat_interval_ms,
                            });
                        is_handshake = false;
                        let response = HeartbeatResponse {
                            header: Some(ResponseHeader::success()),
                            mailbox_message,
                            heartbeat_config,
                            ..Default::default()
                        };

                        let _ = tx.send(Ok(response)).await;
                    }
                }
            });

            Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
        }

        async fn ask_leader(
            &self,
            _: Request<AskLeaderRequest>,
        ) -> std::result::Result<Response<AskLeaderResponse>, Status> {
            Ok(Response::new(AskLeaderResponse {
                header: Some(ResponseHeader::success()),
                leader: Some(Peer {
                    addr: "localhost:0".to_string(),
                    ..Default::default()
                }),
            }))
        }

        async fn pull_meta_config(
            &self,
            _: Request<PullMetaConfigRequest>,
        ) -> std::result::Result<Response<PullMetaConfigResponse>, Status> {
            let res = PullMetaConfigResponse {
                header: Some(ResponseHeader::success()),
                payload: None,
            };

            Ok(Response::new(res))
        }
    }

    async fn create_meta_client(
        options: &MetaClientOptions,
        heartbeat_server: Arc<SuspendableHeartbeatServer>,
    ) -> MetaClientRef {
        let (client, server) = tokio::io::duplex(1024);

        // create the heartbeat server:
        common_runtime::spawn_global(async move {
            let mut router = tonic::transport::Server::builder();
            let router = router.add_service(
                HeartbeatServer::from_arc(heartbeat_server)
                    .accept_compressed(CompressionEncoding::Zstd)
                    .send_compressed(CompressionEncoding::Zstd),
            );
            router
                .serve_with_incoming(futures::stream::iter([Ok::<_, std::io::Error>(server)]))
                .await
        });

        // Move client to an option so we can _move_ the inner value
        // on the first attempt to connect. All other attempts will fail.
        let mut client = Some(client);
        let connector = tower::service_fn(move |_| {
            let client = client.take();
            async move {
                if let Some(client) = client {
                    Ok(hyper_util::rt::TokioIo::new(client))
                } else {
                    Err(std::io::Error::other("client already taken"))
                }
            }
        });
        let manager = ChannelManager::new();
        manager
            .reset_with_connector("localhost:0", connector)
            .unwrap();

        // create the heartbeat client:
        let mut client = MetaClientBuilder::new(0, Role::Frontend)
            .enable_heartbeat()
            .heartbeat_channel_manager(manager)
            .build();
        client.start(&options.metasrv_addrs).await.unwrap();
        Arc::new(client)
    }

    async fn create_frontend(
        options: &FrontendOptions,
        meta_client: MetaClientRef,
    ) -> Result<Frontend> {
        let instance = Arc::new(
            FrontendBuilder::new_test(options, meta_client.clone())
                .try_build()
                .await?,
        );

        let servers =
            Services::new(options.clone(), instance.clone(), Default::default()).build()?;

        let executor = Arc::new(HandlerGroupExecutor::new(vec![
            Arc::new(ParseMailboxMessageHandler),
            Arc::new(SuspendHandler::new(instance.suspend_state())),
        ]));
        let heartbeat_task = Some(HeartbeatTask::new(
            options,
            meta_client,
            executor,
            Arc::new(ResourceStatImpl::default()),
        ));

        let mut frontend = Frontend {
            instance,
            servers,
            heartbeat_task,
        };
        frontend.start().await?;
        Ok(frontend)
    }

    async fn verify_suspend_state_by_http(
        frontend: &Frontend,
        expected: std::result::Result<&str, (StatusCode, &str)>,
    ) {
        let addr = frontend.server_handlers().addr(HTTP_SERVER).unwrap();
        let response = reqwest::get(format!("http://{}/v1/sql?sql=SELECT 1", addr))
            .await
            .unwrap();

        let headers = response.headers();
        let response = if let Some((code, error)) = from_header_to_err_code_msg(headers) {
            Err((code, error))
        } else {
            Ok(response.text().await.unwrap())
        };

        match (response, expected) {
            (Ok(response), Ok(expected)) => {
                let response: GreptimedbV1Response = serde_json::from_str(&response).unwrap();
                let response = serde_json::to_string(response.output()).unwrap();
                assert_eq!(&response, expected);
            }
            (Err(actual), Err(expected)) => assert_eq!(actual, expected),
            _ => unreachable!(),
        }
    }

    async fn verify_suspend_state_by_grpc(
        frontend: &Frontend,
        expected: std::result::Result<&str, (StatusCode, &str)>,
    ) {
        let addr = frontend.server_handlers().addr(GRPC_SERVER).unwrap();
        let client = Client::with_urls([addr.to_string()]);
        let client = Database::new(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, client);
        let response = client.sql("SELECT 1").await;

        match (response, expected) {
            (Ok(response), Ok(expected)) => {
                let response = response.data.pretty_print().await;
                assert_eq!(&response, expected.trim());
            }
            (Err(actual), Err(expected)) => {
                assert_eq!(actual.status_code(), expected.0);
                assert_eq!(actual.output_msg(), expected.1);
            }
            _ => unreachable!(),
        }
    }

    async fn wait_for_suspend_state(frontend: &Frontend, expected: bool) {
        let check = || frontend.instance.is_suspended() == expected;
        if check() {
            return;
        }

        tokio::time::timeout(Duration::from_secs(5), async move {
            while !check() {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_suspend_frontend() -> Result<()> {
        common_telemetry::init_default_ut_logging();

        let meta_client_options = MetaClientOptions {
            metasrv_addrs: vec!["localhost:0".to_string()],
            ..Default::default()
        };
        let options = FrontendOptions {
            http: HttpOptions {
                addr: "127.0.0.1:0".to_string(),
                ..Default::default()
            },
            grpc: GrpcOptions {
                bind_addr: "127.0.0.1:0".to_string(),
                flight_compression: FlightCompression::None,
                ..Default::default()
            },
            mysql: MysqlOptions {
                enable: false,
                ..Default::default()
            },
            postgres: PostgresOptions {
                enable: false,
                ..Default::default()
            },
            meta_client: Some(meta_client_options.clone()),
            ..Default::default()
        };

        let server = Arc::new(SuspendableHeartbeatServer {
            suspend: Arc::new(AtomicBool::new(false)),
        });
        let meta_client = create_meta_client(&meta_client_options, server.clone()).await;
        let frontend = create_frontend(&options, meta_client).await?;

        // initial state: not suspend:
        assert!(!frontend.instance.is_suspended());
        verify_suspend_state_by_http(&frontend, Ok(r#"[{"records":{"schema":{"column_schemas":[{"name":"Int64(1)","data_type":"Int64"}]},"rows":[[1]],"total_rows":1}}]"#)).await;
        verify_suspend_state_by_grpc(
            &frontend,
            Ok(r#"
+----------+
| Int64(1) |
+----------+
| 1        |
+----------+"#),
        )
        .await;

        // make heartbeat server returned "suspend" instruction,
        server.suspend.store(true, Ordering::Relaxed);
        wait_for_suspend_state(&frontend, true).await;
        // ... then the frontend is suspended:
        assert!(frontend.instance.is_suspended());
        verify_suspend_state_by_http(
            &frontend,
            Err((
                StatusCode::Suspended,
                "error: Service suspended, execution_time_ms: 0",
            )),
        )
        .await;
        verify_suspend_state_by_grpc(&frontend, Err((StatusCode::Suspended, "Service suspended")))
            .await;

        // make heartbeat server NOT returned "suspend" instruction,
        server.suspend.store(false, Ordering::Relaxed);
        wait_for_suspend_state(&frontend, false).await;
        // ... then frontend's suspend state is cleared:
        assert!(!frontend.instance.is_suspended());
        verify_suspend_state_by_http(&frontend, Ok(r#"[{"records":{"schema":{"column_schemas":[{"name":"Int64(1)","data_type":"Int64"}]},"rows":[[1]],"total_rows":1}}]"#)).await;
        verify_suspend_state_by_grpc(
            &frontend,
            Ok(r#"
+----------+
| Int64(1) |
+----------+
| 1        |
+----------+"#),
        )
        .await;
        Ok(())
    }
}
