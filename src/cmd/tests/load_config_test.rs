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

use std::time::Duration;

use cmd::options::GreptimeOptions;
use cmd::standalone::StandaloneOptions;
use common_config::Configurable;
use common_grpc::channel_manager::{
    DEFAULT_MAX_GRPC_RECV_MESSAGE_SIZE, DEFAULT_MAX_GRPC_SEND_MESSAGE_SIZE,
};
use common_telemetry::logging::{LoggingOptions, DEFAULT_OTLP_ENDPOINT};
use common_wal::config::raft_engine::RaftEngineConfig;
use common_wal::config::DatanodeWalConfig;
use datanode::config::{DatanodeOptions, RegionEngineConfig, StorageConfig};
use file_engine::config::EngineConfig;
use frontend::frontend::FrontendOptions;
use frontend::service_config::datanode::DatanodeClientOptions;
use meta_client::MetaClientOptions;
use meta_srv::metasrv::MetasrvOptions;
use meta_srv::selector::SelectorType;
use mito2::config::MitoConfig;
use servers::export_metrics::ExportMetricsOption;
use servers::grpc::GrpcOptions;

#[allow(deprecated)]
#[test]
fn test_load_datanode_example_config() {
    let example_config = common_test_util::find_workspace_path("config/datanode.example.toml");
    let options =
        GreptimeOptions::<DatanodeOptions>::load_layered_options(example_config.to_str(), "")
            .unwrap();

    let expected = GreptimeOptions::<DatanodeOptions> {
        component: DatanodeOptions {
            node_id: Some(42),
            meta_client: Some(MetaClientOptions {
                metasrv_addrs: vec!["127.0.0.1:3002".to_string()],
                timeout: Duration::from_secs(3),
                heartbeat_timeout: Duration::from_millis(500),
                ddl_timeout: Duration::from_secs(10),
                connect_timeout: Duration::from_secs(1),
                tcp_nodelay: true,
                metadata_cache_max_capacity: 100000,
                metadata_cache_ttl: Duration::from_secs(600),
                metadata_cache_tti: Duration::from_secs(300),
            }),
            wal: DatanodeWalConfig::RaftEngine(RaftEngineConfig {
                dir: Some("/tmp/greptimedb/wal".to_string()),
                sync_period: Some(Duration::from_secs(10)),
                recovery_parallelism: 2,
                ..Default::default()
            }),
            storage: StorageConfig {
                data_home: "/tmp/greptimedb/".to_string(),
                ..Default::default()
            },
            region_engine: vec![
                RegionEngineConfig::Mito(MitoConfig {
                    auto_flush_interval: Duration::from_secs(3600),
                    scan_parallelism: 0,
                    experimental_write_cache_ttl: Some(Duration::from_secs(60 * 60 * 8)),
                    ..Default::default()
                }),
                RegionEngineConfig::File(EngineConfig {}),
            ],
            logging: LoggingOptions {
                level: Some("info".to_string()),
                otlp_endpoint: Some(DEFAULT_OTLP_ENDPOINT.to_string()),
                tracing_sample_ratio: Some(Default::default()),
                ..Default::default()
            },
            export_metrics: ExportMetricsOption {
                self_import: Some(Default::default()),
                remote_write: Some(Default::default()),
                ..Default::default()
            },
            grpc: GrpcOptions::default().with_addr("127.0.0.1:3001"),
            rpc_addr: Some("127.0.0.1:3001".to_string()),
            rpc_hostname: Some("127.0.0.1".to_string()),
            rpc_runtime_size: Some(8),
            rpc_max_recv_message_size: Some(DEFAULT_MAX_GRPC_RECV_MESSAGE_SIZE),
            rpc_max_send_message_size: Some(DEFAULT_MAX_GRPC_SEND_MESSAGE_SIZE),
            ..Default::default()
        },
        ..Default::default()
    };

    similar_asserts::assert_eq!(options, expected);
}

#[test]
fn test_load_frontend_example_config() {
    let example_config = common_test_util::find_workspace_path("config/frontend.example.toml");
    let options =
        GreptimeOptions::<FrontendOptions>::load_layered_options(example_config.to_str(), "")
            .unwrap();
    let expected = GreptimeOptions::<FrontendOptions> {
        component: FrontendOptions {
            default_timezone: Some("UTC".to_string()),
            meta_client: Some(MetaClientOptions {
                metasrv_addrs: vec!["127.0.0.1:3002".to_string()],
                timeout: Duration::from_secs(3),
                heartbeat_timeout: Duration::from_millis(500),
                ddl_timeout: Duration::from_secs(10),
                connect_timeout: Duration::from_secs(1),
                tcp_nodelay: true,
                metadata_cache_max_capacity: 100000,
                metadata_cache_ttl: Duration::from_secs(600),
                metadata_cache_tti: Duration::from_secs(300),
            }),
            logging: LoggingOptions {
                level: Some("info".to_string()),
                otlp_endpoint: Some(DEFAULT_OTLP_ENDPOINT.to_string()),
                tracing_sample_ratio: Some(Default::default()),
                ..Default::default()
            },
            datanode: frontend::service_config::DatanodeOptions {
                client: DatanodeClientOptions {
                    connect_timeout: Duration::from_secs(10),
                    tcp_nodelay: true,
                },
            },
            export_metrics: ExportMetricsOption {
                self_import: Some(Default::default()),
                remote_write: Some(Default::default()),
                ..Default::default()
            },
            ..Default::default()
        },
        ..Default::default()
    };
    similar_asserts::assert_eq!(options, expected);
}

#[test]
fn test_load_metasrv_example_config() {
    let example_config = common_test_util::find_workspace_path("config/metasrv.example.toml");
    let options =
        GreptimeOptions::<MetasrvOptions>::load_layered_options(example_config.to_str(), "")
            .unwrap();
    let expected = GreptimeOptions::<MetasrvOptions> {
        component: MetasrvOptions {
            selector: SelectorType::default(),
            data_home: "/tmp/metasrv/".to_string(),
            logging: LoggingOptions {
                dir: "/tmp/greptimedb/logs".to_string(),
                level: Some("info".to_string()),
                otlp_endpoint: Some(DEFAULT_OTLP_ENDPOINT.to_string()),
                tracing_sample_ratio: Some(Default::default()),
                ..Default::default()
            },
            export_metrics: ExportMetricsOption {
                self_import: Some(Default::default()),
                remote_write: Some(Default::default()),
                ..Default::default()
            },
            ..Default::default()
        },
        ..Default::default()
    };
    similar_asserts::assert_eq!(options, expected);
}

#[test]
fn test_load_standalone_example_config() {
    let example_config = common_test_util::find_workspace_path("config/standalone.example.toml");
    let options =
        GreptimeOptions::<StandaloneOptions>::load_layered_options(example_config.to_str(), "")
            .unwrap();
    let expected = GreptimeOptions::<StandaloneOptions> {
        component: StandaloneOptions {
            default_timezone: Some("UTC".to_string()),
            wal: DatanodeWalConfig::RaftEngine(RaftEngineConfig {
                dir: Some("/tmp/greptimedb/wal".to_string()),
                sync_period: Some(Duration::from_secs(10)),
                recovery_parallelism: 2,
                ..Default::default()
            }),
            region_engine: vec![
                RegionEngineConfig::Mito(MitoConfig {
                    auto_flush_interval: Duration::from_secs(3600),
                    experimental_write_cache_ttl: Some(Duration::from_secs(60 * 60 * 8)),
                    scan_parallelism: 0,
                    ..Default::default()
                }),
                RegionEngineConfig::File(EngineConfig {}),
            ],
            storage: StorageConfig {
                data_home: "/tmp/greptimedb/".to_string(),
                ..Default::default()
            },
            logging: LoggingOptions {
                level: Some("info".to_string()),
                otlp_endpoint: Some(DEFAULT_OTLP_ENDPOINT.to_string()),
                tracing_sample_ratio: Some(Default::default()),
                ..Default::default()
            },
            export_metrics: ExportMetricsOption {
                self_import: Some(Default::default()),
                remote_write: Some(Default::default()),
                ..Default::default()
            },
            ..Default::default()
        },
        ..Default::default()
    };
    similar_asserts::assert_eq!(options, expected);
}
