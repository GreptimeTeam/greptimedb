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
use common_base::memory_limit::MemoryLimit;
use common_config::{Configurable, DEFAULT_DATA_HOME};
use common_options::datanode::{ClientOptions, DatanodeClientOptions};
use common_telemetry::logging::{DEFAULT_LOGGING_DIR, DEFAULT_OTLP_HTTP_ENDPOINT, LoggingOptions};
use common_wal::config::DatanodeWalConfig;
use common_wal::config::raft_engine::RaftEngineConfig;
use datanode::config::{DatanodeOptions, RegionEngineConfig, StorageConfig};
use file_engine::config::EngineConfig as FileEngineConfig;
use flow::FlownodeOptions;
use frontend::frontend::FrontendOptions;
use meta_client::MetaClientOptions;
use meta_srv::metasrv::MetasrvOptions;
use meta_srv::selector::SelectorType;
use metric_engine::config::EngineConfig as MetricEngineConfig;
use mito2::config::MitoConfig;
use query::options::QueryOptions;
use servers::grpc::GrpcOptions;
use servers::http::HttpOptions;
use servers::tls::{TlsMode, TlsOption};
use standalone::options::StandaloneOptions;
use store_api::path_utils::WAL_DIR;

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
            default_column_prefix: Some("greptime".to_string()),
            meta_client: Some(MetaClientOptions {
                metasrv_addrs: vec!["127.0.0.1:3002".to_string()],
                timeout: Duration::from_secs(3),
                ddl_timeout: Duration::from_secs(10),
                connect_timeout: Duration::from_secs(1),
                tcp_nodelay: true,
                metadata_cache_max_capacity: 100000,
                metadata_cache_ttl: Duration::from_secs(600),
                metadata_cache_tti: Duration::from_secs(300),
            }),
            wal: DatanodeWalConfig::RaftEngine(RaftEngineConfig {
                dir: Some(format!("{}/{}", DEFAULT_DATA_HOME, WAL_DIR)),
                sync_period: Some(Duration::from_secs(10)),
                recovery_parallelism: 2,
                ..Default::default()
            }),
            storage: StorageConfig {
                data_home: DEFAULT_DATA_HOME.to_string(),
                ..Default::default()
            },
            region_engine: vec![
                RegionEngineConfig::Mito(MitoConfig {
                    auto_flush_interval: Duration::from_secs(3600),
                    write_cache_ttl: Some(Duration::from_secs(60 * 60 * 8)),
                    scan_memory_limit: MemoryLimit::Percentage(50),
                    ..Default::default()
                }),
                RegionEngineConfig::File(FileEngineConfig {}),
                RegionEngineConfig::Metric(MetricEngineConfig {
                    sparse_primary_key_encoding: true,
                    flush_metadata_region_interval: Duration::from_secs(30),
                }),
            ],
            query: QueryOptions {
                memory_pool_size: MemoryLimit::Percentage(50),
                ..Default::default()
            },
            logging: LoggingOptions {
                level: Some("info".to_string()),
                dir: format!("{}/{}", DEFAULT_DATA_HOME, DEFAULT_LOGGING_DIR),
                otlp_endpoint: Some(DEFAULT_OTLP_HTTP_ENDPOINT.to_string()),
                tracing_sample_ratio: Some(Default::default()),
                ..Default::default()
            },
            grpc: GrpcOptions::default()
                .with_bind_addr("127.0.0.1:3001")
                .with_server_addr("127.0.0.1:3001"),
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
            default_column_prefix: Some("greptime".to_string()),
            meta_client: Some(MetaClientOptions {
                metasrv_addrs: vec!["127.0.0.1:3002".to_string()],
                timeout: Duration::from_secs(3),
                ddl_timeout: Duration::from_secs(10),
                connect_timeout: Duration::from_secs(1),
                tcp_nodelay: true,
                metadata_cache_max_capacity: 100000,
                metadata_cache_ttl: Duration::from_secs(600),
                metadata_cache_tti: Duration::from_secs(300),
            }),
            logging: LoggingOptions {
                level: Some("info".to_string()),
                dir: format!("{}/{}", DEFAULT_DATA_HOME, DEFAULT_LOGGING_DIR),
                otlp_endpoint: Some(DEFAULT_OTLP_HTTP_ENDPOINT.to_string()),
                tracing_sample_ratio: Some(Default::default()),
                ..Default::default()
            },
            datanode: DatanodeClientOptions {
                client: ClientOptions {
                    connect_timeout: Duration::from_secs(10),
                    tcp_nodelay: true,
                    ..Default::default()
                },
            },
            grpc: GrpcOptions {
                bind_addr: "127.0.0.1:4001".to_string(),
                server_addr: "127.0.0.1:4001".to_string(),
                ..Default::default()
            },
            internal_grpc: Some(GrpcOptions::internal_default()),
            http: HttpOptions {
                cors_allowed_origins: vec!["https://example.com".to_string()],
                ..Default::default()
            },
            query: QueryOptions {
                memory_pool_size: MemoryLimit::Percentage(50),
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
            data_home: DEFAULT_DATA_HOME.to_string(),
            grpc: GrpcOptions {
                bind_addr: "127.0.0.1:3002".to_string(),
                server_addr: "127.0.0.1:3002".to_string(),
                ..Default::default()
            },
            logging: LoggingOptions {
                dir: format!("{}/{}", DEFAULT_DATA_HOME, DEFAULT_LOGGING_DIR),
                level: Some("info".to_string()),
                otlp_endpoint: Some(DEFAULT_OTLP_HTTP_ENDPOINT.to_string()),
                tracing_sample_ratio: Some(Default::default()),
                ..Default::default()
            },
            datanode: DatanodeClientOptions {
                client: ClientOptions {
                    timeout: Duration::from_secs(10),
                    connect_timeout: Duration::from_secs(10),
                    tcp_nodelay: true,
                },
            },
            backend_tls: Some(TlsOption {
                mode: TlsMode::Prefer,
                cert_path: String::new(),
                key_path: String::new(),
                ca_cert_path: String::new(),
                watch: false,
            }),
            meta_schema_name: Some("greptime_schema".to_string()),
            ..Default::default()
        },
        ..Default::default()
    };
    similar_asserts::assert_eq!(options, expected);
}

#[test]
fn test_load_flownode_example_config() {
    let example_config = common_test_util::find_workspace_path("config/flownode.example.toml");
    let options =
        GreptimeOptions::<FlownodeOptions>::load_layered_options(example_config.to_str(), "")
            .unwrap();
    let expected = GreptimeOptions::<FlownodeOptions> {
        component: FlownodeOptions {
            node_id: Some(14),
            flow: Default::default(),
            grpc: GrpcOptions {
                bind_addr: "127.0.0.1:6800".to_string(),
                server_addr: "127.0.0.1:6800".to_string(),
                runtime_size: 2,
                ..Default::default()
            },
            logging: LoggingOptions {
                dir: format!("{}/{}", DEFAULT_DATA_HOME, DEFAULT_LOGGING_DIR),
                level: Some("info".to_string()),
                otlp_endpoint: Some(DEFAULT_OTLP_HTTP_ENDPOINT.to_string()),
                otlp_export_protocol: Some(common_telemetry::logging::OtlpExportProtocol::Http),
                tracing_sample_ratio: Some(Default::default()),
                ..Default::default()
            },
            tracing: Default::default(),
            // flownode deliberately use a slower query parallelism
            // to avoid overwhelming the frontend with too many queries
            query: QueryOptions {
                parallelism: 1,
                allow_query_fallback: false,
                memory_pool_size: MemoryLimit::Percentage(50),
                vector_topk_max_rounds: 8,
                vector_topk_max_k: None,
            },
            meta_client: Some(MetaClientOptions {
                metasrv_addrs: vec!["127.0.0.1:3002".to_string()],
                timeout: Duration::from_secs(3),
                ddl_timeout: Duration::from_secs(10),
                connect_timeout: Duration::from_secs(1),
                tcp_nodelay: true,
                metadata_cache_max_capacity: 100000,
                metadata_cache_ttl: Duration::from_secs(600),
                metadata_cache_tti: Duration::from_secs(300),
            }),
            http: HttpOptions {
                addr: "127.0.0.1:4000".to_string(),
                ..Default::default()
            },
            user_provider: None,
            memory: Default::default(),
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
            default_column_prefix: Some("greptime".to_string()),
            wal: DatanodeWalConfig::RaftEngine(RaftEngineConfig {
                dir: Some(format!("{}/{}", DEFAULT_DATA_HOME, WAL_DIR)),
                sync_period: Some(Duration::from_secs(10)),
                recovery_parallelism: 2,
                ..Default::default()
            }),
            region_engine: vec![
                RegionEngineConfig::Mito(MitoConfig {
                    auto_flush_interval: Duration::from_secs(3600),
                    write_cache_ttl: Some(Duration::from_secs(60 * 60 * 8)),
                    scan_memory_limit: MemoryLimit::Percentage(50),
                    ..Default::default()
                }),
                RegionEngineConfig::File(FileEngineConfig {}),
                RegionEngineConfig::Metric(MetricEngineConfig {
                    sparse_primary_key_encoding: true,
                    flush_metadata_region_interval: Duration::from_secs(30),
                }),
            ],
            storage: StorageConfig {
                data_home: DEFAULT_DATA_HOME.to_string(),
                ..Default::default()
            },
            logging: LoggingOptions {
                level: Some("info".to_string()),
                dir: format!("{}/{}", DEFAULT_DATA_HOME, DEFAULT_LOGGING_DIR),
                otlp_endpoint: Some(DEFAULT_OTLP_HTTP_ENDPOINT.to_string()),
                tracing_sample_ratio: Some(Default::default()),
                ..Default::default()
            },
            http: HttpOptions {
                cors_allowed_origins: vec!["https://example.com".to_string()],
                ..Default::default()
            },
            query: QueryOptions {
                memory_pool_size: MemoryLimit::Percentage(50),
                ..Default::default()
            },
            ..Default::default()
        },
        ..Default::default()
    };
    similar_asserts::assert_eq!(options, expected);
}
