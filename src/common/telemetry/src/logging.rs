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

//! logging stuffs, inspired by databend
use std::env;
use std::io::IsTerminal;
use std::sync::{Arc, Mutex, Once};
use std::time::Duration;

use common_base::serde::empty_string_as_default;
use once_cell::sync::{Lazy, OnceCell};
use opentelemetry::{global, KeyValue};
use opentelemetry_otlp::{Protocol, SpanExporterBuilder, WithExportConfig};
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::Sampler;
use opentelemetry_semantic_conventions::resource;
use serde::{Deserialize, Serialize};
use tracing_appender::non_blocking::WorkerGuard;
use tracing_appender::rolling::{RollingFileAppender, Rotation};
use tracing_log::LogTracer;
use tracing_subscriber::filter::{FilterFn, Targets};
use tracing_subscriber::fmt::Layer;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::prelude::*;
use tracing_subscriber::{filter, EnvFilter, Registry};

use crate::tracing_sampler::{create_sampler, TracingSampleOptions};

/// The default endpoint when use gRPC exporter protocol.
pub const DEFAULT_OTLP_GRPC_ENDPOINT: &str = "http://localhost:4317";

/// The default endpoint when use HTTP exporter protocol.
pub const DEFAULT_OTLP_HTTP_ENDPOINT: &str = "http://localhost:4318";

/// The default logs directory.
pub const DEFAULT_LOGGING_DIR: &str = "logs";

// Handle for reloading log level
pub static RELOAD_HANDLE: OnceCell<tracing_subscriber::reload::Handle<Targets, Registry>> =
    OnceCell::new();

/// The logging options that used to initialize the logger.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct LoggingOptions {
    /// The directory to store log files. If not set, logs will be written to stdout.
    pub dir: String,

    /// The log level that can be one of "trace", "debug", "info", "warn", "error". Default is "info".
    pub level: Option<String>,

    /// The log format that can be one of "json" or "text". Default is "text".
    #[serde(default, deserialize_with = "empty_string_as_default")]
    pub log_format: LogFormat,

    /// The maximum number of log files set by default.
    pub max_log_files: usize,

    /// Whether to append logs to stdout. Default is true.
    pub append_stdout: bool,

    /// Whether to enable tracing with OTLP. Default is false.
    pub enable_otlp_tracing: bool,

    /// The endpoint of OTLP. Default is "http://localhost:4318".
    pub otlp_endpoint: Option<String>,

    /// The tracing sample ratio.
    pub tracing_sample_ratio: Option<TracingSampleOptions>,

    /// The protocol of OTLP export.
    pub otlp_export_protocol: Option<OtlpExportProtocol>,
}

/// The protocol of OTLP export.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum OtlpExportProtocol {
    /// GRPC protocol.
    Grpc,

    /// HTTP protocol with binary protobuf.
    Http,
}

/// The options of slow query.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct SlowQueryOptions {
    /// Whether to enable slow query log.
    pub enable: bool,

    /// The record type of slow queries.
    #[serde(deserialize_with = "empty_string_as_default")]
    pub record_type: SlowQueriesRecordType,

    /// The threshold of slow queries.
    #[serde(with = "humantime_serde")]
    pub threshold: Duration,

    /// The sample ratio of slow queries.
    pub sample_ratio: f64,

    /// The table TTL of `slow_queries` system table. Default is "90d".
    /// It's used when `record_type` is `SystemTable`.
    #[serde(with = "humantime_serde")]
    pub ttl: Duration,
}

impl Default for SlowQueryOptions {
    fn default() -> Self {
        Self {
            enable: true,
            record_type: SlowQueriesRecordType::SystemTable,
            threshold: Duration::from_secs(30),
            sample_ratio: 1.0,
            ttl: Duration::from_days(90),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Copy, PartialEq, Default)]
#[serde(rename_all = "snake_case")]
pub enum SlowQueriesRecordType {
    /// Record the slow query in the system table.
    #[default]
    SystemTable,
    /// Record the slow query in a specific logs file.
    Log,
}

#[derive(Clone, Debug, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum LogFormat {
    Json,
    #[default]
    Text,
}

impl PartialEq for LoggingOptions {
    fn eq(&self, other: &Self) -> bool {
        self.dir == other.dir
            && self.level == other.level
            && self.enable_otlp_tracing == other.enable_otlp_tracing
            && self.otlp_endpoint == other.otlp_endpoint
            && self.tracing_sample_ratio == other.tracing_sample_ratio
            && self.append_stdout == other.append_stdout
    }
}

impl Eq for LoggingOptions {}

impl Default for LoggingOptions {
    fn default() -> Self {
        Self {
            // The directory path will be configured at application startup, typically using the data home directory as a base.
            dir: "".to_string(),
            level: None,
            log_format: LogFormat::Text,
            enable_otlp_tracing: false,
            otlp_endpoint: None,
            tracing_sample_ratio: None,
            append_stdout: true,
            // Rotation hourly, 24 files per day, keeps info log files of 30 days
            max_log_files: 720,
            otlp_export_protocol: None,
        }
    }
}

#[derive(Default, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TracingOptions {
    #[cfg(feature = "tokio-console")]
    pub tokio_console_addr: Option<String>,
}

/// Init tracing for unittest.
/// Write logs to file `unittest`.
pub fn init_default_ut_logging() {
    static START: Once = Once::new();

    START.call_once(|| {
        let mut g = GLOBAL_UT_LOG_GUARD.as_ref().lock().unwrap();

        // When running in Github's actions, env "UNITTEST_LOG_DIR" is set to a directory other
        // than "/tmp".
        // This is to fix the problem that the "/tmp" disk space of action runner's is small,
        // if we write testing logs in it, actions would fail due to disk out of space error.
        let dir =
            env::var("UNITTEST_LOG_DIR").unwrap_or_else(|_| "/tmp/__unittest_logs".to_string());

        let level = env::var("UNITTEST_LOG_LEVEL").unwrap_or_else(|_|
            "debug,hyper=warn,tower=warn,datafusion=warn,reqwest=warn,sqlparser=warn,h2=info,opendal=info,rskafka=info".to_string()
        );
        let opts = LoggingOptions {
            dir: dir.clone(),
            level: Some(level),
            ..Default::default()
        };
        *g = Some(init_global_logging(
            "unittest",
            &opts,
            &TracingOptions::default(),
            None,
            None,
        ));

        crate::info!("logs dir = {}", dir);
    });
}

static GLOBAL_UT_LOG_GUARD: Lazy<Arc<Mutex<Option<Vec<WorkerGuard>>>>> =
    Lazy::new(|| Arc::new(Mutex::new(None)));

const DEFAULT_LOG_TARGETS: &str = "info";

#[allow(clippy::print_stdout)]
pub fn init_global_logging(
    app_name: &str,
    opts: &LoggingOptions,
    tracing_opts: &TracingOptions,
    node_id: Option<String>,
    slow_query_opts: Option<&SlowQueryOptions>,
) -> Vec<WorkerGuard> {
    static START: Once = Once::new();
    let mut guards = vec![];

    START.call_once(|| {
        // Enable log compatible layer to convert log record to tracing span.
        LogTracer::init().expect("log tracer must be valid");

        // Configure the stdout logging layer.
        let stdout_logging_layer = if opts.append_stdout {
            let (writer, guard) = tracing_appender::non_blocking(std::io::stdout());
            guards.push(guard);

            if opts.log_format == LogFormat::Json {
                Some(
                    Layer::new()
                        .json()
                        .with_writer(writer)
                        .with_ansi(std::io::stdout().is_terminal())
                        .boxed(),
                )
            } else {
                Some(
                    Layer::new()
                        .with_writer(writer)
                        .with_ansi(std::io::stdout().is_terminal())
                        .boxed(),
                )
            }
        } else {
            None
        };

        // Configure the file logging layer with rolling policy.
        let file_logging_layer = if !opts.dir.is_empty() {
            let rolling_appender = RollingFileAppender::builder()
                .rotation(Rotation::HOURLY)
                .filename_prefix("greptimedb")
                .max_log_files(opts.max_log_files)
                .build(&opts.dir)
                .unwrap_or_else(|e| {
                    panic!(
                        "initializing rolling file appender at {} failed: {}",
                        &opts.dir, e
                    )
                });
            let (writer, guard) = tracing_appender::non_blocking(rolling_appender);
            guards.push(guard);

            if opts.log_format == LogFormat::Json {
                Some(
                    Layer::new()
                        .json()
                        .with_writer(writer)
                        .with_ansi(false)
                        .boxed(),
                )
            } else {
                Some(Layer::new().with_writer(writer).with_ansi(false).boxed())
            }
        } else {
            None
        };

        // Configure the error file logging layer with rolling policy.
        let err_file_logging_layer = if !opts.dir.is_empty() {
            let rolling_appender = RollingFileAppender::builder()
                .rotation(Rotation::HOURLY)
                .filename_prefix("greptimedb-err")
                .max_log_files(opts.max_log_files)
                .build(&opts.dir)
                .unwrap_or_else(|e| {
                    panic!(
                        "initializing rolling file appender at {} failed: {}",
                        &opts.dir, e
                    )
                });
            let (writer, guard) = tracing_appender::non_blocking(rolling_appender);
            guards.push(guard);

            if opts.log_format == LogFormat::Json {
                Some(
                    Layer::new()
                        .json()
                        .with_writer(writer)
                        .with_ansi(false)
                        .with_filter(filter::LevelFilter::ERROR)
                        .boxed(),
                )
            } else {
                Some(
                    Layer::new()
                        .with_writer(writer)
                        .with_ansi(false)
                        .with_filter(filter::LevelFilter::ERROR)
                        .boxed(),
                )
            }
        } else {
            None
        };

        let slow_query_logging_layer = build_slow_query_logger(opts, slow_query_opts, &mut guards);

        // resolve log level settings from:
        // - options from command line or config files
        // - environment variable: RUST_LOG
        // - default settings
        let filter = opts
            .level
            .as_deref()
            .or(env::var(EnvFilter::DEFAULT_ENV).ok().as_deref())
            .unwrap_or(DEFAULT_LOG_TARGETS)
            .parse::<filter::Targets>()
            .expect("error parsing log level string");

        let (dyn_filter, reload_handle) = tracing_subscriber::reload::Layer::new(filter.clone());

        RELOAD_HANDLE
            .set(reload_handle)
            .expect("reload handle already set, maybe init_global_logging get called twice?");

        // Must enable 'tokio_unstable' cfg to use this feature.
        // For example: `RUSTFLAGS="--cfg tokio_unstable" cargo run -F common-telemetry/console -- standalone start`
        #[cfg(feature = "tokio-console")]
        let subscriber = {
            let tokio_console_layer =
                if let Some(tokio_console_addr) = &tracing_opts.tokio_console_addr {
                    let addr: std::net::SocketAddr = tokio_console_addr.parse().unwrap_or_else(|e| {
                    panic!("Invalid binding address '{tokio_console_addr}' for tokio-console: {e}");
                });
                    println!("tokio-console listening on {addr}");

                    Some(
                        console_subscriber::ConsoleLayer::builder()
                            .server_addr(addr)
                            .spawn(),
                    )
                } else {
                    None
                };

            Registry::default()
                .with(dyn_filter)
                .with(tokio_console_layer)
                .with(stdout_logging_layer)
                .with(file_logging_layer)
                .with(err_file_logging_layer)
                .with(slow_query_logging_layer)
        };

        // consume the `tracing_opts` to avoid "unused" warnings.
        let _ = tracing_opts;

        #[cfg(not(feature = "tokio-console"))]
        let subscriber = Registry::default()
            .with(dyn_filter)
            .with(stdout_logging_layer)
            .with(file_logging_layer)
            .with(err_file_logging_layer)
            .with(slow_query_logging_layer);

        if opts.enable_otlp_tracing {
            global::set_text_map_propagator(TraceContextPropagator::new());

            let sampler = opts
                .tracing_sample_ratio
                .as_ref()
                .map(create_sampler)
                .map(Sampler::ParentBased)
                .unwrap_or(Sampler::ParentBased(Box::new(Sampler::AlwaysOn)));

            let trace_config = opentelemetry_sdk::trace::config()
                .with_sampler(sampler)
                .with_resource(opentelemetry_sdk::Resource::new(vec![
                    KeyValue::new(resource::SERVICE_NAME, app_name.to_string()),
                    KeyValue::new(
                        resource::SERVICE_INSTANCE_ID,
                        node_id.unwrap_or("none".to_string()),
                    ),
                    KeyValue::new(resource::SERVICE_VERSION, common_version::version()),
                    KeyValue::new(resource::PROCESS_PID, std::process::id().to_string()),
                ]));

            let tracer = opentelemetry_otlp::new_pipeline()
                .tracing()
                .with_exporter(build_otlp_exporter(opts))
                .with_trace_config(trace_config)
                .install_batch(opentelemetry_sdk::runtime::Tokio)
                .expect("otlp tracer install failed");

            tracing::subscriber::set_global_default(
                subscriber.with(tracing_opentelemetry::layer().with_tracer(tracer)),
            )
            .expect("error setting global tracing subscriber");
        } else {
            tracing::subscriber::set_global_default(subscriber)
                .expect("error setting global tracing subscriber");
        }
    });

    guards
}

fn build_otlp_exporter(opts: &LoggingOptions) -> SpanExporterBuilder {
    let protocol = opts
        .otlp_export_protocol
        .clone()
        .unwrap_or(OtlpExportProtocol::Http);

    let endpoint = opts
        .otlp_endpoint
        .as_ref()
        .map(|e| {
            if e.starts_with("http") {
                e.to_string()
            } else {
                format!("http://{}", e)
            }
        })
        .unwrap_or_else(|| match protocol {
            OtlpExportProtocol::Grpc => DEFAULT_OTLP_GRPC_ENDPOINT.to_string(),
            OtlpExportProtocol::Http => DEFAULT_OTLP_HTTP_ENDPOINT.to_string(),
        });

    match protocol {
        OtlpExportProtocol::Grpc => SpanExporterBuilder::Tonic(
            opentelemetry_otlp::new_exporter()
                .tonic()
                .with_endpoint(endpoint),
        ),
        OtlpExportProtocol::Http => SpanExporterBuilder::Http(
            opentelemetry_otlp::new_exporter()
                .http()
                .with_endpoint(endpoint)
                .with_protocol(Protocol::HttpBinary),
        ),
    }
}

fn build_slow_query_logger<S>(
    opts: &LoggingOptions,
    slow_query_opts: Option<&SlowQueryOptions>,
    guards: &mut Vec<WorkerGuard>,
) -> Option<Box<dyn tracing_subscriber::Layer<S> + Send + Sync + 'static>>
where
    S: tracing::Subscriber
        + Send
        + 'static
        + for<'span> tracing_subscriber::registry::LookupSpan<'span>,
{
    if let Some(slow_query_opts) = slow_query_opts {
        if !opts.dir.is_empty()
            && slow_query_opts.enable
            && slow_query_opts.record_type == SlowQueriesRecordType::Log
        {
            let rolling_appender = RollingFileAppender::builder()
                .rotation(Rotation::HOURLY)
                .filename_prefix("greptimedb-slow-queries")
                .max_log_files(opts.max_log_files)
                .build(&opts.dir)
                .unwrap_or_else(|e| {
                    panic!(
                        "initializing rolling file appender at {} failed: {}",
                        &opts.dir, e
                    )
                });
            let (writer, guard) = tracing_appender::non_blocking(rolling_appender);
            guards.push(guard);

            // Only logs if the field contains "slow".
            let slow_query_filter = FilterFn::new(|metadata| {
                metadata
                    .fields()
                    .iter()
                    .any(|field| field.name().contains("slow"))
            });

            if opts.log_format == LogFormat::Json {
                Some(
                    Layer::new()
                        .json()
                        .with_writer(writer)
                        .with_ansi(false)
                        .with_filter(slow_query_filter)
                        .boxed(),
                )
            } else {
                Some(
                    Layer::new()
                        .with_writer(writer)
                        .with_ansi(false)
                        .with_filter(slow_query_filter)
                        .boxed(),
                )
            }
        } else {
            None
        }
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_logging_options_deserialization_default() {
        let json = r#"{}"#;
        let opts: LoggingOptions = serde_json::from_str(json).unwrap();

        assert_eq!(opts.log_format, LogFormat::Text);
        assert_eq!(opts.dir, "");
        assert_eq!(opts.level, None);
        assert!(opts.append_stdout);
    }

    #[test]
    fn test_logging_options_deserialization_empty_log_format() {
        let json = r#"{"log_format": ""}"#;
        let opts: LoggingOptions = serde_json::from_str(json).unwrap();

        // Empty string should use default (Text)
        assert_eq!(opts.log_format, LogFormat::Text);
    }

    #[test]
    fn test_logging_options_deserialization_valid_log_format() {
        let json_format = r#"{"log_format": "json"}"#;
        let opts: LoggingOptions = serde_json::from_str(json_format).unwrap();
        assert_eq!(opts.log_format, LogFormat::Json);

        let text_format = r#"{"log_format": "text"}"#;
        let opts: LoggingOptions = serde_json::from_str(text_format).unwrap();
        assert_eq!(opts.log_format, LogFormat::Text);
    }

    #[test]
    fn test_logging_options_deserialization_missing_log_format() {
        let json = r#"{"dir": "/tmp/logs"}"#;
        let opts: LoggingOptions = serde_json::from_str(json).unwrap();

        // Missing log_format should use default (Text)
        assert_eq!(opts.log_format, LogFormat::Text);
        assert_eq!(opts.dir, "/tmp/logs");
    }

    #[test]
    fn test_slow_query_options_deserialization_default() {
        let json = r#"{"enable": true, "threshold": "30s"}"#;
        let opts: SlowQueryOptions = serde_json::from_str(json).unwrap();

        assert_eq!(opts.record_type, SlowQueriesRecordType::SystemTable);
        assert!(opts.enable);
    }

    #[test]
    fn test_slow_query_options_deserialization_empty_record_type() {
        let json = r#"{"enable": true, "record_type": "", "threshold": "30s"}"#;
        let opts: SlowQueryOptions = serde_json::from_str(json).unwrap();

        // Empty string should use default (SystemTable)
        assert_eq!(opts.record_type, SlowQueriesRecordType::SystemTable);
        assert!(opts.enable);
    }

    #[test]
    fn test_slow_query_options_deserialization_valid_record_type() {
        let system_table_json =
            r#"{"enable": true, "record_type": "system_table", "threshold": "30s"}"#;
        let opts: SlowQueryOptions = serde_json::from_str(system_table_json).unwrap();
        assert_eq!(opts.record_type, SlowQueriesRecordType::SystemTable);

        let log_json = r#"{"enable": true, "record_type": "log", "threshold": "30s"}"#;
        let opts: SlowQueryOptions = serde_json::from_str(log_json).unwrap();
        assert_eq!(opts.record_type, SlowQueriesRecordType::Log);
    }

    #[test]
    fn test_slow_query_options_deserialization_missing_record_type() {
        let json = r#"{"enable": false, "threshold": "30s"}"#;
        let opts: SlowQueryOptions = serde_json::from_str(json).unwrap();

        // Missing record_type should use default (SystemTable)
        assert_eq!(opts.record_type, SlowQueriesRecordType::SystemTable);
        assert!(!opts.enable);
    }

    #[test]
    fn test_otlp_export_protocol_deserialization_valid_values() {
        let grpc_json = r#""grpc""#;
        let protocol: OtlpExportProtocol = serde_json::from_str(grpc_json).unwrap();
        assert_eq!(protocol, OtlpExportProtocol::Grpc);

        let http_json = r#""http""#;
        let protocol: OtlpExportProtocol = serde_json::from_str(http_json).unwrap();
        assert_eq!(protocol, OtlpExportProtocol::Http);
    }
}
