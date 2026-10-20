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
mod file_retention;

use std::collections::HashMap;
use std::env;
use std::io::IsTerminal;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Once};
use std::time::Duration;

use common_base::readable_size::ReadableSize;
use common_base::serde::empty_string_as_default;
use file_retention::{DirectoryRetention, LogFileKind, build_file_appender};
use once_cell::sync::{Lazy, OnceCell};
use opentelemetry::trace::TracerProvider;
use opentelemetry::{KeyValue, global};
use opentelemetry_otlp::{Protocol, SpanExporter, WithExportConfig, WithHttpConfig};
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::{Sampler, Tracer};
use opentelemetry_semantic_conventions::resource;
use serde::{Deserialize, Serialize};
use tracing::callsite;
use tracing::metadata::LevelFilter;
use tracing_appender::non_blocking::{NonBlocking, WorkerGuard};
use tracing_log::LogTracer;
use tracing_subscriber::filter::{FilterFn, Targets};
use tracing_subscriber::fmt::Layer;
use tracing_subscriber::layer::{Layered, SubscriberExt};
use tracing_subscriber::prelude::*;
use tracing_subscriber::{EnvFilter, Registry, filter};

use crate::tracing_sampler::{TracingSampleOptions, create_sampler};

/// The default endpoint when use gRPC exporter protocol.
pub const DEFAULT_OTLP_GRPC_ENDPOINT: &str = "http://localhost:4317";

/// The default endpoint when use HTTP exporter protocol.
pub const DEFAULT_OTLP_HTTP_ENDPOINT: &str = "http://localhost:4318/v1/traces";

/// The default logs directory.
pub const DEFAULT_LOGGING_DIR: &str = "logs";

/// Handle for reloading log level
pub static LOG_RELOAD_HANDLE: OnceCell<tracing_subscriber::reload::Handle<Targets, Registry>> =
    OnceCell::new();

/// The independent dispatcher used by the `slow!` macro.
#[doc(hidden)]
pub static SLOW_QUERY_LOGGER: OnceCell<tracing::Dispatch> = OnceCell::new();

type DynSubscriber = Layered<tracing_subscriber::reload::Layer<Targets, Registry>, Registry>;
type OtelTraceLayer = tracing_opentelemetry::OpenTelemetryLayer<DynSubscriber, Tracer>;

struct TraceLayerState {
    enabled: AtomicBool,
    layer: OnceCell<OtelTraceLayer>,
}

#[derive(Clone)]
pub struct TraceReloadHandle {
    inner: Arc<TraceLayerState>,
}

impl TraceReloadHandle {
    fn new(inner: Arc<TraceLayerState>) -> Self {
        Self { inner }
    }

    /// Enables or disables OTLP data collection, initializing it on first enable.
    /// Disabling stops new spans, events, fields and links. Existing spans still
    /// finish their lifecycle and export on close.
    pub fn set_enabled(&self, enabled: bool) -> Result<(), &'static str> {
        self.set_enabled_with(enabled, || {
            get_or_init_tracer().map(|tracer| tracing_opentelemetry::layer().with_tracer(tracer))
        })
    }

    fn set_enabled_with(
        &self,
        enabled: bool,
        init: impl FnOnce() -> Result<OtelTraceLayer, &'static str>,
    ) -> Result<(), &'static str> {
        if enabled {
            self.inner.layer.get_or_try_init(init)?;
        }
        self.inner.enabled.store(enabled, Ordering::Release);
        callsite::rebuild_interest_cache();
        Ok(())
    }
}

/// An OTLP layer with a runtime switch and a stable address for downcasts.
struct TraceLayer {
    inner: Arc<TraceLayerState>,
}

impl TraceLayer {
    fn new(initial: Option<OtelTraceLayer>) -> (Self, TraceReloadHandle) {
        let inner = Arc::new(TraceLayerState {
            enabled: AtomicBool::new(initial.is_some()),
            layer: initial.map(OnceCell::with_value).unwrap_or_default(),
        });
        (
            Self {
                inner: inner.clone(),
            },
            TraceReloadHandle::new(inner),
        )
    }

    fn with_layer<R>(&self, f: impl FnOnce(&OtelTraceLayer) -> R) -> Option<R> {
        self.inner.layer.get().map(f)
    }

    fn is_enabled(&self) -> bool {
        self.inner.enabled.load(Ordering::Acquire)
    }
}

impl tracing_subscriber::Layer<DynSubscriber> for TraceLayer {
    fn on_register_dispatch(&self, subscriber: &tracing::Dispatch) {
        let _ = self.with_layer(|layer| layer.on_register_dispatch(subscriber));
    }

    fn register_callsite(
        &self,
        metadata: &'static tracing::Metadata<'static>,
    ) -> tracing::subscriber::Interest {
        self.with_layer(|layer| layer.register_callsite(metadata))
            .unwrap_or_else(tracing::subscriber::Interest::always)
    }

    fn enabled(
        &self,
        metadata: &tracing::Metadata<'_>,
        ctx: tracing_subscriber::layer::Context<'_, DynSubscriber>,
    ) -> bool {
        self.with_layer(|layer| layer.enabled(metadata, ctx))
            .unwrap_or(true)
    }

    fn on_new_span(
        &self,
        attrs: &tracing::span::Attributes<'_>,
        id: &tracing::span::Id,
        ctx: tracing_subscriber::layer::Context<'_, DynSubscriber>,
    ) {
        if self.is_enabled() {
            let _ = self.with_layer(|layer| layer.on_new_span(attrs, id, ctx));
        }
    }

    fn max_level_hint(&self) -> Option<LevelFilter> {
        self.with_layer(|layer| layer.max_level_hint()).flatten()
    }

    fn on_record(
        &self,
        span: &tracing::span::Id,
        values: &tracing::span::Record<'_>,
        ctx: tracing_subscriber::layer::Context<'_, DynSubscriber>,
    ) {
        if self.is_enabled() {
            let _ = self.with_layer(|layer| layer.on_record(span, values, ctx));
        }
    }

    fn on_follows_from(
        &self,
        span: &tracing::span::Id,
        follows: &tracing::span::Id,
        ctx: tracing_subscriber::layer::Context<'_, DynSubscriber>,
    ) {
        if self.is_enabled() {
            let _ = self.with_layer(|layer| layer.on_follows_from(span, follows, ctx));
        }
    }

    fn event_enabled(
        &self,
        event: &tracing::Event<'_>,
        ctx: tracing_subscriber::layer::Context<'_, DynSubscriber>,
    ) -> bool {
        self.with_layer(|layer| layer.event_enabled(event, ctx))
            .unwrap_or(true)
    }

    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        ctx: tracing_subscriber::layer::Context<'_, DynSubscriber>,
    ) {
        if self.is_enabled() {
            let _ = self.with_layer(|layer| layer.on_event(event, ctx));
        }
    }

    fn on_enter(
        &self,
        id: &tracing::span::Id,
        ctx: tracing_subscriber::layer::Context<'_, DynSubscriber>,
    ) {
        let _ = self.with_layer(|layer| layer.on_enter(id, ctx));
    }

    fn on_exit(
        &self,
        id: &tracing::span::Id,
        ctx: tracing_subscriber::layer::Context<'_, DynSubscriber>,
    ) {
        let _ = self.with_layer(|layer| layer.on_exit(id, ctx));
    }

    fn on_close(
        &self,
        id: tracing::span::Id,
        ctx: tracing_subscriber::layer::Context<'_, DynSubscriber>,
    ) {
        let _ = self.with_layer(|layer| layer.on_close(id, ctx));
    }

    fn on_id_change(
        &self,
        old: &tracing::span::Id,
        new: &tracing::span::Id,
        ctx: tracing_subscriber::layer::Context<'_, DynSubscriber>,
    ) {
        let _ = self.with_layer(|layer| layer.on_id_change(old, new, ctx));
    }

    unsafe fn downcast_raw(&self, id: std::any::TypeId) -> Option<*const ()> {
        // Keep downcasts available while disabled: an in-flight WithContext
        // callback may still need the layer. OnceCell keeps both addresses valid
        // for the subscriber's lifetime, even across concurrent toggles.
        self.inner
            .layer
            .get()
            .and_then(|layer| unsafe { layer.downcast_raw(id) })
    }
}

/// Handle for reloading trace level
pub static TRACE_RELOAD_HANDLE: OnceCell<TraceReloadHandle> = OnceCell::new();

static TRACER: OnceCell<Mutex<TraceState>> = OnceCell::new();

#[derive(Debug)]
enum TraceState {
    Ready(Tracer),
    Deferred(TraceContext),
}

/// The logging options that used to initialize the logger.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
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

    /// The maximum total size of managed log files in `dir`. Zero disables size-based retention.
    pub max_log_dir_size: ReadableSize,

    /// Whether to append logs to stdout. Default is true.
    pub append_stdout: bool,

    /// Whether to write logs to files in `dir`. Default is true.
    pub enable_file_logging: bool,

    /// Whether to enable tracing with OTLP. Default is false.
    pub enable_otlp_tracing: bool,

    /// The endpoint of OTLP.
    pub otlp_endpoint: Option<String>,

    /// The tracing sample ratio.
    pub tracing_sample_ratio: Option<TracingSampleOptions>,

    /// The protocol of OTLP export.
    pub otlp_export_protocol: Option<OtlpExportProtocol>,

    /// Additional HTTP headers for OTLP exporter.
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    pub otlp_headers: HashMap<String, String>,

    /// Whether to enable per-region metrics.
    pub enable_per_region_metrics: bool,
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

    /// The record type of slow queries. Log records ignore the global log level and use
    /// the configured stdout and file destinations, format, and retention settings.
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
            ttl: Duration::from_secs(90 * 86400),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Copy, PartialEq, Default)]
#[serde(rename_all = "snake_case")]
pub enum SlowQueriesRecordType {
    /// Record the slow query in the system table.
    #[default]
    SystemTable,
    /// Record the slow query to the configured stdout and file destinations.
    Log,
}

#[derive(Clone, Debug, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum LogFormat {
    Json,
    #[default]
    Text,
}

#[derive(Clone, Debug)]
struct TraceContext {
    app_name: String,
    node_id: String,
    logging_opts: LoggingOptions,
}

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
            enable_file_logging: true,
            // Rotation hourly, 24 files per day, keeps info log files of 30 days
            max_log_files: 720,
            max_log_dir_size: ReadableSize::default(),
            otlp_export_protocol: None,
            otlp_headers: HashMap::new(),
            enable_per_region_metrics: false,
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
    let node_id = node_id.unwrap_or_else(|| "none".to_string());

    START.call_once(|| {
        // Enable log compatible layer to convert log record to tracing span.
        LogTracer::init().expect("log tracer must be valid");

        let stdout_writer = if opts.append_stdout {
            let (writer, guard) = tracing_appender::non_blocking(std::io::stdout());
            guards.push(guard);
            Some(writer)
        } else {
            None
        };
        let stdout_logging_layer = stdout_writer.clone().map(|writer| {
            build_logging_layer(writer, opts.log_format, std::io::stdout().is_terminal())
        });

        let file_logging_enabled = opts.enable_file_logging && !opts.dir.is_empty();
        let slow_query_logging_enabled = slow_query_opts
            .is_some_and(|opts| opts.enable && opts.record_type == SlowQueriesRecordType::Log);

        let retention = file_logging_enabled
            .then(|| {
                DirectoryRetention::new(opts.dir.clone(), opts.max_log_dir_size, opts.max_log_files)
            })
            .flatten();

        // Configure the file logging layer with rolling policy.
        let file_logging_layer = if file_logging_enabled {
            let rolling_appender =
                build_file_appender(opts, LogFileKind::Default, retention.as_ref());
            let (writer, guard) = tracing_appender::non_blocking(rolling_appender);
            guards.push(guard);

            Some(build_logging_layer(writer, opts.log_format, false))
        } else {
            None
        };

        // Configure the error file logging layer with rolling policy.
        let err_file_logging_layer = if file_logging_enabled {
            let rolling_appender =
                build_file_appender(opts, LogFileKind::Error, retention.as_ref());
            let (writer, guard) = tracing_appender::non_blocking(rolling_appender);
            guards.push(guard);

            Some(
                build_logging_layer(writer, opts.log_format, false)
                    .with_filter(filter::LevelFilter::ERROR),
            )
        } else {
            None
        };

        if slow_query_logging_enabled {
            SLOW_QUERY_LOGGER
                .set(build_slow_query_logger(
                    opts,
                    stdout_writer,
                    retention.as_ref(),
                    &mut guards,
                ))
                .expect("slow query logger already initialized");
        }

        if let Some(retention) = &retention {
            retention.initialize();
        }

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

        LOG_RELOAD_HANDLE
            .set(reload_handle)
            .expect("reload handle already set, maybe init_global_logging get called twice?");

        let mut initial_tracer = None;
        let trace_state = if opts.enable_otlp_tracing {
            let tracer = create_tracer(app_name, &node_id, opts);
            initial_tracer = Some(tracer.clone());
            TraceState::Ready(tracer)
        } else {
            TraceState::Deferred(TraceContext {
                app_name: app_name.to_string(),
                node_id: node_id.clone(),
                logging_opts: opts.clone(),
            })
        };

        TRACER
            .set(Mutex::new(trace_state))
            .expect("trace state already initialized");

        let initial_trace_layer = initial_tracer
            .as_ref()
            .map(|tracer| tracing_opentelemetry::layer().with_tracer(tracer.clone()));

        let (dyn_trace_layer, trace_reload_handle) = TraceLayer::new(initial_trace_layer);

        TRACE_RELOAD_HANDLE
            .set(trace_reload_handle)
            .unwrap_or_else(|_| panic!("failed to set trace reload handle"));

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
                .with(dyn_trace_layer)
                .with(tokio_console_layer)
                .with(stdout_logging_layer)
                .with(file_logging_layer)
                .with(err_file_logging_layer)
        };

        // consume the `tracing_opts` to avoid "unused" warnings.
        let _ = tracing_opts;

        #[cfg(not(feature = "tokio-console"))]
        let subscriber = Registry::default()
            .with(dyn_filter)
            .with(dyn_trace_layer)
            .with(stdout_logging_layer)
            .with(file_logging_layer)
            .with(err_file_logging_layer);

        global::set_text_map_propagator(TraceContextPropagator::new());

        tracing::subscriber::set_global_default(subscriber)
            .expect("error setting global tracing subscriber");
    });

    guards
}

fn create_tracer(app_name: &str, node_id: &str, opts: &LoggingOptions) -> Tracer {
    let sampler = opts
        .tracing_sample_ratio
        .as_ref()
        .map(create_sampler)
        .map(Sampler::ParentBased)
        .unwrap_or(Sampler::ParentBased(Box::new(Sampler::AlwaysOn)));

    let resource = opentelemetry_sdk::Resource::builder_empty()
        .with_attributes([
            KeyValue::new(resource::SERVICE_NAME, app_name.to_string()),
            KeyValue::new(resource::SERVICE_INSTANCE_ID, node_id.to_string()),
            KeyValue::new(resource::SERVICE_VERSION, common_version::version()),
            KeyValue::new(resource::PROCESS_PID, std::process::id().to_string()),
        ])
        .build();

    opentelemetry_sdk::trace::SdkTracerProvider::builder()
        .with_batch_exporter(build_otlp_exporter(opts))
        .with_sampler(sampler)
        .with_resource(resource)
        .build()
        .tracer("greptimedb")
}

/// Ensure that the OTLP tracer has been constructed, building it lazily if needed.
pub fn get_or_init_tracer() -> Result<Tracer, &'static str> {
    let state = TRACER.get().ok_or("trace state is not initialized")?;
    let mut guard = state.lock().expect("trace state lock poisoned");

    match &mut *guard {
        TraceState::Ready(tracer) => Ok(tracer.clone()),
        TraceState::Deferred(context) => {
            let tracer = create_tracer(&context.app_name, &context.node_id, &context.logging_opts);
            *guard = TraceState::Ready(tracer.clone());
            Ok(tracer)
        }
    }
}

fn build_otlp_exporter(opts: &LoggingOptions) -> SpanExporter {
    let protocol = opts
        .otlp_export_protocol
        .clone()
        .unwrap_or(OtlpExportProtocol::Http);

    let endpoint = opts
        .otlp_endpoint
        .as_ref()
        .map(|e| {
            if e.starts_with("http") {
                e.clone()
            } else {
                format!("http://{}", e)
            }
        })
        .unwrap_or_else(|| match protocol {
            OtlpExportProtocol::Grpc => DEFAULT_OTLP_GRPC_ENDPOINT.to_string(),
            OtlpExportProtocol::Http => DEFAULT_OTLP_HTTP_ENDPOINT.to_string(),
        });

    match protocol {
        OtlpExportProtocol::Grpc => SpanExporter::builder()
            .with_tonic()
            .with_endpoint(endpoint)
            .build()
            .expect("Failed to create OTLP gRPC exporter "),

        OtlpExportProtocol::Http => SpanExporter::builder()
            .with_http()
            .with_endpoint(endpoint)
            .with_protocol(Protocol::HttpBinary)
            .with_headers(opts.otlp_headers.clone())
            .build()
            .expect("Failed to create OTLP HTTP exporter "),
    }
}

fn build_slow_query_logger(
    opts: &LoggingOptions,
    stdout_writer: Option<NonBlocking>,
    retention: Option<&DirectoryRetention>,
    guards: &mut Vec<WorkerGuard>,
) -> tracing::Dispatch {
    let stdout_layer = stdout_writer.map(|writer| {
        build_logging_layer(writer, opts.log_format, std::io::stdout().is_terminal())
    });
    let file_layer = if opts.enable_file_logging && !opts.dir.is_empty() {
        let rolling_appender = build_file_appender(opts, LogFileKind::SlowQuery, retention);
        let (writer, guard) = tracing_appender::non_blocking(rolling_appender);
        guards.push(guard);
        Some(build_logging_layer(writer, opts.log_format, false))
    } else {
        None
    };
    // Avoid enabling unrelated callsites through this independently registered dispatcher.
    let filter = FilterFn::new(|metadata| {
        *metadata.level() == tracing::Level::INFO && metadata.fields().field("slow").is_some()
    })
    .with_max_level_hint(LevelFilter::INFO);
    tracing::Dispatch::new(
        Registry::default()
            .with(filter)
            .with(stdout_layer)
            .with(file_layer),
    )
}

fn build_logging_layer<S>(
    writer: NonBlocking,
    log_format: LogFormat,
    ansi: bool,
) -> Box<dyn tracing_subscriber::Layer<S> + Send + Sync>
where
    S: tracing::Subscriber + for<'span> tracing_subscriber::registry::LookupSpan<'span>,
{
    if log_format == LogFormat::Json {
        Layer::new()
            .json()
            .with_writer(writer)
            .with_ansi(ansi)
            .boxed()
    } else {
        Layer::new().with_writer(writer).with_ansi(ansi).boxed()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Barrier;
    use std::sync::atomic::AtomicUsize;

    use opentelemetry::trace::TraceContextExt;
    use opentelemetry_sdk::trace::SdkTracerProvider;
    use tracing_opentelemetry::OpenTelemetrySpanExt;

    use super::*;

    #[test]
    fn test_slow_query_logging() {
        const CASE_ENV: &str = "GREPTIME_TEST_SLOW_QUERY_LOGGING";
        let Ok(case) = env::var(CASE_ENV) else {
            // Each configuration needs a fresh process because logging initializes only once.
            for case in [
                "text",
                "json",
                "disabled",
                "system_table",
                "file_disabled",
                "stdout_only_json",
                "stdout_disabled",
                "no_directory",
                "no_options",
            ] {
                let output = std::process::Command::new(env::current_exe().unwrap())
                    .args([
                        "--exact",
                        "logging::tests::test_slow_query_logging",
                        "--nocapture",
                    ])
                    .env(CASE_ENV, case)
                    .output()
                    .unwrap();
                assert!(
                    output.status.success(),
                    "case {case} failed:\n{}\n{}",
                    String::from_utf8_lossy(&output.stdout),
                    String::from_utf8_lossy(&output.stderr),
                );
                let stdout = String::from_utf8(output.stdout).unwrap();
                let expected_slow_queries = if matches!(
                    case,
                    "text" | "json" | "file_disabled" | "stdout_only_json" | "no_directory"
                ) {
                    4
                } else {
                    0
                };
                assert_eq!(
                    stdout.matches("SELECT 1").count(),
                    expected_slow_queries,
                    "{case}: {stdout}"
                );
                assert_eq!(
                    stdout.matches("SELECT 2").count(),
                    expected_slow_queries,
                    "{case}: {stdout}"
                );
                assert_eq!(
                    stdout.matches("custom_slow_target").count(),
                    expected_slow_queries,
                    "{case}: {stdout}"
                );
                if case != "stdout_disabled" {
                    assert_eq!(
                        stdout.matches("ordinary-info").count(),
                        1,
                        "{case}: {stdout}"
                    );
                    assert_eq!(
                        stdout.matches("ordinary-warning").count(),
                        2,
                        "{case}: {stdout}"
                    );
                } else {
                    assert!(!stdout.contains("ordinary-"), "{stdout}");
                }
                if matches!(case, "json" | "stdout_only_json") {
                    for line in stdout.lines().filter(|line| line.contains("SELECT")) {
                        let event: serde_json::Value = serde_json::from_str(line).unwrap();
                        assert_eq!(event["fields"]["slow"], true);
                        assert_eq!(event["level"], "INFO");
                    }
                }
            }
            return;
        };

        let dir = tempfile::tempdir().unwrap();
        let opts = LoggingOptions {
            dir: if case == "no_directory" {
                String::new()
            } else {
                dir.path().to_str().unwrap().to_string()
            },
            level: Some("warn".to_string()),
            log_format: if matches!(case.as_str(), "json" | "stdout_only_json") {
                LogFormat::Json
            } else {
                LogFormat::Text
            },
            append_stdout: case != "stdout_disabled",
            enable_file_logging: !matches!(case.as_str(), "file_disabled" | "stdout_only_json"),
            max_log_dir_size: ReadableSize::mb(1),
            ..Default::default()
        };
        let slow_opts = SlowQueryOptions {
            enable: case != "disabled",
            record_type: if case == "system_table" {
                SlowQueriesRecordType::SystemTable
            } else {
                SlowQueriesRecordType::Log
            },
            ..Default::default()
        };
        let guards = init_global_logging(
            "slow-query-test",
            &opts,
            &TracingOptions::default(),
            None,
            (case != "no_options").then_some(&slow_opts),
        );

        for level in ["warn", "error", "off", "info"] {
            LOG_RELOAD_HANDLE
                .get()
                .unwrap()
                .reload(level.parse::<Targets>().unwrap())
                .unwrap();
            crate::info!(slow_operation = true, "ordinary-info");
            crate::warn!("ordinary-warning");
            crate::slow!(query = "SELECT 1", cost = 31, threshold = 30);
            crate::slow!(target: "custom_slow_target", query = "SELECT 2");
        }
        // Dropping the guards flushes the asynchronous writers before reading the files.
        drop(guards);

        let mut slow_logs = String::new();
        let mut regular_logs = String::new();
        for entry in std::fs::read_dir(dir.path()).unwrap() {
            let entry = entry.unwrap();
            let name = entry.file_name();
            let name = name.to_str().unwrap();
            if name.starts_with("greptimedb-slow-queries.") {
                slow_logs.push_str(&std::fs::read_to_string(entry.path()).unwrap());
            } else if name.starts_with("greptimedb.") {
                regular_logs.push_str(&std::fs::read_to_string(entry.path()).unwrap());
            }
        }

        if matches!(case.as_str(), "text" | "json" | "stdout_disabled") {
            assert_eq!(slow_logs.lines().count(), 8, "{slow_logs}");
            assert_eq!(slow_logs.matches("SELECT 1").count(), 4);
            assert_eq!(slow_logs.matches("SELECT 2").count(), 4);
            assert_eq!(slow_logs.matches("custom_slow_target").count(), 4);
            assert!(!slow_logs.contains("ordinary-"));
            if case == "json" {
                for line in slow_logs.lines() {
                    let event: serde_json::Value = serde_json::from_str(line).unwrap();
                    assert_eq!(event["fields"]["slow"], true);
                    assert_eq!(event["level"], "INFO");
                }
            }
        } else {
            assert!(slow_logs.is_empty(), "{slow_logs}");
        }
        if matches!(
            case.as_str(),
            "file_disabled" | "stdout_only_json" | "no_directory"
        ) {
            assert!(regular_logs.is_empty(), "{regular_logs}");
        } else {
            assert_eq!(regular_logs.matches("ordinary-info").count(), 1);
            assert_eq!(regular_logs.matches("ordinary-warning").count(), 2);
            assert!(!regular_logs.contains("SELECT"), "{regular_logs}");
        }
    }

    #[test]
    fn test_trace_switch_preserves_active_spans() {
        let provider = SdkTracerProvider::builder()
            .with_sampler(Sampler::AlwaysOn)
            .build();
        for initially_enabled in [false, true] {
            let new_layer =
                || tracing_opentelemetry::layer().with_tracer(provider.tracer("switch"));
            let (filter, _) = tracing_subscriber::reload::Layer::new(
                Targets::new().with_default(tracing::Level::INFO),
            );
            let (layer, handle) = TraceLayer::new(initially_enabled.then(new_layer));
            let dispatch = tracing::Dispatch::new(Registry::default().with(filter).with(layer));
            tracing::dispatcher::with_default(&dispatch, || {
                let initial = tracing::info_span!("initial");
                assert!(!initial.is_disabled());
                assert_eq!(
                    initial.context().span().span_context().is_valid(),
                    initially_enabled
                );
                handle.set_enabled_with(true, || Ok(new_layer())).unwrap();
                let parent = tracing::info_span!("parent");
                let parent_context = parent.context();
                assert!(parent_context.span().span_context().is_valid());
                let previous_context = opentelemetry::Context::current();
                let entered = parent.enter();
                handle.set_enabled(false).unwrap();
                let disabled = tracing::info_span!("disabled");
                assert!(!disabled.is_disabled());
                assert!(!disabled.context().span().span_context().is_valid());
                assert_eq!(
                    parent.context().span().span_context(),
                    parent_context.span().span_context()
                );
                assert!(dispatch.downcast_ref::<OtelTraceLayer>().is_some());
                drop(entered);
                assert_eq!(
                    opentelemetry::Context::current().span().span_context(),
                    previous_context.span().span_context()
                );
                let disabled_entered = disabled.enter();
                handle
                    .set_enabled_with(true, || panic!("layer must not be replaced"))
                    .unwrap();
                drop(disabled_entered);
                let child = tracing::info_span!(parent: &parent, "child");
                assert_eq!(
                    child.context().span().span_context().trace_id(),
                    parent_context.span().span_context().trace_id()
                );
                assert!(!disabled.context().span().span_context().is_valid());
            });
        }
    }

    #[test]
    fn test_trace_switch_retries_initialization() {
        let (filter, _) = tracing_subscriber::reload::Layer::new(
            Targets::new().with_default(tracing::Level::INFO),
        );
        let (layer, handle) = TraceLayer::new(None);
        let subscriber = Registry::default().with(filter).with(layer);
        tracing::subscriber::with_default(subscriber, || {
            handle
                .set_enabled_with(false, || panic!("disabling must not initialize OTLP"))
                .unwrap();
            assert_eq!(
                handle.set_enabled_with(true, || Err("initialization failed")),
                Err("initialization failed")
            );
            let failed = tracing::info_span!("after_failed_enable");
            assert!(!failed.context().span().span_context().is_valid());
            let provider = SdkTracerProvider::builder()
                .with_sampler(Sampler::AlwaysOn)
                .build();
            handle
                .set_enabled_with(true, || {
                    Ok(tracing_opentelemetry::layer().with_tracer(provider.tracer("retry")))
                })
                .unwrap();
            let enabled = tracing::info_span!("after_successful_enable");
            assert!(enabled.context().span().span_context().is_valid());
        });
    }

    #[test]
    fn test_trace_switch_stops_collecting_fields_when_disabled() {
        struct CountFormatting(AtomicUsize);

        impl std::fmt::Debug for CountFormatting {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                self.0.fetch_add(1, Ordering::Relaxed);
                f.write_str("value")
            }
        }

        let value = CountFormatting(AtomicUsize::new(0));
        let provider = SdkTracerProvider::builder()
            .with_sampler(Sampler::AlwaysOn)
            .build();
        let (filter, _) = tracing_subscriber::reload::Layer::new(
            Targets::new().with_default(tracing::Level::INFO),
        );
        let (layer, handle) = TraceLayer::new(Some(
            tracing_opentelemetry::layer().with_tracer(provider.tracer("fields")),
        ));
        tracing::subscriber::with_default(Registry::default().with(filter).with(layer), || {
            let admitted = tracing::info_span!("admitted", field = tracing::field::Empty);
            admitted.record("field", tracing::field::debug(&value));
            admitted.in_scope(|| tracing::info!(value = ?value));
            assert_eq!(value.0.load(Ordering::Relaxed), 2);

            handle.set_enabled(false).unwrap();
            let unadmitted = tracing::info_span!("unadmitted", field = tracing::field::Empty);
            for span in [&admitted, &unadmitted] {
                span.record("field", tracing::field::debug(&value));
                span.in_scope(|| tracing::info!(value = ?value));
            }
            assert_eq!(value.0.load(Ordering::Relaxed), 2);

            handle
                .set_enabled_with(true, || panic!("layer must not be replaced"))
                .unwrap();
            admitted.record("field", tracing::field::debug(&value));
            admitted.in_scope(|| tracing::info!(value = ?value));
            assert_eq!(value.0.load(Ordering::Relaxed), 4);
        });
    }

    #[test]
    fn test_trace_switch_finishes_admitted_spans() {
        #[derive(Debug)]
        struct Exporter(std::sync::mpsc::Sender<String>);

        impl opentelemetry_sdk::trace::SpanExporter for Exporter {
            async fn export(
                &self,
                batch: Vec<opentelemetry_sdk::trace::SpanData>,
            ) -> opentelemetry_sdk::error::OTelSdkResult {
                for span in batch {
                    self.0.send(span.name.into_owned()).unwrap();
                }
                Ok(())
            }
        }

        let (sender, receiver) = std::sync::mpsc::channel();
        let provider = SdkTracerProvider::builder()
            .with_sampler(Sampler::AlwaysOn)
            .with_simple_exporter(Exporter(sender))
            .build();
        let (filter, _) = tracing_subscriber::reload::Layer::new(
            Targets::new().with_default(tracing::Level::INFO),
        );
        let (layer, handle) = TraceLayer::new(Some(
            tracing_opentelemetry::layer().with_tracer(provider.tracer("export")),
        ));
        tracing::subscriber::with_default(Registry::default().with(filter).with(layer), || {
            let active = tracing::info_span!("admitted");
            handle.set_enabled(false).unwrap();
            let disabled = tracing::info_span!("not_admitted");
            drop(active);
            drop(disabled);
        });
        provider.force_flush().unwrap();
        assert_eq!(receiver.try_iter().collect::<Vec<_>>(), ["admitted"]);
    }

    #[test]
    fn test_trace_switch_concurrent_context_access() {
        let provider = SdkTracerProvider::builder()
            .with_sampler(Sampler::AlwaysOn)
            .build();
        let (filter, _) = tracing_subscriber::reload::Layer::new(
            Targets::new().with_default(tracing::Level::INFO),
        );
        let (layer, handle) = TraceLayer::new(Some(
            tracing_opentelemetry::layer().with_tracer(provider.tracer("concurrent")),
        ));
        let dispatch = tracing::Dispatch::new(Registry::default().with(filter).with(layer));
        let parent = tracing::dispatcher::with_default(&dispatch, || {
            tracing::info_span!("concurrent_parent")
        });
        let context = parent.context();
        let barrier = Barrier::new(4);
        std::thread::scope(|scope| {
            for _ in 0..3 {
                scope.spawn(|| {
                    tracing::dispatcher::with_default(&dispatch, || {
                        let _entered = parent.enter();
                        barrier.wait();
                        for _ in 0..500 {
                            assert_eq!(
                                parent.context().span().span_context(),
                                context.span().span_context()
                            );
                            {
                                let child = tracing::info_span!("concurrent_child");
                                let _entered = child.enter();
                                let _ = child.context();
                                tracing::info!("concurrent event");
                            }
                            assert_eq!(
                                opentelemetry::Context::current().span().span_context(),
                                context.span().span_context()
                            );
                        }
                    });
                });
            }
            barrier.wait();
            for i in 0..1000 {
                handle
                    .set_enabled_with(i % 2 == 0, || panic!("layer must not be replaced"))
                    .unwrap();
            }
        });
    }

    #[test]
    fn test_logging_options_deserialization_default() {
        let json = r#"{}"#;
        let opts: LoggingOptions = serde_json::from_str(json).unwrap();

        assert_eq!(opts.log_format, LogFormat::Text);
        assert_eq!(opts.dir, "");
        assert_eq!(opts.level, None);
        assert!(opts.append_stdout);
        assert!(opts.enable_file_logging);
    }

    #[test]
    fn test_logging_options_deserialization_enable_file_logging() {
        let json = r#"{"enable_file_logging": false}"#;
        let opts: LoggingOptions = serde_json::from_str(json).unwrap();

        assert!(!opts.enable_file_logging);
    }

    #[test]
    fn test_logging_options_deserialization_max_log_dir_size() {
        let json = r#"{"max_log_dir_size": "1MiB"}"#;
        let opts: LoggingOptions = serde_json::from_str(json).unwrap();

        assert_eq!(opts.max_log_dir_size, ReadableSize::mb(1));
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

    #[test]
    fn test_logging_options_partial_eq_all_fields() {
        let base = LoggingOptions::default();

        let mut log_format = base.clone();
        log_format.log_format = LogFormat::Json;
        assert_ne!(base, log_format);

        let mut max_log_files = base.clone();
        max_log_files.max_log_files += 1;
        assert_ne!(base, max_log_files);

        let mut max_log_dir_size = base.clone();
        max_log_dir_size.max_log_dir_size = ReadableSize::mb(1);
        assert_ne!(base, max_log_dir_size);

        let mut otlp_export_protocol = base.clone();
        otlp_export_protocol.otlp_export_protocol = Some(OtlpExportProtocol::Http);
        assert_ne!(base, otlp_export_protocol);

        let mut otlp_headers = base.clone();
        otlp_headers
            .otlp_headers
            .insert("key".to_string(), "value".to_string());
        assert_ne!(base, otlp_headers);
    }
}
