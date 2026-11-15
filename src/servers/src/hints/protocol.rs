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

//! Protocol identities used by executable hint bindings and discovery.

use serde::Serialize;

/// A request protocol used to select executable hint registrations.
///
/// HTTP and gRPC adapters provide an [`Endpoint`]; discovery projects those
/// bindings onto this protocol identity rather than maintaining a support list.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Protocol {
    Sql,
    Promql,
    LogQuery,
    Jaeger,
    NativeInsert,
    BulkInsert,
    Influxdb,
    Opentsdb,
    PrometheusRemoteWrite,
    PrometheusRemoteRead,
    OtlpMetrics,
    OtlpLogs,
    OtlpTraces,
    OtelArrow,
    Logs,
    Elasticsearch,
    Loki,
    Splunk,
    /// Routes without a public query or ingestion protocol.
    Other,
}

/// Transport-wide hint behavior retained from the existing request adapters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Transport {
    Http,
    Grpc,
}

/// A server entry point passed by an adapter to the registry.
///
/// Keeping transport here preserves narrower HTTP and gRPC support while the
/// public catalog exposes only the associated [`Protocol`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum Endpoint {
    Http(Protocol),
    Grpc(Protocol),
}

impl Endpoint {
    /// Returns the transport that owns compatibility parsing for this request.
    pub(crate) const fn transport(self) -> Transport {
        match self {
            Self::Http(_) => Transport::Http,
            Self::Grpc(_) => Transport::Grpc,
        }
    }

    /// Metadata-only services historically accepted a restricted hint subset.
    pub(crate) const fn is_metadata_only(self) -> bool {
        matches!(self, Self::Grpc(Protocol::OtelArrow | Protocol::BulkInsert))
    }

    /// Returns the protocol shared by execution bindings and discovery.
    pub(crate) const fn protocol(self) -> Protocol {
        match self {
            Self::Http(protocol) | Self::Grpc(protocol) => protocol,
        }
    }
}

pub(crate) const QUERY_ENDPOINTS: &[Endpoint] = &[
    Endpoint::Http(Protocol::Sql),
    Endpoint::Grpc(Protocol::Sql),
    Endpoint::Http(Protocol::Promql),
    Endpoint::Grpc(Protocol::Promql),
    Endpoint::Http(Protocol::LogQuery),
    Endpoint::Http(Protocol::PrometheusRemoteRead),
];

pub(crate) const INGESTION_ENDPOINTS: &[Endpoint] = &[
    Endpoint::Grpc(Protocol::NativeInsert),
    Endpoint::Http(Protocol::Influxdb),
    Endpoint::Http(Protocol::Opentsdb),
    Endpoint::Http(Protocol::PrometheusRemoteWrite),
    Endpoint::Http(Protocol::OtlpMetrics),
    Endpoint::Http(Protocol::OtlpLogs),
    Endpoint::Http(Protocol::OtlpTraces),
    Endpoint::Http(Protocol::Logs),
    Endpoint::Http(Protocol::Elasticsearch),
    Endpoint::Http(Protocol::Loki),
    Endpoint::Http(Protocol::Splunk),
];

pub(crate) const WRITE_ENDPOINTS: &[Endpoint] = &[
    Endpoint::Http(Protocol::Sql),
    Endpoint::Grpc(Protocol::Sql),
    Endpoint::Grpc(Protocol::NativeInsert),
    Endpoint::Grpc(Protocol::BulkInsert),
    Endpoint::Http(Protocol::Influxdb),
    Endpoint::Http(Protocol::Opentsdb),
    Endpoint::Http(Protocol::PrometheusRemoteWrite),
    Endpoint::Http(Protocol::OtlpMetrics),
    Endpoint::Http(Protocol::OtlpLogs),
    Endpoint::Http(Protocol::OtlpTraces),
    Endpoint::Grpc(Protocol::OtelArrow),
    Endpoint::Http(Protocol::Logs),
    Endpoint::Http(Protocol::Elasticsearch),
    Endpoint::Http(Protocol::Loki),
    Endpoint::Http(Protocol::Splunk),
];

/// Protocol paths that can select a physical table's merge policy.
pub(crate) const PHYSICAL_TABLE_ENDPOINTS: &[Endpoint] = &[
    Endpoint::Grpc(Protocol::NativeInsert),
    Endpoint::Http(Protocol::Influxdb),
    Endpoint::Http(Protocol::Opentsdb),
    Endpoint::Http(Protocol::PrometheusRemoteWrite),
    Endpoint::Http(Protocol::OtlpMetrics),
];

/// Bulk ingestion additionally supports the automatic-table-creation switch,
/// without gaining the other table-option hints accepted by row ingestion.
pub(crate) fn auto_create_endpoints() -> &'static [Endpoint] {
    static ENDPOINTS: std::sync::OnceLock<Vec<Endpoint>> = std::sync::OnceLock::new();
    ENDPOINTS.get_or_init(|| {
        let mut endpoints = INGESTION_ENDPOINTS.to_vec();
        endpoints.push(Endpoint::Grpc(Protocol::BulkInsert));
        endpoints
    })
}
