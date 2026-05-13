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

use api::v1::ColumnDataType;
use opentelemetry_semantic_conventions::{attribute, resource};

/// Returns fixed scalar types for flattened trace semconv columns.
///
/// The mapping is maintained from the official OpenTelemetry semantic
/// conventions docs under `docs/` and `docs/registry/attributes/` in:
/// https://github.com/open-telemetry/semantic-conventions/tree/main/docs
///
/// Only attributes whose docs mark them as `Stable` or `Release Candidate` are
/// included here. `Development` and lower-stability attributes must keep the
/// dynamic reconciliation path instead.
pub(super) fn trace_semconv_fixed_type(column_name: &str) -> Option<ColumnDataType> {
    if let Some(resource_attribute) = column_name.strip_prefix("resource_attributes.") {
        return match resource_attribute {
            // Service resource attributes:
            // https://github.com/open-telemetry/semantic-conventions/blob/main/docs/resource/service.md
            // https://github.com/open-telemetry/semantic-conventions/blob/main/docs/registry/attributes/service.md
            resource::SERVICE_NAME
            | resource::SERVICE_INSTANCE_ID
            | resource::SERVICE_NAMESPACE
            | resource::SERVICE_VERSION => Some(ColumnDataType::String),

            // Telemetry SDK resource attributes:
            // https://github.com/open-telemetry/semantic-conventions/blob/main/docs/resource/README.md
            // https://github.com/open-telemetry/semantic-conventions/blob/main/docs/registry/attributes/telemetry.md
            resource::TELEMETRY_SDK_LANGUAGE
            | resource::TELEMETRY_SDK_NAME
            | resource::TELEMETRY_SDK_VERSION => Some(ColumnDataType::String),

            // Container resource attributes:
            // https://github.com/open-telemetry/semantic-conventions/blob/main/docs/resource/container.md
            resource::CONTAINER_ID => Some(ColumnDataType::String),

            // Browser resource attributes:
            // https://github.com/open-telemetry/semantic-conventions/blob/main/docs/resource/browser.md
            resource::USER_AGENT_ORIGINAL => Some(ColumnDataType::String),

            // Kubernetes resource attributes:
            // https://github.com/open-telemetry/semantic-conventions/blob/main/docs/resource/k8s/README.md
            resource::K8S_CLUSTER_NAME
            | resource::K8S_CLUSTER_UID
            | resource::K8S_CONTAINER_NAME
            | resource::K8S_CRONJOB_NAME
            | resource::K8S_CRONJOB_UID
            | resource::K8S_DAEMONSET_NAME
            | resource::K8S_DAEMONSET_UID
            | resource::K8S_DEPLOYMENT_NAME
            | resource::K8S_DEPLOYMENT_UID
            | resource::K8S_JOB_NAME
            | resource::K8S_JOB_UID
            | resource::K8S_NAMESPACE_NAME
            | resource::K8S_NODE_NAME
            | resource::K8S_NODE_UID
            | resource::K8S_POD_NAME
            | resource::K8S_POD_UID
            | resource::K8S_REPLICASET_NAME
            | resource::K8S_REPLICASET_UID
            | resource::K8S_STATEFULSET_NAME
            | resource::K8S_STATEFULSET_UID
            // The current docs include these `k8s.pod.*` keys, but crate 0.31
            // does not export constants for them yet.
            | "k8s.pod.hostname"
            | "k8s.pod.ip"
            | "k8s.pod.start_time" => Some(ColumnDataType::String),
            resource::K8S_CONTAINER_RESTART_COUNT => Some(ColumnDataType::Int64),

            _ => None,
        };
    }

    if let Some(span_attribute) = column_name.strip_prefix("span_attributes.") {
        return match span_attribute {
            // General client, server, and network attributes:
            // https://github.com/open-telemetry/semantic-conventions/blob/main/docs/general/attributes.md
            attribute::CLIENT_ADDRESS
            | attribute::SERVER_ADDRESS
            | attribute::NETWORK_LOCAL_ADDRESS
            | attribute::NETWORK_PEER_ADDRESS
            | attribute::NETWORK_PROTOCOL_NAME
            | attribute::NETWORK_PROTOCOL_VERSION
            | attribute::NETWORK_TRANSPORT
            | attribute::NETWORK_TYPE => Some(ColumnDataType::String),
            attribute::CLIENT_PORT
            | attribute::SERVER_PORT
            | attribute::NETWORK_LOCAL_PORT
            | attribute::NETWORK_PEER_PORT => Some(ColumnDataType::Int64),

            // HTTP attributes:
            // https://github.com/open-telemetry/semantic-conventions/blob/main/docs/http/http-spans.md
            // https://github.com/open-telemetry/semantic-conventions/blob/main/docs/registry/attributes/http.md
            attribute::HTTP_REQUEST_METHOD
            | attribute::HTTP_REQUEST_METHOD_ORIGINAL
            | attribute::HTTP_ROUTE
            | attribute::URL_FULL
            | attribute::URL_PATH
            | attribute::URL_QUERY
            | attribute::URL_SCHEME
            | attribute::USER_AGENT_ORIGINAL => Some(ColumnDataType::String),
            attribute::HTTP_REQUEST_RESEND_COUNT | attribute::HTTP_RESPONSE_STATUS_CODE => {
                Some(ColumnDataType::Int64)
            }

            // RPC attributes:
            // https://github.com/open-telemetry/semantic-conventions/blob/main/docs/rpc/rpc-spans.md
            // https://github.com/open-telemetry/semantic-conventions/blob/main/docs/registry/attributes/rpc.md
            attribute::RPC_METHOD
            | attribute::RPC_SYSTEM
            // The current docs renamed this attribute to `rpc.system.name`,
            // but crate 0.31 still exports `RPC_SYSTEM = "rpc.system"`.
            | "rpc.system.name"
            | "rpc.method_original"
            | "rpc.response.status_code" => Some(ColumnDataType::String),

            // General database attributes:
            // https://github.com/open-telemetry/semantic-conventions/blob/main/docs/db/database-spans.md
            // https://github.com/open-telemetry/semantic-conventions/blob/main/docs/registry/attributes/db.md
            attribute::DB_COLLECTION_NAME
            | attribute::DB_NAMESPACE
            | attribute::DB_OPERATION_NAME
            | attribute::DB_QUERY_SUMMARY
            | attribute::DB_QUERY_TEXT
            | attribute::DB_RESPONSE_STATUS_CODE
            | attribute::DB_STORED_PROCEDURE_NAME
            | attribute::DB_SYSTEM_NAME => Some(ColumnDataType::String),
            attribute::DB_OPERATION_BATCH_SIZE => Some(ColumnDataType::Int64),

            // Error attributes:
            // https://github.com/open-telemetry/semantic-conventions/blob/main/docs/registry/attributes/error.md
            attribute::ERROR_TYPE => Some(ColumnDataType::String),

            _ => None,
        };
    }

    None
}

#[cfg(test)]
mod tests {
    use api::v1::ColumnDataType;
    use opentelemetry_semantic_conventions::{attribute, resource};

    use super::trace_semconv_fixed_type;

    fn resource_column(key: &str) -> String {
        format!("resource_attributes.{key}")
    }

    fn span_column(key: &str) -> String {
        format!("span_attributes.{key}")
    }

    #[test]
    fn test_trace_semconv_fixed_type_includes_stable_service_key() {
        assert_eq!(
            trace_semconv_fixed_type(&resource_column(resource::SERVICE_NAME)),
            Some(ColumnDataType::String)
        );
        assert_eq!(
            trace_semconv_fixed_type(&resource_column(resource::SERVICE_VERSION)),
            Some(ColumnDataType::String)
        );
        assert_eq!(
            trace_semconv_fixed_type(&resource_column(resource::SERVICE_INSTANCE_ID)),
            Some(ColumnDataType::String)
        );
        assert_eq!(
            trace_semconv_fixed_type(&resource_column(resource::SERVICE_NAMESPACE)),
            Some(ColumnDataType::String)
        );
    }

    #[test]
    fn test_trace_semconv_fixed_type_includes_http_server_and_error_keys() {
        assert_eq!(
            trace_semconv_fixed_type(&span_column(attribute::HTTP_RESPONSE_STATUS_CODE)),
            Some(ColumnDataType::Int64)
        );
        assert_eq!(
            trace_semconv_fixed_type(&span_column(attribute::SERVER_PORT)),
            Some(ColumnDataType::Int64)
        );
        assert_eq!(
            trace_semconv_fixed_type(&span_column(attribute::ERROR_TYPE)),
            Some(ColumnDataType::String)
        );
        assert_eq!(
            trace_semconv_fixed_type(&span_column(attribute::CLIENT_ADDRESS)),
            Some(ColumnDataType::String)
        );
        assert_eq!(
            trace_semconv_fixed_type(&span_column(attribute::CLIENT_PORT)),
            Some(ColumnDataType::Int64)
        );
        assert_eq!(
            trace_semconv_fixed_type(&span_column(attribute::URL_FULL)),
            Some(ColumnDataType::String)
        );
        assert_eq!(
            trace_semconv_fixed_type(&span_column(attribute::URL_PATH)),
            Some(ColumnDataType::String)
        );
        assert_eq!(
            trace_semconv_fixed_type(&span_column(attribute::URL_QUERY)),
            Some(ColumnDataType::String)
        );
        assert_eq!(
            trace_semconv_fixed_type(&span_column(attribute::URL_SCHEME)),
            Some(ColumnDataType::String)
        );
        assert_eq!(
            trace_semconv_fixed_type(&span_column(attribute::USER_AGENT_ORIGINAL)),
            Some(ColumnDataType::String)
        );
    }

    #[test]
    fn test_trace_semconv_fixed_type_includes_rc_rpc_key() {
        assert_eq!(
            trace_semconv_fixed_type(&span_column(attribute::RPC_SYSTEM)),
            Some(ColumnDataType::String)
        );
        assert_eq!(
            trace_semconv_fixed_type("span_attributes.rpc.system.name"),
            Some(ColumnDataType::String)
        );
        assert_eq!(
            trace_semconv_fixed_type(&span_column("rpc.response.status_code")),
            Some(ColumnDataType::String)
        );
    }

    #[test]
    fn test_trace_semconv_fixed_type_includes_db_and_network_keys() {
        assert_eq!(
            trace_semconv_fixed_type(&span_column(attribute::DB_SYSTEM_NAME)),
            Some(ColumnDataType::String)
        );
        assert_eq!(
            trace_semconv_fixed_type(&span_column(attribute::DB_OPERATION_BATCH_SIZE)),
            Some(ColumnDataType::Int64)
        );
        assert_eq!(
            trace_semconv_fixed_type(&span_column(attribute::NETWORK_PEER_PORT)),
            Some(ColumnDataType::Int64)
        );
    }

    #[test]
    fn test_trace_semconv_fixed_type_includes_resource_semconv_keys() {
        assert_eq!(
            trace_semconv_fixed_type(&resource_column(resource::CONTAINER_ID)),
            Some(ColumnDataType::String)
        );
        assert_eq!(
            trace_semconv_fixed_type(&resource_column(resource::K8S_CONTAINER_RESTART_COUNT)),
            Some(ColumnDataType::Int64)
        );
        assert_eq!(
            trace_semconv_fixed_type(&resource_column(resource::TELEMETRY_SDK_LANGUAGE)),
            Some(ColumnDataType::String)
        );
        assert_eq!(
            trace_semconv_fixed_type(&resource_column(resource::TELEMETRY_SDK_NAME)),
            Some(ColumnDataType::String)
        );
        assert_eq!(
            trace_semconv_fixed_type(&resource_column(resource::TELEMETRY_SDK_VERSION)),
            Some(ColumnDataType::String)
        );
    }

    #[test]
    fn test_trace_semconv_fixed_type_excludes_development_keys() {
        assert_eq!(
            trace_semconv_fixed_type(&span_column("messaging.system")),
            None
        );
        assert_eq!(
            trace_semconv_fixed_type(&span_column("rpc.request.metadata.x-request-id")),
            None
        );
        assert_eq!(
            trace_semconv_fixed_type(&resource_column("k8s.pod.label.app")),
            None
        );
    }

    #[test]
    fn test_trace_semconv_fixed_type_unknown_key() {
        assert_eq!(trace_semconv_fixed_type(&span_column("custom.attr")), None);
    }
}
