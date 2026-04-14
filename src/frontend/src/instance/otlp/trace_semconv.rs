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
            resource::SERVICE_NAME | resource::SERVICE_VERSION => Some(ColumnDataType::String),
            _ => None,
        };
    }

    if let Some(span_attribute) = column_name.strip_prefix("span_attributes.") {
        return match span_attribute {
            // HTTP attributes:
            // https://github.com/open-telemetry/semantic-conventions/blob/main/docs/registry/attributes/http.md
            attribute::HTTP_REQUEST_METHOD
            | attribute::HTTP_REQUEST_METHOD_ORIGINAL
            | attribute::HTTP_ROUTE => Some(ColumnDataType::String),
            attribute::HTTP_REQUEST_RESEND_COUNT | attribute::HTTP_RESPONSE_STATUS_CODE => {
                Some(ColumnDataType::Int64)
            }

            // Server attributes:
            // https://github.com/open-telemetry/semantic-conventions/blob/main/docs/registry/attributes/server.md
            attribute::SERVER_ADDRESS => Some(ColumnDataType::String),
            attribute::SERVER_PORT => Some(ColumnDataType::Int64),

            // RPC attributes:
            // https://github.com/open-telemetry/semantic-conventions/blob/main/docs/registry/attributes/rpc.md
            attribute::RPC_METHOD
            | attribute::RPC_SYSTEM
            | "rpc.method_original"
            | "rpc.response.status_code" => Some(ColumnDataType::String),

            // General database attributes:
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

            // Network attributes:
            // https://github.com/open-telemetry/semantic-conventions/blob/main/docs/registry/attributes/network.md
            attribute::NETWORK_LOCAL_ADDRESS
            | attribute::NETWORK_PEER_ADDRESS
            | attribute::NETWORK_PROTOCOL_NAME
            | attribute::NETWORK_PROTOCOL_VERSION
            | attribute::NETWORK_TRANSPORT
            | attribute::NETWORK_TYPE => Some(ColumnDataType::String),
            attribute::NETWORK_LOCAL_PORT | attribute::NETWORK_PEER_PORT => {
                Some(ColumnDataType::Int64)
            }

            _ => None,
        };
    }

    None
}

#[cfg(test)]
mod tests {
    use api::v1::ColumnDataType;

    use super::trace_semconv_fixed_type;

    #[test]
    fn test_trace_semconv_fixed_type_includes_stable_service_key() {
        assert_eq!(
            trace_semconv_fixed_type("resource_attributes.service.name"),
            Some(ColumnDataType::String)
        );
        assert_eq!(
            trace_semconv_fixed_type("resource_attributes.service.version"),
            Some(ColumnDataType::String)
        );
    }

    #[test]
    fn test_trace_semconv_fixed_type_includes_http_server_and_error_keys() {
        assert_eq!(
            trace_semconv_fixed_type("span_attributes.http.response.status_code"),
            Some(ColumnDataType::Int64)
        );
        assert_eq!(
            trace_semconv_fixed_type("span_attributes.server.port"),
            Some(ColumnDataType::Int64)
        );
        assert_eq!(
            trace_semconv_fixed_type("span_attributes.error.type"),
            Some(ColumnDataType::String)
        );
    }

    #[test]
    fn test_trace_semconv_fixed_type_includes_rc_rpc_key() {
        assert_eq!(
            trace_semconv_fixed_type("span_attributes.rpc.system"),
            Some(ColumnDataType::String)
        );
        assert_eq!(
            trace_semconv_fixed_type("span_attributes.rpc.response.status_code"),
            Some(ColumnDataType::String)
        );
    }

    #[test]
    fn test_trace_semconv_fixed_type_includes_db_and_network_keys() {
        assert_eq!(
            trace_semconv_fixed_type("span_attributes.db.system.name"),
            Some(ColumnDataType::String)
        );
        assert_eq!(
            trace_semconv_fixed_type("span_attributes.db.operation.batch.size"),
            Some(ColumnDataType::Int64)
        );
        assert_eq!(
            trace_semconv_fixed_type("span_attributes.network.peer.port"),
            Some(ColumnDataType::Int64)
        );
    }

    #[test]
    fn test_trace_semconv_fixed_type_excludes_development_keys() {
        assert_eq!(
            trace_semconv_fixed_type("resource_attributes.service.instance.id"),
            None
        );
        assert_eq!(
            trace_semconv_fixed_type("span_attributes.messaging.system"),
            None
        );
        assert_eq!(
            trace_semconv_fixed_type("span_attributes.rpc.request.metadata.x-request-id"),
            None
        );
    }

    #[test]
    fn test_trace_semconv_fixed_type_unknown_key() {
        assert_eq!(
            trace_semconv_fixed_type("span_attributes.custom.attr"),
            None
        );
    }
}
