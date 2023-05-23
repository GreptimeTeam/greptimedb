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

pub(crate) const METRIC_DB_LABEL: &str = "db";
pub(crate) const METRIC_CODE_LABEL: &str = "code";

pub(crate) const METRIC_HTTP_SQL_ELAPSED: &str = "servers.http_sql_elapsed";
pub(crate) const METRIC_HTTP_PROMQL_ELAPSED: &str = "servers.http_promql_elapsed";
pub(crate) const METRIC_AUTH_FAILURE: &str = "servers.auth_failure_count";
pub(crate) const METRIC_HTTP_INFLUXDB_WRITE_ELAPSED: &str = "servers.http_influxdb_write_elapsed";
pub(crate) const METRIC_HTTP_PROMETHEUS_WRITE_ELAPSED: &str =
    "servers.http_prometheus_write_elapsed";
pub(crate) const METRIC_HTTP_PROMETHEUS_READ_ELAPSED: &str = "servers.http_prometheus_read_elapsed";
pub(crate) const METRIC_TCP_OPENTSDB_LINE_WRITE_ELAPSED: &str =
    "servers.opentsdb_line_write_elapsed";

pub(crate) const METRIC_MYSQL_CONNECTIONS: &str = "servers.mysql_connection_count";
pub(crate) const METRIC_MYSQL_QUERY_TIMER: &str = "servers.mysql_query_elapsed";
pub(crate) const METRIC_MYSQL_SUBPROTOCOL_LABEL: &str = "subprotocol";
pub(crate) const METRIC_MYSQL_BINQUERY: &str = "binquery";
pub(crate) const METRIC_MYSQL_TEXTQUERY: &str = "textquery";
pub(crate) const METRIC_MYSQL_PREPARED_COUNT: &str = "servers.mysql_prepared_count";

pub(crate) const METRIC_POSTGRES_CONNECTIONS: &str = "servers.postgres_connection_count";
pub(crate) const METRIC_POSTGRES_QUERY_TIMER: &str = "servers.postgres_query_elapsed";
pub(crate) const METRIC_POSTGRES_SUBPROTOCOL_LABEL: &str = "subprotocol";
pub(crate) const METRIC_POSTGRES_SIMPLE_QUERY: &str = "simple";
pub(crate) const METRIC_POSTGRES_EXTENDED_QUERY: &str = "extended";
pub(crate) const METRIC_POSTGRES_PREPARED_COUNT: &str = "servers.postgres_prepared_count";

pub(crate) const METRIC_SERVER_GRPC_DB_REQUEST_TIMER: &str = "servers.grpc.db_request_elapsed";
pub(crate) const METRIC_SERVER_GRPC_PROM_REQUEST_TIMER: &str = "servers.grpc.prom_request_elapsed";
