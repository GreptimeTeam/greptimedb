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

//! client metrics
pub const METRIC_GRPC_CREATE_TABLE: &str = "grpc.create_table";
pub const METRIC_GRPC_PROMQL_RANGE_QUERY: &str = "grpc.promql.range_query";
pub const METRIC_GRPC_INSERT: &str = "grpc.insert";
pub const METRIC_GRPC_DELETE: &str = "grpc.delete";
pub const METRIC_GRPC_SQL: &str = "grpc.sql";
pub const METRIC_GRPC_LOGICAL_PLAN: &str = "grpc.logical_plan";
pub const METRIC_GRPC_ALTER: &str = "grpc.alter";
pub const METRIC_GRPC_DROP_TABLE: &str = "grpc.drop_table";
pub const METRIC_GRPC_TRUNCATE_TABLE: &str = "grpc.truncate_table";
pub const METRIC_GRPC_DO_GET: &str = "grpc.do_get";
pub(crate) const METRIC_REGION_REQUEST_GRPC: &str = "grpc.region_request";
