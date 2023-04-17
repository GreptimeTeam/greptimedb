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
pub const METRIC_CREATE_TABLE: &str = "create.table";
pub const METRIC_PROMQL_RANGE_QUERY: &str = "promql.range.query";
pub const METRIC_INSERT: &str = "insert";
pub const METRIC_SQL: &str = "sql";
pub const METRIC_LOGICAL_PLAN: &str = "logical.plan";
pub const METRIC_ALTER: &str = "alter";
pub const METRIC_DROP_TABLE: &str = "drop.table";
pub const METRIC_FLUSH_TABLE: &str = "flush.table";
pub const METRIC_GRPC_DO_GET: &str = "grpc.do.get";
