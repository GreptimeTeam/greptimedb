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

//! datanode metrics

pub const HANDLE_SQL_ELAPSED: &str = "datanode.handle_sql_elapsed";
pub const HANDLE_PROMQL_ELAPSED: &str = "datanode.handle_promql_elapsed";
pub const HANDLE_REGION_REQUEST_ELAPSED: &str = "datanode.handle_region_request_elapsed";
pub const REGION_REQUEST_TYPE: &str = "datanode.region_request_type";
