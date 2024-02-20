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

pub mod http_headers {
    // New HTTP headers would better distinguish use cases among:
    // * GreptimeDB
    // * GreptimeCloud
    // * ...
    //
    // And thus trying to use:
    // * x-greptime-db-xxx
    // * x-greptime-cloud-xxx
    //
    // ... accordingly
    //
    // Most of the headers are for GreptimeDB and thus using `x-greptime-db-` as prefix.
    // Only use `x-greptime-cloud` when it's intentionally used by GreptimeCloud.

    // LEGACY HEADERS - KEEP IT UNMODIFIED
    pub const GREPTIME_DB_HEADER_FORMAT: &str = "x-greptime-format";
    pub const GREPTIME_DB_HEADER_EXECUTION_TIME: &str = "x-greptime-execution-time";
    pub const GREPTIME_DB_HEADER_METRICS: &str = "x-greptime-metrics";
    pub const GREPTIME_DB_HEADER_NAME: &str = "x-greptime-db-name";
    pub const GREPTIME_TIMEZONE_HEADER_NAME: &str = "x-greptime-timezone";
    pub const GREPTIME_DB_HEADER_ERROR_CODE: &str = "x-greptime-err-code";
    pub const GREPTIME_DB_HEADER_ERROR_MSG: &str = "x-greptime-err-msg";
}
