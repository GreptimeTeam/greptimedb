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

pub(crate) mod arrow_result;
pub(crate) mod csv_result;
pub mod error_result;
pub(crate) mod greptime_manage_resp;
pub mod greptime_result_v1;
pub mod influxdb_result_v1;
pub(crate) mod json_result;
pub(crate) mod prometheus_resp;
pub(crate) mod table_result;
