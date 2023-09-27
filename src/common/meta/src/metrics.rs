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

pub const METRIC_META_TXN_REQUEST: &str = "meta.txn_request";

pub(crate) const METRIC_META_CREATE_CATALOG: &str = "meta.create_catalog";
pub(crate) const METRIC_META_CREATE_SCHEMA: &str = "meta.create_schema";
pub(crate) const METRIC_META_PROCEDURE_CREATE_TABLE: &str = "meta.procedure.create_table";
pub(crate) const METRIC_META_PROCEDURE_DROP_TABLE: &str = "meta.procedure.drop_table";
pub(crate) const METRIC_META_PROCEDURE_ALTER_TABLE: &str = "meta.procedure.alter_table";
pub(crate) const METRIC_META_PROCEDURE_TRUNCATE_TABLE: &str = "meta.procedure.truncate_table";
