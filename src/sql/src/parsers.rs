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

mod alter_parser;
pub(crate) mod copy_parser;
pub(crate) mod create_parser;
pub(crate) mod delete_parser;
pub(crate) mod describe_parser;
pub(crate) mod drop_parser;
pub(crate) mod explain_parser;
pub(crate) mod insert_parser;
pub(crate) mod query_parser;
pub(crate) mod show_parser;
pub(crate) mod tql_parser;
pub(crate) mod truncate_parser;
