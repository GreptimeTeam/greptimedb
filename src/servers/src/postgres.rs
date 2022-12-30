// Copyright 2022 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

mod auth_handler;
mod handler;
mod server;

pub(crate) const METADATA_USER: &str = "user";
pub(crate) const METADATA_DATABASE: &str = "database";
/// key to store our parsed catalog
pub(crate) const METADATA_CATALOG: &str = "catalog";
/// key to store our parsed schema
pub(crate) const METADATA_SCHEMA: &str = "schema";

pub use server::PostgresServer;
