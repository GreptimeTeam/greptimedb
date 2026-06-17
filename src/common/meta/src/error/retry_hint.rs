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

mod etcd;
mod mysql;

#[cfg(feature = "pg_kvbackend")]
mod postgres;

pub use etcd::retry_hint_from_etcd_error;
#[cfg(feature = "mysql_kvbackend")]
pub use mysql::retry_hint_from_sqlx_error;
#[cfg(feature = "pg_kvbackend")]
pub use postgres::{retry_hint_from_postgres_error, retry_hint_from_postgres_pool_error};
