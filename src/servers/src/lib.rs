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

#![feature(assert_matches)]
#![feature(try_blocks)]

use common_catalog::consts::DEFAULT_CATALOG_NAME;
use serde::{Deserialize, Serialize};

pub mod auth;
pub mod configurator;
pub mod error;
pub mod grpc;
pub mod http;
pub mod influxdb;
pub mod interceptor;
pub mod line_writer;
mod metrics;
pub mod metrics_handler;
pub mod mysql;
pub mod opentsdb;
pub mod postgres;
pub mod prom;
pub mod prometheus;
pub mod query_handler;
pub mod server;
mod shutdown;
pub mod tls;

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum Mode {
    Standalone,
    Distributed,
}

/// Attempt to parse catalog and schema from given database name
///
/// The database name may come from different sources:
///
/// - MySQL `schema` name in MySQL protocol login request: it's optional and user
/// and switch database using `USE` command
/// - Postgres `database` parameter in Postgres wire protocol, required
/// - HTTP RESTful API: the database parameter, optional
/// - gRPC: the dbname field in header, optional but has a higher priority than
/// original catalog/schema
///
/// When database name is provided, we attempt to parse catalog and schema from
/// it. We assume the format `[<catalog>-]<schema>`:
///
/// - If `[<catalog>-]` part is not provided, we use whole database name as
/// schema name
/// - if `[<catalog>-]` is provided, we split database name with `-` and use
/// `<catalog>` and `<schema>`.
pub fn parse_catalog_and_schema_from_client_database_name(db: &str) -> (&str, &str) {
    let parts = db.splitn(2, '-').collect::<Vec<&str>>();
    if parts.len() == 2 {
        (parts[0], parts[1])
    } else {
        (DEFAULT_CATALOG_NAME, db)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_catalog_and_schema() {
        assert_eq!(
            (DEFAULT_CATALOG_NAME, "fullschema"),
            parse_catalog_and_schema_from_client_database_name("fullschema")
        );

        assert_eq!(
            ("catalog", "schema"),
            parse_catalog_and_schema_from_client_database_name("catalog-schema")
        );

        assert_eq!(
            ("catalog", "schema1-schema2"),
            parse_catalog_and_schema_from_client_database_name("catalog-schema1-schema2")
        );
    }
}
