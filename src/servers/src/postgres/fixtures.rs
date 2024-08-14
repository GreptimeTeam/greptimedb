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

use std::sync::Arc;

use futures::stream;
use pgwire::api::results::{
    DataRowEncoder, DescribePortalResponse, DescribeStatementResponse, FieldFormat, FieldInfo,
    QueryResponse, Response, Tag,
};
use pgwire::api::Type;
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use session::context::QueryContextRef;

/// Process unsupported SQL and return fixed result as a compatibility solution
pub(crate) fn process<'a>(
    query: &str,
    query_ctx: QueryContextRef,
) -> Option<PgWireResult<Vec<Response<'a>>>> {
    dbg!(query);
    if query.trim().starts_with("BEGIN") {
        Some(Ok(vec![Response::Execution(Tag::new("BEGIN"))]))
    } else if query.trim().starts_with("ROLLBACK") {
        Some(Ok(vec![Response::Execution(Tag::new("ROLLBACK"))]))
    } else if query.trim().starts_with("COMMIT") {
        Some(Ok(vec![Response::Execution(Tag::new("COMMIT"))]))
    } else if query.trim().starts_with("SET") {
        Some(Ok(vec![Response::Execution(Tag::new("SET"))]))
    } else if query == "SELECT t.oid, NULL\nFROM pg_type t JOIN pg_namespace ns\n    ON typnamespace = ns.oid\nWHERE typname = 'hstore';\n"{
        let f1 = FieldInfo::new("t.oid".into(), None, None, Type::OID, FieldFormat::Text);
        let f2 = FieldInfo::new("?coulmn?".into(), None, None, Type::VARCHAR, FieldFormat::Text);

        Some(Ok(vec![Response::Query(QueryResponse::new(Arc::new(vec![f1, f2]), stream::iter(vec![])))]))
    } else if query == "show transaction isolation level"{
        let f1 = FieldInfo::new("transaction_isolation".into(), None, None, Type::VARCHAR, FieldFormat::Text);
        let schema = Arc::new(vec![f1]);
        let data = stream::iter(vec![
            {
                let mut encoder = DataRowEncoder::new(schema.clone());
                let _ = encoder.encode_field(&Some("read committed"));
                encoder.finish()
            }

        ]);

        Some(Ok(vec![ Response::Query(QueryResponse::new(schema, data))]))
    } else if query == "show standard_conforming_strings"{
        let f1 = FieldInfo::new("standard_conforming_strings".into(), None, None, Type::VARCHAR, FieldFormat::Text);
        let schema = Arc::new(vec![f1]);
        let data = stream::iter(vec![
            {
                let mut encoder = DataRowEncoder::new(schema.clone());
                let _ = encoder.encode_field(&Some("on"));
                encoder.finish()
            }

        ]);

        Some(Ok(vec![ Response::Query(QueryResponse::new(schema, data))]))
    } else if query == "SELECT nspname FROM pg_namespace WHERE nspname NOT LIKE 'pg_%' ORDER BY nspname" {
        let f1 = FieldInfo::new("nspname".into(), None, None, Type::VARCHAR, FieldFormat::Text);
        let schema = Arc::new(vec![f1]);
        let data = stream::iter(vec![
            {
                let mut encoder = DataRowEncoder::new(schema.clone());
                let _ = encoder.encode_field(&Some(query_ctx.current_schema()));
                encoder.finish()
            }

        ]);

        Some(Ok(vec![ Response::Query(QueryResponse::new(schema, data))]))
    } else {
        None
    }
}

// else if query == "select pg_catalog.version()"{
//         let f1 = FieldInfo::new("version".into(), None, None, Type::VARCHAR, FieldFormat::Text);
//         let schema = Arc::new(vec![f1]);
//         let data = stream::iter(vec![
//             {
//                 let mut encoder = DataRowEncoder::new(schema.clone());
//                 encoder.encode_field(&Some("PostgreSQL 16.3 on x86_64-pc-linux-gnu, compiled by gcc (GCC) 14.1.1 20240720, 64-bit"));
//                 encoder.finish()
//             }

//         ]);

//         Some(Ok(vec![ Response::Query(QueryResponse::new(schema, data))]))
//     }
