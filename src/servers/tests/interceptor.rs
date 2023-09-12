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

use std::borrow::Cow;

use api::v1::greptime_request::Request;
use api::v1::{InsertRequest, InsertRequests};
use common_query::Output;
use query::parser::PromQuery;
use servers::error::{self, InternalSnafu, NotSupportedSnafu, Result};
use servers::interceptor::{GrpcQueryInterceptor, PromQueryInterceptor, SqlQueryInterceptor};
use session::context::{QueryContext, QueryContextRef};
use snafu::ensure;

pub struct NoopInterceptor;

impl SqlQueryInterceptor for NoopInterceptor {
    type Error = error::Error;

    fn pre_parsing<'a>(&self, query: &'a str, _query_ctx: QueryContextRef) -> Result<Cow<'a, str>> {
        let modified_query = format!("{query};");
        Ok(Cow::Owned(modified_query))
    }
}

#[test]
fn test_default_interceptor_behaviour() {
    let di = NoopInterceptor;
    let ctx = QueryContext::arc();

    let query = "SELECT 1";
    assert_eq!("SELECT 1;", di.pre_parsing(query, ctx).unwrap());
}

impl GrpcQueryInterceptor for NoopInterceptor {
    type Error = error::Error;

    fn pre_execute(
        &self,
        req: &Request,
        _query_ctx: QueryContextRef,
    ) -> std::result::Result<(), Self::Error> {
        match req {
            Request::Inserts(insert) => {
                ensure!(
                    insert.inserts.iter().all(|x| x.row_count > 0),
                    NotSupportedSnafu { feat: "" }
                )
            }
            _ => {
                unreachable!()
            }
        };
        Ok(())
    }
}

#[test]
fn test_grpc_interceptor() {
    let di = NoopInterceptor;
    let ctx = QueryContext::arc();

    let req = Request::Inserts(InsertRequests {
        inserts: vec![InsertRequest::default()],
    });

    let fail = GrpcQueryInterceptor::pre_execute(&di, &req, ctx.clone());
    assert!(fail.is_err());

    let req = Request::Inserts(InsertRequests::default());
    GrpcQueryInterceptor::pre_execute(&di, &req, ctx).unwrap();
}

impl PromQueryInterceptor for NoopInterceptor {
    type Error = error::Error;

    fn pre_execute(
        &self,
        query: &PromQuery,
        _query_ctx: QueryContextRef,
    ) -> std::result::Result<(), Self::Error> {
        match query.query.as_str() {
            "up" => InternalSnafu { err_msg: "test" }.fail(),
            _ => Ok(()),
        }
    }

    fn post_execute(
        &self,
        output: Output,
        _query_ctx: QueryContextRef,
    ) -> std::result::Result<Output, Self::Error> {
        match output {
            Output::AffectedRows(1) => Ok(Output::AffectedRows(2)),
            _ => Ok(output),
        }
    }
}

#[test]
fn test_prom_interceptor() {
    let di = NoopInterceptor;
    let ctx = QueryContext::arc();

    let query = PromQuery {
        query: "up".to_string(),
        ..Default::default()
    };

    let fail = PromQueryInterceptor::pre_execute(&di, &query, ctx.clone());
    assert!(fail.is_err());

    let output = Output::AffectedRows(1);
    let two = PromQueryInterceptor::post_execute(&di, output, ctx);
    assert!(two.is_ok());
    matches!(two.unwrap(), Output::AffectedRows(2));
}
