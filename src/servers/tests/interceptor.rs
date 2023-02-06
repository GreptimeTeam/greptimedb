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
use std::sync::Arc;

use query::interceptor::SqlQueryInterceptor;
use servers::error::{self, Result};
use session::context::{QueryContext, QueryContextRef};

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
    let ctx = Arc::new(QueryContext::new());

    let query = "SELECT 1";
    assert_eq!("SELECT 1;", di.pre_parsing(query, ctx).unwrap());
}
