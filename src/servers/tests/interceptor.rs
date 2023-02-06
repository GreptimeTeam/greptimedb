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

use query::parser::QueryLanguage;
use servers::error::{self, Result};
use servers::interceptor::SqlQueryInterceptor;
use session::context::{QueryContext, QueryContextRef};

pub struct RewriteInterceptor;

impl SqlQueryInterceptor for RewriteInterceptor {
    type Error = error::Error;

    fn pre_parsing(
        &self,
        _query: QueryLanguage,
        _query_ctx: QueryContextRef,
    ) -> Result<QueryLanguage> {
        let modified_query = QueryLanguage::Sql("SELECT 1;".to_string());
        Ok(modified_query)
    }
}

#[test]
fn test_default_interceptor_behaviour() {
    let di = RewriteInterceptor;
    let ctx = Arc::new(QueryContext::new());

    let query = QueryLanguage::Promql("blabla[1m]".to_string());
    assert_eq!(
        QueryLanguage::Sql("SELECT 1;".to_string()),
        di.pre_parsing(query, ctx).unwrap()
    );
}
