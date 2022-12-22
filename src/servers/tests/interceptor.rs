use std::borrow::Cow;
use std::sync::Arc;

use servers::error::Result;
use servers::interceptor::SqlQueryInterceptor;
use session::context::{QueryContext, QueryContextRef};

pub struct NoopInterceptor;

impl SqlQueryInterceptor for NoopInterceptor {
    fn pre_parsing<'a>(&self, query: &'a str, _query_ctx: QueryContextRef) -> Result<Cow<'a, str>> {
        let modified_query = format!("{};", query);
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
