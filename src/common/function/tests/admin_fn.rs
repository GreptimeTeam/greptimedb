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

use common_function::function::FunctionContext;
use common_function::function_factory::ScalarFunctionFactory;
use common_function::handlers::ProcedureServiceHandlerRef;
use common_macro::admin_fn;
use common_query::error::{MissingProcedureServiceHandlerSnafu, Result};
use datafusion_expr::{Signature, Volatility};
use datatypes::value::{Value, ValueRef};
use session::context::QueryContextRef;

// Integration tests compile as a separate crate, matching downstream users of
// the macro. All generated paths must be accessible through `common_function`.
#[admin_fn(
    name = ExternalAdminFunction,
    display_name = external_admin,
    sig_fn = signature,
    ret = uint64,
    user_path = common_function
)]
async fn external_admin(
    _handler: &ProcedureServiceHandlerRef,
    _query_ctx: &QueryContextRef,
    _params: &[ValueRef<'_>],
) -> Result<Value> {
    Ok(Value::from(0u64))
}

fn signature() -> Signature {
    Signature::nullary(Volatility::Volatile)
}

#[test]
fn test_admin_fn_from_external_crate() {
    let factory: ScalarFunctionFactory = ExternalAdminFunction::factory().into();
    let udf = factory.provide(FunctionContext::default());

    assert_eq!(udf.name(), "external_admin");
    assert_eq!(udf.signature(), &signature());
    assert!(udf.as_async().is_some());
}
