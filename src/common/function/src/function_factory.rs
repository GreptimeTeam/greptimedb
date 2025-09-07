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

use datafusion_expr::ScalarUDF;

use crate::function::{FunctionContext, FunctionRef};
use crate::scalars::udf::create_udf;

/// A factory for creating `ScalarUDF` that require a function context.
#[derive(Clone)]
pub struct ScalarFunctionFactory {
    pub name: String,
    pub factory: Arc<dyn Fn(FunctionContext) -> ScalarUDF + Send + Sync>,
}

impl ScalarFunctionFactory {
    /// Returns the name of the function.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns a `ScalarUDF` when given a function context.
    pub fn provide(&self, ctx: FunctionContext) -> ScalarUDF {
        (self.factory)(ctx)
    }
}

impl From<ScalarUDF> for ScalarFunctionFactory {
    fn from(df_udf: ScalarUDF) -> Self {
        let name = df_udf.name().to_string();
        let func = Arc::new(move |_ctx| df_udf.clone());
        Self {
            name,
            factory: func,
        }
    }
}

impl From<FunctionRef> for ScalarFunctionFactory {
    fn from(func: FunctionRef) -> Self {
        let name = func.name().to_string();
        let func = Arc::new(move |ctx: FunctionContext| {
            create_udf(func.clone(), ctx.query_ctx, ctx.state)
        });
        Self {
            name,
            factory: func,
        }
    }
}
