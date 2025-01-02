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

//! Transform Substrait into execution plan
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use common_error::ext::BoxedError;
use datatypes::data_type::ConcreteDataType as CDT;
use query::QueryEngine;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
/// note here we are using the `substrait_proto_df` crate from the `substrait` module and
/// rename it to `substrait_proto`
use substrait::substrait_proto_df as substrait_proto;
use substrait_proto::proto::extensions::simple_extension_declaration::MappingType;
use substrait_proto::proto::extensions::SimpleExtensionDeclaration;

use crate::adapter::FlownodeContext;
use crate::error::{Error, NotImplementedSnafu, UnexpectedSnafu};
use crate::expr::{TUMBLE_END, TUMBLE_START};
/// a simple macro to generate a not implemented error
macro_rules! not_impl_err {
    ($($arg:tt)*)  => {
        NotImplementedSnafu {
            reason: format!($($arg)*),
        }.fail()
    };
}

/// generate a plan error
macro_rules! plan_err {
    ($($arg:tt)*)  => {
        PlanSnafu {
            reason: format!($($arg)*),
        }.fail()
    };
}

mod aggr;
mod expr;
mod literal;
mod plan;

pub(crate) use expr::from_scalar_fn_to_df_fn_impl;

/// In Substrait, a function can be define by an u32 anchor, and the anchor can be mapped to a name
///
/// So in substrait plan, a ref to a function can be a single u32 anchor instead of a full name in string
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FunctionExtensions {
    anchor_to_name: BTreeMap<u32, String>,
}

impl FunctionExtensions {
    pub fn from_iter(inner: impl IntoIterator<Item = (u32, impl ToString)>) -> Self {
        Self {
            anchor_to_name: inner.into_iter().map(|(k, s)| (k, s.to_string())).collect(),
        }
    }

    /// Create a new FunctionExtensions from a list of SimpleExtensionDeclaration
    pub fn try_from_proto(extensions: &[SimpleExtensionDeclaration]) -> Result<Self, Error> {
        let mut anchor_to_name = BTreeMap::new();
        for e in extensions {
            match &e.mapping_type {
                Some(ext) => match ext {
                    MappingType::ExtensionFunction(ext_f) => {
                        anchor_to_name.insert(ext_f.function_anchor, ext_f.name.clone());
                    }
                    _ => not_impl_err!("Extension type not supported: {ext:?}")?,
                },
                None => not_impl_err!("Cannot parse empty extension")?,
            }
        }
        Ok(Self { anchor_to_name })
    }

    /// Get the name of a function by it's anchor
    pub fn get(&self, anchor: &u32) -> Option<&String> {
        self.anchor_to_name.get(anchor)
    }

    pub fn inner_ref(&self) -> HashMap<u32, &String> {
        self.anchor_to_name.iter().map(|(k, v)| (*k, v)).collect()
    }
}

/// register flow-specific functions to the query engine
pub fn register_function_to_query_engine(engine: &Arc<dyn QueryEngine>) {
    engine.register_function(Arc::new(TumbleFunction::new("tumble")));
    engine.register_function(Arc::new(TumbleFunction::new(TUMBLE_START)));
    engine.register_function(Arc::new(TumbleFunction::new(TUMBLE_END)));
}

#[derive(Debug)]
pub struct TumbleFunction {
    name: String,
}

impl TumbleFunction {
    fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
        }
    }
}

impl std::fmt::Display for TumbleFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.name.to_ascii_uppercase())
    }
}

impl common_function::function::Function for TumbleFunction {
    fn name(&self) -> &str {
        &self.name
    }

    fn return_type(&self, _input_types: &[CDT]) -> common_query::error::Result<CDT> {
        Ok(CDT::timestamp_millisecond_datatype())
    }

    fn signature(&self) -> common_query::prelude::Signature {
        common_query::prelude::Signature::variadic_any(common_query::prelude::Volatility::Immutable)
    }

    fn eval(
        &self,
        _func_ctx: common_function::function::FunctionContext,
        _columns: &[datatypes::prelude::VectorRef],
    ) -> common_query::error::Result<datatypes::prelude::VectorRef> {
        UnexpectedSnafu {
            reason: "Tumbler function is not implemented for datafusion executor",
        }
        .fail()
        .map_err(BoxedError::new)
        .context(common_query::error::ExecuteSnafu)
    }
}

#[cfg(test)]
mod test {

    use query::parser::QueryLanguageParser;
    use session::context::QueryContext;

    use crate::df_optimizer::apply_df_optimizer;
    use crate::test_utils::create_test_query_engine;

    /// TODO(discord9): add more illegal sql tests
    #[tokio::test]
    async fn test_missing_key_check() {
        let engine = create_test_query_engine();
        let sql = "SELECT avg(number) FROM numbers_with_ts GROUP BY tumble(ts, '1 hour'), number";

        let stmt = QueryLanguageParser::parse_sql(sql, &QueryContext::arc()).unwrap();
        let plan = engine
            .planner()
            .plan(&stmt, QueryContext::arc())
            .await
            .unwrap();
        let plan = apply_df_optimizer(plan).await;

        assert!(plan.is_err());
    }
}
