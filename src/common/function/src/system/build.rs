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

use std::fmt;
use std::sync::Arc;

use common_query::error::Result;
use common_query::prelude::{Signature, Volatility};
use datatypes::prelude::*;
use datatypes::vectors::{StringVector, VectorRef};

use crate::function::{Function, FunctionContext};

const DEFAULT_VALUE: &str = "unknown";

/// Generates build information  
#[derive(Clone, Debug, Default)]
pub struct BuildFunction;

impl fmt::Display for BuildFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "BUILD")
    }
}

impl Function for BuildFunction {
    fn name(&self) -> &str {
        "build"
    }

    fn return_type(&self, _input_types: &[ConcreteDataType]) -> Result<ConcreteDataType> {
        Ok(ConcreteDataType::string_datatype())
    }

    fn signature(&self) -> Signature {
        Signature::uniform(
            0,
            vec![ConcreteDataType::string_datatype()],
            Volatility::Immutable,
        )
    }

    fn eval(&self, _func_ctx: FunctionContext, _columns: &[VectorRef]) -> Result<VectorRef> {
        let build_info = format!(
            "branch: {}\ncommit: {}\ncommit short: {}\ndirty: {}\nversion: {}",
            build_data::get_git_branch().unwrap_or_else(|_| DEFAULT_VALUE.to_string()),
            build_data::get_git_commit().unwrap_or_else(|_| DEFAULT_VALUE.to_string()),
            build_data::get_git_commit_short().unwrap_or_else(|_| DEFAULT_VALUE.to_string()),
            build_data::get_git_dirty().map_or(DEFAULT_VALUE.to_string(), |v| v.to_string()),
            env!("CARGO_PKG_VERSION")
        );

        let v = Arc::new(StringVector::from(vec![build_info]));
        Ok(v)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_query::prelude::TypeSignature;

    use super::*;
    #[test]
    fn test_build_function() {
        let build = BuildFunction;
        assert_eq!("build", build.name());
        assert_eq!(
            ConcreteDataType::string_datatype(),
            build.return_type(&[]).unwrap()
        );
        assert!(matches!(build.signature(),
                         Signature {
                             type_signature: TypeSignature::Uniform(0, valid_types),
                             volatility: Volatility::Immutable
                         } if  valid_types == vec![ConcreteDataType::string_datatype()]
        ));
        let build_info = format!(
            "branch: {}\ncommit: {}\ncommit short: {}\ndirty: {}\nversion: {}",
            build_data::get_git_branch().unwrap_or_else(|_| DEFAULT_VALUE.to_string()),
            build_data::get_git_commit().unwrap_or_else(|_| DEFAULT_VALUE.to_string()),
            build_data::get_git_commit_short().unwrap_or_else(|_| DEFAULT_VALUE.to_string()),
            build_data::get_git_dirty().map_or(DEFAULT_VALUE.to_string(), |v| v.to_string()),
            env!("CARGO_PKG_VERSION")
        );
        let vector = build.eval(FunctionContext::default(), &[]).unwrap();
        let expect: VectorRef = Arc::new(StringVector::from(vec![build_info]));
        assert_eq!(expect, vector);
    }
}
