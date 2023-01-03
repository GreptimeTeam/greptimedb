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

//! Udf module contains foundational types that are used to represent UDFs.
//! It's modified from datafusion.
use std::fmt;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use datafusion_expr::{
    ColumnarValue as DfColumnarValue,
    ScalarFunctionImplementation as DfScalarFunctionImplementation, ScalarUDF as DfScalarUDF,
};

use crate::error::Result;
use crate::function::{ReturnTypeFunction, ScalarFunctionImplementation};
use crate::prelude::to_df_return_type;
use crate::signature::Signature;

/// Logical representation of a UDF.
#[derive(Clone)]
pub struct ScalarUdf {
    /// name
    pub name: String,
    /// signature
    pub signature: Signature,
    /// Return type
    pub return_type: ReturnTypeFunction,
    /// actual implementation
    pub fun: ScalarFunctionImplementation,
}

impl Debug for ScalarUdf {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        f.debug_struct("ScalarUdf")
            .field("name", &self.name)
            .field("signature", &self.signature)
            .field("fun", &"<FUNC>")
            .finish()
    }
}

impl ScalarUdf {
    /// Create a new ScalarUdf
    pub fn new(
        name: &str,
        signature: &Signature,
        return_type: &ReturnTypeFunction,
        fun: &ScalarFunctionImplementation,
    ) -> Self {
        Self {
            name: name.to_owned(),
            signature: signature.clone(),
            return_type: return_type.clone(),
            fun: fun.clone(),
        }
    }

    /// Cast self into datafusion UDF.
    pub fn into_df_udf(self) -> DfScalarUDF {
        DfScalarUDF::new(
            &self.name,
            &self.signature.into(),
            &to_df_return_type(self.return_type),
            &to_df_scalar_func(self.fun),
        )
    }
}

fn to_df_scalar_func(fun: ScalarFunctionImplementation) -> DfScalarFunctionImplementation {
    Arc::new(move |args: &[DfColumnarValue]| {
        let args: Result<Vec<_>> = args.iter().map(TryFrom::try_from).collect();

        let result = (fun)(&args?);

        result.map(From::from).map_err(|e| e.into())
    })
}
