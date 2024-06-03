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
use std::any::Any;
use std::fmt;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use datafusion_expr::{
    ColumnarValue as DfColumnarValue,
    ScalarFunctionImplementation as DfScalarFunctionImplementation, ScalarUDF as DfScalarUDF,
    ScalarUDFImpl,
};
use datatypes::arrow::datatypes::DataType;

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
}

#[derive(Clone)]
struct DfUdfAdapter {
    name: String,
    signature: datafusion_expr::Signature,
    return_type: datafusion_expr::ReturnTypeFunction,
    fun: DfScalarFunctionImplementation,
}

impl Debug for DfUdfAdapter {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        f.debug_struct("DfUdfAdapter")
            .field("name", &self.name)
            .field("signature", &self.signature)
            .finish()
    }
}

impl ScalarUDFImpl for DfUdfAdapter {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &datafusion_expr::Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> datafusion_common::Result<DataType> {
        (self.return_type)(arg_types).map(|ty| ty.as_ref().clone())
    }

    fn invoke(&self, args: &[DfColumnarValue]) -> datafusion_common::Result<DfColumnarValue> {
        (self.fun)(args)
    }

    fn invoke_no_args(&self, number_rows: usize) -> datafusion_common::Result<DfColumnarValue> {
        Ok((self.fun)(&[])?.into_array(number_rows)?.into())
    }
}

impl From<ScalarUdf> for DfScalarUDF {
    fn from(udf: ScalarUdf) -> Self {
        DfScalarUDF::new_from_impl(DfUdfAdapter {
            name: udf.name,
            signature: udf.signature.into(),
            return_type: to_df_return_type(udf.return_type),
            fun: to_df_scalar_func(udf.fun),
        })
    }
}

fn to_df_scalar_func(fun: ScalarFunctionImplementation) -> DfScalarFunctionImplementation {
    Arc::new(move |args: &[DfColumnarValue]| {
        let args: Result<Vec<_>> = args.iter().map(TryFrom::try_from).collect();
        let result = fun(&args?);
        result.map(From::from).map_err(|e| e.into())
    })
}
