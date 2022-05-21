//! Udf module contains foundational types that are used to represent UDFs.
//! It's modifed from datafusion.
use std::fmt;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;

use arrow::datatypes::DataType as ArrowDataType;
use datafusion_expr::{
    ColumnarValue as DfColumnarValue, ReturnTypeFunction as DfReturnTypeFunction,
    ScalarFunctionImplementation as DfScalarFunctionImplementation, ScalarUDF as DfScalarUDF,
};
use datatypes::prelude::{ConcreteDataType, DataType};

use crate::error::Result;
use crate::function::{ReturnTypeFunction, ScalarFunctionImplementation};
use crate::prelude::ColumnarValue;
use crate::signature::Signature;

/// Logical representation of a UDF.
#[derive(Clone)]
pub struct ScalarUDF {
    /// name
    pub name: String,
    /// signature
    pub signature: Signature,
    /// Return type
    pub return_type: ReturnTypeFunction,
    /// actual implementation
    pub fun: ScalarFunctionImplementation,
}

impl Debug for ScalarUDF {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        f.debug_struct("ScalarUDF")
            .field("name", &self.name)
            .field("signature", &self.signature)
            .field("fun", &"<FUNC>")
            .finish()
    }
}

impl ScalarUDF {
    /// Create a new ScalarUDF
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
            &cast_udf_returntype(self.return_type),
            &cast_udf_impl(self.fun),
        )
    }
}

fn cast_udf_returntype(fun: ReturnTypeFunction) -> DfReturnTypeFunction {
    Arc::new(move |data_types: &[ArrowDataType]| {
        let concret_types = data_types
            .iter()
            .map(ConcreteDataType::from_arrow_type)
            .collect::<Vec<ConcreteDataType>>();

        let result = (fun)(&concret_types);

        result
            .map(|t| Arc::new(t.as_arrow_type()))
            .map_err(|e| e.into())
    })
}

fn cast_udf_impl(fun: ScalarFunctionImplementation) -> DfScalarFunctionImplementation {
    Arc::new(move |args: &[DfColumnarValue]| {
        let args: Result<Vec<_>> = args
            .iter()
            .map(ColumnarValue::try_from_df_columnar_value)
            .collect();

        let result = (fun)(&args?);

        result.map(From::from).map_err(|e| e.into())
    })
}
