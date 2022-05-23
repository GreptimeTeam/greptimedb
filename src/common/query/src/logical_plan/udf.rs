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
            &to_df_returntype(self.return_type),
            &to_df_scalar_func(self.fun),
        )
    }
}

fn to_df_returntype(fun: ReturnTypeFunction) -> DfReturnTypeFunction {
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

fn to_df_scalar_func(fun: ScalarFunctionImplementation) -> DfScalarFunctionImplementation {
    Arc::new(move |args: &[DfColumnarValue]| {
        let args: Result<Vec<_>> = args.iter().map(TryFrom::try_from).collect();

        let result = (fun)(&args?);

        result.map(From::from).map_err(|e| e.into())
    })
}
