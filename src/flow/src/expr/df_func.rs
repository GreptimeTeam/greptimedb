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

//! Porting Datafusion scalar function to our scalar function to be used in dataflow

use std::sync::Arc;

use arrow::array::RecordBatchOptions;
use bytes::BytesMut;
use common_error::ext::BoxedError;
use common_recordbatch::DfRecordBatch;
use common_telemetry::debug;
use datafusion_physical_expr::PhysicalExpr;
use datatypes::data_type::DataType;
use datatypes::value::Value;
use datatypes::vectors::VectorRef;
use prost::Message;
use snafu::{IntoError, ResultExt};
use substrait::error::{DecodeRelSnafu, EncodeRelSnafu};
use substrait::substrait_proto_df::proto::expression::ScalarFunction;

use crate::error::Error;
use crate::expr::error::{
    ArrowSnafu, DatafusionSnafu as EvalDatafusionSnafu, EvalError, ExternalSnafu,
    InvalidArgumentSnafu,
};
use crate::expr::{Batch, ScalarExpr};
use crate::repr::RelationDesc;
use crate::transform::{from_scalar_fn_to_df_fn_impl, FunctionExtensions};

/// A way to represent a scalar function that is implemented in Datafusion
#[derive(Debug, Clone)]
pub struct DfScalarFunction {
    /// The raw bytes encoded datafusion scalar function
    pub(crate) raw_fn: RawDfScalarFn,
    // TODO(discord9): directly from datafusion expr
    /// The implementation of the function
    pub(crate) fn_impl: Arc<dyn PhysicalExpr>,
    /// The input schema of the function
    pub(crate) df_schema: Arc<datafusion_common::DFSchema>,
}

impl DfScalarFunction {
    pub fn new(raw_fn: RawDfScalarFn, fn_impl: Arc<dyn PhysicalExpr>) -> Result<Self, Error> {
        Ok(Self {
            df_schema: Arc::new(raw_fn.input_schema.to_df_schema()?),
            raw_fn,
            fn_impl,
        })
    }

    pub async fn try_from_raw_fn(raw_fn: RawDfScalarFn) -> Result<Self, Error> {
        Ok(Self {
            fn_impl: raw_fn.get_fn_impl().await?,
            df_schema: Arc::new(raw_fn.input_schema.to_df_schema()?),
            raw_fn,
        })
    }

    /// Evaluate a batch of expressions using input values
    pub fn eval_batch(&self, batch: &Batch, exprs: &[ScalarExpr]) -> Result<VectorRef, EvalError> {
        let row_count = batch.row_count();
        let batch: Vec<_> = exprs
            .iter()
            .map(|expr| expr.eval_batch(batch))
            .collect::<Result<_, _>>()?;

        let schema = self.df_schema.inner().clone();

        let arrays = batch
            .iter()
            .map(|array| array.to_arrow_array())
            .collect::<Vec<_>>();
        let rb = DfRecordBatch::try_new_with_options(schema, arrays, &RecordBatchOptions::new().with_row_count(Some(row_count))).map_err(|err| {
            ArrowSnafu {
                context:
                    "Failed to create RecordBatch from values when eval_batch datafusion scalar function",
            }
            .into_error(err)
        })?;

        let len = rb.num_rows();

        let res = self.fn_impl.evaluate(&rb).context(EvalDatafusionSnafu {
            context: "Failed to evaluate datafusion scalar function",
        })?;
        let res = common_query::columnar_value::ColumnarValue::try_from(&res)
            .map_err(BoxedError::new)
            .context(ExternalSnafu)?;
        let res_vec = res
            .try_into_vector(len)
            .map_err(BoxedError::new)
            .context(ExternalSnafu)?;

        Ok(res_vec)
    }

    /// eval a list of expressions using input values
    fn eval_args(values: &[Value], exprs: &[ScalarExpr]) -> Result<Vec<Value>, EvalError> {
        exprs
            .iter()
            .map(|expr| expr.eval(values))
            .collect::<Result<_, _>>()
    }

    // TODO(discord9): add RecordBatch support
    pub fn eval(&self, values: &[Value], exprs: &[ScalarExpr]) -> Result<Value, EvalError> {
        // first eval exprs to construct values to feed to datafusion
        let values: Vec<_> = Self::eval_args(values, exprs)?;
        if values.is_empty() {
            return InvalidArgumentSnafu {
                reason: "values is empty".to_string(),
            }
            .fail();
        }
        // TODO(discord9): make cols all array length of one
        let mut cols = vec![];
        for (idx, typ) in self
            .raw_fn
            .input_schema
            .typ()
            .column_types
            .iter()
            .enumerate()
        {
            let typ = typ.scalar_type();
            let mut array = typ.create_mutable_vector(1);
            array.push_value_ref(values[idx].as_value_ref());
            cols.push(array.to_vector().to_arrow_array());
        }
        let schema = self.df_schema.inner().clone();
        let rb = DfRecordBatch::try_new_with_options(
            schema,
            cols,
            &RecordBatchOptions::new().with_row_count(Some(1)),
        )
        .map_err(|err| {
            ArrowSnafu {
                context:
                    "Failed to create RecordBatch from values when eval datafusion scalar function",
            }
            .into_error(err)
        })?;

        let res = self.fn_impl.evaluate(&rb).context(EvalDatafusionSnafu {
            context: "Failed to evaluate datafusion scalar function",
        })?;
        let res = common_query::columnar_value::ColumnarValue::try_from(&res)
            .map_err(BoxedError::new)
            .context(ExternalSnafu)?;
        let res_vec = res
            .try_into_vector(1)
            .map_err(BoxedError::new)
            .context(ExternalSnafu)?;
        let res_val = res_vec
            .try_get(0)
            .map_err(BoxedError::new)
            .context(ExternalSnafu)?;
        Ok(res_val)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RawDfScalarFn {
    /// The raw bytes encoded datafusion scalar function
    pub(crate) f: bytes::BytesMut,
    /// The input schema of the function
    pub(crate) input_schema: RelationDesc,
    /// Extension contains mapping from function reference to function name
    pub(crate) extensions: FunctionExtensions,
}

impl RawDfScalarFn {
    pub fn from_proto(
        f: &substrait::substrait_proto_df::proto::expression::ScalarFunction,
        input_schema: RelationDesc,
        extensions: FunctionExtensions,
    ) -> Result<Self, Error> {
        let mut buf = BytesMut::new();
        f.encode(&mut buf)
            .context(EncodeRelSnafu)
            .map_err(BoxedError::new)
            .context(crate::error::ExternalSnafu)?;
        Ok(Self {
            f: buf,
            input_schema,
            extensions,
        })
    }
    async fn get_fn_impl(&self) -> Result<Arc<dyn PhysicalExpr>, Error> {
        let f = ScalarFunction::decode(&mut self.f.as_ref())
            .context(DecodeRelSnafu)
            .map_err(BoxedError::new)
            .context(crate::error::ExternalSnafu)?;
        debug!("Decoded scalar function: {:?}", f);

        let input_schema = &self.input_schema;
        let extensions = &self.extensions;

        from_scalar_fn_to_df_fn_impl(&f, input_schema, extensions).await
    }
}

impl std::cmp::PartialEq for DfScalarFunction {
    fn eq(&self, other: &Self) -> bool {
        self.raw_fn.eq(&other.raw_fn)
    }
}

// can't derive Eq because of Arc<dyn PhysicalExpr> not eq, so implement it manually
impl std::cmp::Eq for DfScalarFunction {}

impl std::cmp::PartialOrd for DfScalarFunction {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl std::cmp::Ord for DfScalarFunction {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.raw_fn.cmp(&other.raw_fn)
    }
}
impl std::hash::Hash for DfScalarFunction {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.raw_fn.hash(state);
    }
}

#[cfg(test)]
mod test {

    use datatypes::prelude::ConcreteDataType;
    use substrait::substrait_proto_df::proto::expression::literal::LiteralType;
    use substrait::substrait_proto_df::proto::expression::{Literal, RexType};
    use substrait::substrait_proto_df::proto::function_argument::ArgType;
    use substrait::substrait_proto_df::proto::{Expression, FunctionArgument};

    use super::*;
    use crate::repr::{ColumnType, RelationType};

    #[tokio::test]
    async fn test_df_scalar_function() {
        let raw_scalar_func = ScalarFunction {
            function_reference: 0,
            arguments: vec![FunctionArgument {
                arg_type: Some(ArgType::Value(Expression {
                    rex_type: Some(RexType::Literal(Literal {
                        nullable: false,
                        type_variation_reference: 0,
                        literal_type: Some(LiteralType::I64(-1)),
                    })),
                })),
            }],
            output_type: None,
            ..Default::default()
        };
        let input_schema = RelationDesc::try_new(
            RelationType::new(vec![ColumnType::new_nullable(
                ConcreteDataType::null_datatype(),
            )]),
            vec!["null_column".to_string()],
        )
        .unwrap();
        let extensions = FunctionExtensions::from_iter(vec![(0, "abs")]);
        let raw_fn = RawDfScalarFn::from_proto(&raw_scalar_func, input_schema, extensions).unwrap();
        let df_func = DfScalarFunction::try_from_raw_fn(raw_fn).await.unwrap();
        assert_eq!(
            df_func
                .eval(&[Value::Null], &[ScalarExpr::Column(0)])
                .unwrap(),
            Value::Int64(1)
        );
    }
}
