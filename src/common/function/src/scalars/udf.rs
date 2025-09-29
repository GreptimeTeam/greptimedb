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

use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use common_query::prelude::ColumnarValue;
use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::{ScalarFunctionArgs, ScalarUDFImpl};
use datafusion_expr::ScalarUDF;
use session::context::QueryContextRef;

use crate::function::{FunctionContext, FunctionRef};
use crate::state::FunctionState;

struct ScalarUdf {
    function: FunctionRef,
    signature: datafusion_expr::Signature,
    context: FunctionContext,
}

impl Debug for ScalarUdf {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ScalarUdf")
            .field("function", &self.function.name())
            .field("signature", &self.signature)
            .finish()
    }
}

impl ScalarUDFImpl for ScalarUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        self.function.name()
    }

    fn aliases(&self) -> &[String] {
        self.function.aliases()
    }

    fn signature(&self) -> &datafusion_expr::Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> datafusion_common::Result<DataType> {
        self.function.return_type(arg_types).map_err(Into::into)
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion_common::Result<datafusion_expr::ColumnarValue> {
        let result = self.function.invoke_with_args(args.clone());
        if !matches!(
            result,
            Err(datafusion_common::DataFusionError::NotImplemented(_))
        ) {
            return result;
        }

        let columns = args
            .args
            .iter()
            .map(|x| ColumnarValue::try_from(x).and_then(|y| y.try_into_vector(args.number_rows)))
            .collect::<common_query::error::Result<Vec<_>>>()?;
        let v = self
            .function
            .eval(&self.context, &columns)
            .map(ColumnarValue::Vector)?;
        Ok(v.into())
    }
}

/// Create a ScalarUdf from function, query context and state.
pub fn create_udf(
    func: FunctionRef,
    query_ctx: QueryContextRef,
    state: Arc<FunctionState>,
) -> ScalarUDF {
    let signature = func.signature();
    let udf = ScalarUdf {
        function: func,
        signature,
        context: FunctionContext { query_ctx, state },
    };
    ScalarUDF::new_from_impl(udf)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_query::prelude::ScalarValue;
    use datafusion::arrow::array::BooleanArray;
    use datafusion_common::arrow::array::AsArray;
    use datafusion_common::arrow::datatypes::DataType as ArrowDataType;
    use datafusion_common::config::ConfigOptions;
    use datatypes::arrow::datatypes::Field;
    use datatypes::data_type::{ConcreteDataType, DataType};
    use session::context::QueryContextBuilder;

    use super::*;
    use crate::function::Function;
    use crate::scalars::test::TestAndFunction;

    #[test]
    fn test_create_udf() {
        let f = Arc::new(TestAndFunction);
        let query_ctx = QueryContextBuilder::default().build().into();

        let args = ScalarFunctionArgs {
            args: vec![
                datafusion_expr::ColumnarValue::Array(Arc::new(BooleanArray::from(vec![
                    true, true, true,
                ]))),
                datafusion_expr::ColumnarValue::Array(Arc::new(BooleanArray::from(vec![
                    true, false, true,
                ]))),
            ],
            arg_fields: vec![],
            number_rows: 3,
            return_field: Arc::new(Field::new("x", ArrowDataType::Boolean, true)),
            config_options: Arc::new(Default::default()),
        };

        let result = f
            .invoke_with_args(args)
            .and_then(|x| x.to_array(3))
            .unwrap();
        let vector = result.as_boolean();
        assert_eq!(3, vector.len());

        assert!(vector.value(0));
        assert!(!vector.value(1));
        assert!(vector.value(2));

        // create a udf and test it again
        let udf = create_udf(f.clone(), query_ctx, Arc::new(FunctionState::default()));

        assert_eq!("test_and", udf.name());
        let expected_signature: datafusion_expr::Signature = f.signature();
        assert_eq!(udf.signature(), &expected_signature);
        assert_eq!(
            ConcreteDataType::boolean_datatype(),
            udf.return_type(&[])
                .map(|x| ConcreteDataType::from_arrow_type(&x))
                .unwrap()
        );

        let args = vec![
            datafusion_expr::ColumnarValue::Scalar(ScalarValue::Boolean(Some(true))),
            datafusion_expr::ColumnarValue::Array(Arc::new(BooleanArray::from(vec![
                true, false, false, true,
            ]))),
        ];

        let arg_fields = vec![
            Arc::new(Field::new("a", args[0].data_type(), false)),
            Arc::new(Field::new("b", args[1].data_type(), false)),
        ];
        let return_field = Arc::new(Field::new(
            "x",
            ConcreteDataType::boolean_datatype().as_arrow_type(),
            false,
        ));
        let args = ScalarFunctionArgs {
            args,
            arg_fields,
            number_rows: 4,
            return_field,
            config_options: Arc::new(ConfigOptions::default()),
        };
        match udf.invoke_with_args(args).unwrap() {
            datafusion_expr::ColumnarValue::Array(x) => {
                let x = x.as_any().downcast_ref::<BooleanArray>().unwrap();
                assert_eq!(x.len(), 4);
                assert_eq!(
                    x.iter().flatten().collect::<Vec<bool>>(),
                    vec![true, false, false, true]
                );
            }
            _ => unreachable!(),
        }
    }
}
