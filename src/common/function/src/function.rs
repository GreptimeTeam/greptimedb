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
use std::fmt;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::logical_expr::{ColumnarValue, ReturnFieldArgs};
use datafusion_common::DataFusionError;
use datafusion_common::arrow::array::ArrayRef;
use datafusion_common::config::{ConfigEntry, ConfigExtension, ExtensionOptions};
use datafusion_expr::{ScalarFunctionArgs, Signature};
use session::context::{QueryContextBuilder, QueryContextRef};

use crate::state::FunctionState;

/// The function execution context
#[derive(Clone)]
pub struct FunctionContext {
    pub query_ctx: QueryContextRef,
    pub state: Arc<FunctionState>,
}

impl FunctionContext {
    /// Create a mock [`FunctionContext`] for test.
    #[cfg(any(test, feature = "testing"))]
    pub fn mock() -> Self {
        Self {
            query_ctx: QueryContextBuilder::default().build().into(),
            state: Arc::new(FunctionState::mock()),
        }
    }
}

impl std::fmt::Display for FunctionContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FunctionContext {{ query_ctx: {} }}", self.query_ctx)
    }
}

impl Default for FunctionContext {
    fn default() -> Self {
        Self {
            query_ctx: QueryContextBuilder::default().build().into(),
            state: Arc::new(FunctionState::default()),
        }
    }
}

impl ExtensionOptions for FunctionContext {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn cloned(&self) -> Box<dyn ExtensionOptions> {
        Box::new(self.clone())
    }

    fn set(&mut self, _: &str, _: &str) -> datafusion_common::Result<()> {
        Err(DataFusionError::NotImplemented(
            "set options for `FunctionContext`".to_string(),
        ))
    }

    fn entries(&self) -> Vec<ConfigEntry> {
        vec![]
    }
}

impl Debug for FunctionContext {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("FunctionContext")
            .field("query_ctx", &self.query_ctx)
            .finish()
    }
}

impl ConfigExtension for FunctionContext {
    const PREFIX: &'static str = "FunctionContext";
}

/// Scalar function trait, modified from databend to adapt datafusion
/// TODO(dennis): optimize function by it's features such as monotonicity etc.
pub trait Function: fmt::Display + Sync + Send {
    /// Returns the name of the function, should be unique.
    fn name(&self) -> &str;

    /// The returned data type of function execution.
    fn return_type(&self, input_types: &[DataType]) -> datafusion_common::Result<DataType>;

    /// The signature of function.
    fn signature(&self) -> &Signature;

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion_common::Result<ColumnarValue>;

    fn aliases(&self) -> &[String] {
        &[]
    }

    /// Returns the return field for this function given the input fields.
    ///
    /// Default implementation extracts data types from input fields and calls
    /// [`Function::return_type`], creating a generic field with the returned type.
    fn return_field_from_args(
        &self,
        args: ReturnFieldArgs<'_>,
    ) -> Result<Arc<Field>, DataFusionError> {
        let input_types = args
            .arg_fields
            .iter()
            .map(|f| f.data_type().clone())
            .collect::<Vec<_>>();
        let return_type = self.return_type(&input_types)?;
        Ok(Arc::new(Field::new(self.name(), return_type, true)))
    }
}

pub type FunctionRef = Arc<dyn Function>;

/// Find the [FunctionContext] in the [ScalarFunctionArgs]. The [FunctionContext] was set
/// previously in the DataFusion session context creation, and is passed all the way down to the
/// args by DataFusion.
pub(crate) fn find_function_context(
    args: &ScalarFunctionArgs,
) -> datafusion_common::Result<&FunctionContext> {
    let Some(x) = args.config_options.extensions.get::<FunctionContext>() else {
        return Err(DataFusionError::Execution(
            "function context is not set".to_string(),
        ));
    };
    Ok(x)
}

/// Extract UDF arguments (as Arrow's [ArrayRef]) from [ScalarFunctionArgs] directly.
pub(crate) fn extract_args<const N: usize>(
    name: &str,
    args: &ScalarFunctionArgs,
) -> datafusion_common::Result<[ArrayRef; N]> {
    ColumnarValue::values_to_arrays(&args.args)
        .and_then(|x| datafusion_common::utils::take_function_args(name, x))
}
