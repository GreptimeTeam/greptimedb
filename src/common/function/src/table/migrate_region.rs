use std::fmt::{self};
use std::sync::Arc;

use common_query::error::{MissingTableMutationHandlerSnafu, Result};
use common_query::prelude::{Signature, Volatility};
use datatypes::prelude::{ConcreteDataType, ScalarVector};
use datatypes::vectors::{StringVector, VectorRef};
use snafu::OptionExt;

use crate::function::{Function, FunctionContext};

#[derive(Clone, Debug, Default)]
pub struct MigrateRegionFunction;

const NAME: &str = "migrate_region";

impl Function for MigrateRegionFunction {
    fn name(&self) -> &str {
        NAME
    }

    fn return_type(&self, _input_types: &[ConcreteDataType]) -> Result<ConcreteDataType> {
        Ok(ConcreteDataType::string_datatype())
    }

    fn signature(&self) -> Signature {
        Signature::uniform(0, vec![], Volatility::Immutable)
    }

    fn eval(&self, func_ctx: FunctionContext, _columns: &[VectorRef]) -> Result<VectorRef> {
        let pid = func_ctx
            .state
            .table_mutation_handler
            .as_ref()
            .context(MissingTableMutationHandlerSnafu)?;

        todo!();
    }
}

impl fmt::Display for MigrateRegionFunction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "MIGRATE_REGION")
    }
}
