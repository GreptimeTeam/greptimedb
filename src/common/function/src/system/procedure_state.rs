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

use api::v1::meta::ProcedureStatus;
use common_meta::rpc::procedure::ProcedureStateResponse;
use common_query::error::Error::ThreadJoin;
use common_query::error::{
    InvalidFuncArgsSnafu, MissingProcedureServiceHandlerSnafu, Result,
    UnsupportedInputDataTypeSnafu,
};
use common_query::prelude::{Signature, Volatility};
use common_telemetry::error;
use datatypes::prelude::*;
use datatypes::vectors::{ConstantVector, Helper, StringVector, VectorRef};
use snafu::{ensure, Location, OptionExt};

use crate::function::{Function, FunctionContext};

const NAME: &str = "procedure_state";

/// A function to query procedure state by its id.
/// Such as `procedure_state(pid)`.
#[derive(Clone, Debug, Default)]
pub struct ProcedureStateFunction;

impl fmt::Display for ProcedureStateFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "PROCEDURE_STATE")
    }
}

impl Function for ProcedureStateFunction {
    fn name(&self) -> &str {
        NAME
    }

    fn return_type(&self, _input_types: &[ConcreteDataType]) -> Result<ConcreteDataType> {
        Ok(ConcreteDataType::string_datatype())
    }

    fn signature(&self) -> Signature {
        Signature::uniform(
            1,
            vec![ConcreteDataType::string_datatype()],
            Volatility::Immutable,
        )
    }

    fn eval(&self, func_ctx: FunctionContext, columns: &[VectorRef]) -> Result<VectorRef> {
        ensure!(
            columns.len() == 1,
            InvalidFuncArgsSnafu {
                err_msg: format!(
                    "The length of the args is not correct, expect 1, have: {}",
                    columns.len()
                ),
            }
        );

        let pids = columns[0].clone();
        let expect_len = pids.len();
        let is_const = pids.is_const();

        match pids.data_type() {
            ConcreteDataType::String(_) => {
                // TODO(dennis): datafusion UDF doesn't support async function currently
                std::thread::spawn(move || {
                    let pids: &StringVector = if is_const {
                        let pids: &ConstantVector = unsafe { Helper::static_cast(&pids) };
                        unsafe { Helper::static_cast(pids.inner()) }
                    } else {
                        unsafe { Helper::static_cast(&pids) }
                    };

                    let procedure_service_handler = func_ctx
                        .state
                        .procedure_service_handler
                        .as_ref()
                        .context(MissingProcedureServiceHandlerSnafu)?;

                    let states = pids
                        .iter_data()
                        .map(|pid| {
                            if let Some(pid) = pid {
                                let ProcedureStateResponse { status, error, .. } =
                                    common_runtime::block_on_read(async move {
                                        procedure_service_handler.query_procedure_state(pid).await
                                    })?;

                                let status = ProcedureStatus::try_from(status)
                                    .map(|v| v.as_str_name())
                                    .unwrap_or("Unknown");

                                Ok(Some(format!("status: {status}, error: {error}")))
                            } else {
                                Ok(None)
                            }
                        })
                        .collect::<Result<Vec<_>>>()?;

                    let results: VectorRef = Arc::new(StringVector::from(states));

                    if is_const {
                        Ok(Arc::new(ConstantVector::new(results, expect_len)) as _)
                    } else {
                        Ok(results)
                    }
                })
                .join()
                .map_err(|e| {
                    error!(e; "Join thread error");
                    ThreadJoin {
                        location: Location::default(),
                    }
                })?
            }
            _ => UnsupportedInputDataTypeSnafu {
                function: NAME,
                datatypes: columns.iter().map(|c| c.data_type()).collect::<Vec<_>>(),
            }
            .fail(),
        }
    }
}
