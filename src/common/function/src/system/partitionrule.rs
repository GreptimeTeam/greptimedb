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

use common_error::ext::{BoxedError, PlainError};
use common_error::status_code::StatusCode;
use common_query::error::{self, Result};
use common_query::prelude::{Signature, TypeSignature, Volatility};
use datatypes::prelude::*;
use datatypes::vectors::{StringVectorBuilder, VectorRef};
use derive_more::Display;
use snafu::ResultExt;
use sql::partition::gen::partition_rule_for_range;

use crate::function::{Function, FunctionContext};
use crate::scalars::geo::helpers::{ensure_columns_len, ensure_columns_n};

/// The helper function to generate partition rule from given criteria.
///
/// It takes 4 arguments:
/// - field name
/// - all possible characters, represented in json arrays like `[["0", "9"],
/// ["a", "z"]]`
/// - number of expected partitions
/// - given hardstops, as json string array
#[derive(Clone, Debug, Default, Display)]
#[display("{}", self.name())]
pub struct GeneratePartitionRuleFunction;

impl Function for GeneratePartitionRuleFunction {
    fn name(&self) -> &str {
        "generate_partition_rule"
    }

    fn return_type(&self, _input_types: &[ConcreteDataType]) -> Result<ConcreteDataType> {
        Ok(ConcreteDataType::string_datatype())
    }

    fn signature(&self) -> Signature {
        Signature::new(
            TypeSignature::Exact(vec![
                ConcreteDataType::string_datatype(),
                ConcreteDataType::string_datatype(),
                ConcreteDataType::uint32_datatype(),
                ConcreteDataType::string_datatype(),
            ]),
            Volatility::Stable,
        )
    }

    fn eval(&self, _ctx: &FunctionContext, columns: &[VectorRef]) -> Result<VectorRef> {
        ensure_columns_n!(columns, 4);

        let field_name_vec = &columns[0];
        let ranges_vec = &columns[1];
        let partition_num_vec = &columns[2];
        let hardstops_vec = &columns[3];

        let size = ranges_vec.len();

        let mut results = StringVectorBuilder::with_capacity(size);

        for i in 0..size {
            let field_name = field_name_vec.get(i).as_string().unwrap();
            let ranges = ranges_vec.get(i).as_string().unwrap();
            let partition_num = partition_num_vec.get(i).as_u64().unwrap();
            let hardstops = hardstops_vec.get(i).as_string().unwrap();

            let ranges: Vec<(char, char)> = serde_json::from_str::<Vec<Vec<String>>>(&ranges)
                .map_err(|e| {
                    BoxedError::new(PlainError::new(
                        format!("Json parse error: {}", e),
                        StatusCode::EngineExecuteQuery,
                    ))
                })
                .context(error::ExecuteSnafu)?
                .iter()
                .map(|v| (v[0].chars().nth(0).unwrap(), v[1].chars().nth(0).unwrap()))
                .collect();
            let hardstops = serde_json::from_str::<Vec<String>>(&hardstops)
                .map_err(|e| {
                    BoxedError::new(PlainError::new(
                        format!("Json parse error: {}", e),
                        StatusCode::EngineExecuteQuery,
                    ))
                })
                .context(error::ExecuteSnafu)?;

            let rules =
                partition_rule_for_range(&field_name, &ranges, partition_num as u32, &hardstops)
                    .map_err(|e| {
                        BoxedError::new(PlainError::new(
                            format!("Json parse error: {}", e),
                            StatusCode::EngineExecuteQuery,
                        ))
                    })
                    .context(error::ExecuteSnafu)?;
            results.push(
                Some(
                    rules
                        .iter()
                        .map(|r| r.to_string())
                        .collect::<Vec<String>>()
                        .join(",\n"),
                )
                .as_deref(),
            );
        }
        Ok(results.to_vector())
    }
}
