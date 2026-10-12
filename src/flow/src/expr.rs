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

//! Small row-batch conversion helper used by stateless streaming.

pub(crate) mod error;
use common_recordbatch::RecordBatch;
use datatypes::data_type::DataType;
use datatypes::prelude::ConcreteDataType;
use datatypes::vectors::{Helper, VectorRef};
use itertools::Itertools;
use snafu::{ResultExt, ensure};

use crate::Error;
use crate::error::DatatypesSnafu;
use crate::repr::Row;

#[derive(Debug, Clone)]
pub struct Batch {
    batch: Vec<VectorRef>,
    row_count: usize,
}

impl TryFrom<RecordBatch> for Batch {
    type Error = Error;
    fn try_from(value: RecordBatch) -> Result<Self, Self::Error> {
        Ok(Self {
            batch: Helper::try_into_vectors(value.columns()).context(DatatypesSnafu {
                extra: "failed to convert Arrow array to vector",
            })?,
            row_count: value.num_rows(),
        })
    }
}

impl Batch {
    pub fn try_from_rows_with_types(
        rows: Vec<Row>,
        types: &[ConcreteDataType],
    ) -> Result<Self, error::EvalError> {
        if rows.is_empty() {
            return Ok(Self {
                batch: vec![],
                row_count: 0,
            });
        }
        let len = rows.len();
        let mut builders = types
            .iter()
            .map(|ty| ty.create_mutable_vector(len))
            .collect_vec();
        ensure!(
            rows.iter().all(|row| row.len() == builders.len()),
            error::InvalidArgumentSnafu {
                reason: "row length does not match schema".to_string()
            }
        );
        for row in rows {
            for (idx, value) in row.iter().enumerate() {
                builders[idx]
                    .try_push_value_ref(&value.as_value_ref())
                    .context(error::DataTypeSnafu {
                        msg: "failed to convert rows to columns",
                    })?;
            }
        }
        Ok(Self {
            batch: builders.into_iter().map(|mut b| b.to_vector()).collect(),
            row_count: len,
        })
    }
    pub fn batch(&self) -> &[VectorRef] {
        &self.batch
    }
    pub fn row_count(&self) -> usize {
        self.row_count
    }
}
