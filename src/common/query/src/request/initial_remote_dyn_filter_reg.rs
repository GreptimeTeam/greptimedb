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

use std::collections::HashSet;
use std::sync::Arc;

use datafusion::arrow::datatypes::Schema;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::PhysicalExpr;
use datafusion_common::Result as DataFusionResult;
use serde::{Deserialize, Serialize};

use crate::request::{decode_physical_expr_from_bytes, encode_physical_expr_to_bytes};

pub const INITIAL_REMOTE_DYN_FILTER_REGISTRATIONS_EXTENSION_KEY: &str =
    "initial_remote_dyn_filter_registrations";
pub const INITIAL_REMOTE_DYN_FILTER_REGS_MAX_COUNT: usize = 64;
pub const INITIAL_REMOTE_DYN_FILTER_REGS_MAX_TOTAL_PROTO_BYTES: usize = 64 * 1024;

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct InitialDynFilterRegs {
    #[serde(rename = "registrations")]
    pub regs: Vec<InitialDynFilterReg>,
}

impl InitialDynFilterRegs {
    pub fn new(regs: Vec<InitialDynFilterReg>) -> Self {
        Self { regs }
    }

    pub fn is_empty(&self) -> bool {
        self.regs.is_empty()
    }

    pub fn total_encoded_child_expr_bytes(&self) -> usize {
        self.regs
            .iter()
            .map(InitialDynFilterReg::encoded_child_expr_bytes)
            .sum()
    }

    pub fn validate_default_bounds(&self) -> Result<(), String> {
        self.validate_bounds(
            INITIAL_REMOTE_DYN_FILTER_REGS_MAX_COUNT,
            INITIAL_REMOTE_DYN_FILTER_REGS_MAX_TOTAL_PROTO_BYTES,
        )
    }

    pub fn validate_bounds(
        &self,
        max_count: usize,
        max_total_proto_bytes: usize,
    ) -> Result<(), String> {
        if self.regs.len() > max_count {
            return Err(format!(
                "InitialDynFilterRegs contains {} registrations, which exceeds the configured limit of {}",
                self.regs.len(),
                max_count
            ));
        }

        let total_proto_bytes = self.total_encoded_child_expr_bytes();
        if total_proto_bytes > max_total_proto_bytes {
            return Err(format!(
                "InitialDynFilterRegs contains {} total child expr proto bytes, which exceeds the configured limit of {}",
                total_proto_bytes, max_total_proto_bytes
            ));
        }

        let mut seen_filter_ids = HashSet::with_capacity(self.regs.len());
        for reg in &self.regs {
            if !seen_filter_ids.insert(reg.filter_id.as_str()) {
                return Err(format!(
                    "InitialDynFilterRegs contains duplicate filter_id '{}'",
                    reg.filter_id
                ));
            }
        }

        Ok(())
    }

    pub fn to_extension_value(&self) -> serde_json::Result<String> {
        serde_json::to_string(self)
    }

    pub fn from_extension_value(value: &str) -> serde_json::Result<Self> {
        serde_json::from_str(value)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct InitialDynFilterReg {
    pub filter_id: String,
    pub child_exprs_datafusion_proto: Vec<Vec<u8>>,
}

impl InitialDynFilterReg {
    pub fn new(filter_id: impl Into<String>, child_exprs_datafusion_proto: Vec<Vec<u8>>) -> Self {
        Self {
            filter_id: filter_id.into(),
            child_exprs_datafusion_proto,
        }
    }

    pub fn from_filter_id_and_children(
        filter_id: impl Into<String>,
        children: &[Arc<dyn PhysicalExpr>],
    ) -> DataFusionResult<Self> {
        let child_exprs_datafusion_proto = children
            .iter()
            .map(encode_physical_expr_to_bytes)
            .collect::<DataFusionResult<Vec<_>>>()?;

        Ok(Self::new(filter_id, child_exprs_datafusion_proto))
    }

    pub fn encoded_child_expr_bytes(&self) -> usize {
        self.child_exprs_datafusion_proto.iter().map(Vec::len).sum()
    }

    pub fn decode_children(
        &self,
        task_ctx: &TaskContext,
        input_schema: &Schema,
        max_payload_bytes: usize,
    ) -> DataFusionResult<Vec<Arc<dyn PhysicalExpr>>> {
        self.child_exprs_datafusion_proto
            .iter()
            .map(|expr_bytes| {
                decode_physical_expr_from_bytes(
                    expr_bytes,
                    task_ctx,
                    input_schema,
                    max_payload_bytes,
                )
            })
            .collect::<DataFusionResult<Vec<_>>>()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_plan::PhysicalExpr;
    use datafusion_common::DataFusionError;

    use super::*;

    #[test]
    fn initial_dyn_filter_regs_json_round_trip() {
        let regs = InitialDynFilterRegs::new(vec![
            InitialDynFilterReg::new("filter-a", vec![vec![1, 2, 3]]),
            InitialDynFilterReg::new("filter-b", vec![vec![4, 5]]),
        ]);

        let encoded = regs.to_extension_value().unwrap();
        let decoded = InitialDynFilterRegs::from_extension_value(&encoded).unwrap();

        assert_eq!(decoded, regs);
    }

    #[test]
    fn initial_dyn_filter_regs_validate_bounds_rejects_duplicate_filter_ids() {
        let regs = InitialDynFilterRegs::new(vec![
            InitialDynFilterReg::new("filter-a", vec![vec![1]]),
            InitialDynFilterReg::new("filter-a", vec![vec![2]]),
        ]);

        let err = regs.validate_bounds(8, 1024).unwrap_err();

        assert!(err.contains("duplicate filter_id 'filter-a'"));
    }

    #[test]
    fn initial_dyn_filter_regs_validate_bounds_rejects_too_many_regs() {
        let regs = InitialDynFilterRegs::new(vec![
            InitialDynFilterReg::new("filter-a", vec![vec![1]]),
            InitialDynFilterReg::new("filter-b", vec![vec![2]]),
        ]);

        let err = regs.validate_bounds(1, 1024).unwrap_err();

        assert!(err.contains("exceeds the configured limit of 1"));
    }

    #[test]
    fn initial_dyn_filter_regs_validate_bounds_rejects_total_proto_bytes_over_limit() {
        let regs = InitialDynFilterRegs::new(vec![
            InitialDynFilterReg::new("filter-a", vec![vec![1, 2, 3]]),
            InitialDynFilterReg::new("filter-b", vec![vec![4, 5, 6]]),
        ]);

        let err = regs.validate_bounds(8, 5).unwrap_err();

        assert!(err.contains("6 total child expr proto bytes"));
    }

    #[test]
    fn initial_dyn_filter_reg_round_trips_child_exprs() {
        let schema = Schema::new(vec![Field::new("host", DataType::Utf8, false)]);
        let child: Arc<dyn PhysicalExpr> =
            Arc::new(Column::new_with_schema("host", &schema).unwrap());
        let reg = InitialDynFilterReg::from_filter_id_and_children("filter-1", &[child]).unwrap();

        let decoded = reg
            .decode_children(&TaskContext::default(), &schema, 1024)
            .unwrap();
        let decoded = decoded[0].as_any().downcast_ref::<Column>().unwrap();

        assert_eq!(reg.filter_id, "filter-1");
        assert_eq!(decoded.name(), "host");
        assert_eq!(decoded.index(), 0);
    }

    #[test]
    fn initial_dyn_filter_reg_decode_rejects_column_name_index_mismatch() {
        let schema = Schema::new(vec![Field::new("host", DataType::Utf8, false)]);
        let reg = InitialDynFilterReg::from_filter_id_and_children(
            "filter-1",
            &[Arc::new(Column::new("service", 0)) as Arc<dyn PhysicalExpr>],
        )
        .unwrap();

        let err = reg
            .decode_children(&TaskContext::default(), &schema, 1024)
            .unwrap_err();

        assert!(matches!(err, DataFusionError::Plan(_)));
    }
}
