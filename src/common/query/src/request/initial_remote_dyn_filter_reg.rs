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

use crate::request::{
    DynFilterPayload, decode_physical_expr_from_bytes, encode_physical_expr_to_bytes,
};

pub const INITIAL_REMOTE_DYN_FILTER_REGISTRATIONS_EXTENSION_KEY: &str =
    "initial_remote_dyn_filter_registrations";
pub const INITIAL_REMOTE_DYN_FILTER_REGS_MAX_COUNT: usize = 64;
/// Raw encoded registration byte budget for initial remote dynamic filter registrations.
///
/// Counts proto payload bytes before JSON/base64 expansion, not the final extension size.
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

    pub fn total_encoded_registration_bytes(&self) -> usize {
        self.regs
            .iter()
            .map(InitialDynFilterReg::encoded_registration_bytes)
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

        let total_registration_bytes = self.total_encoded_registration_bytes();
        if total_registration_bytes > max_total_proto_bytes {
            return Err(format!(
                "InitialDynFilterRegs contains {} total encoded registration bytes, which exceeds the configured limit of {}",
                total_registration_bytes, max_total_proto_bytes
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
        let regs = serde_json::from_str::<Self>(value)?;
        regs.validate_default_bounds()
            .map_err(serde::de::Error::custom)?;
        Ok(regs)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct InitialDynFilterReg {
    pub filter_id: String,
    #[serde(with = "super::base64_serde::bytes_vec")]
    pub child_exprs_datafusion_proto: Vec<Vec<u8>>,
    /// Optional producer-side predicate snapshot captured at initial registration time.
    ///
    /// This is only an initial pending update for the remote runtime filter. It is not part of
    /// registration identity; identity is carried by `filter_id` and child expressions.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub initial_snapshot: Option<InitialDynFilterSnapshot>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct InitialDynFilterSnapshot {
    pub payload: DynFilterPayload,
    /// Producer-side generation used to ignore stale snapshots.
    pub generation: u64,
    /// Whether this snapshot completes the dynamic filter stream.
    pub is_complete: bool,
}

impl InitialDynFilterReg {
    pub fn new(filter_id: impl Into<String>, child_exprs_datafusion_proto: Vec<Vec<u8>>) -> Self {
        Self {
            filter_id: filter_id.into(),
            child_exprs_datafusion_proto,
            initial_snapshot: None,
        }
    }

    pub fn with_initial_snapshot(mut self, initial_snapshot: InitialDynFilterSnapshot) -> Self {
        self.initial_snapshot = Some(initial_snapshot);
        self
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

    pub fn encoded_registration_bytes(&self) -> usize {
        self.encoded_child_expr_bytes()
            + self
                .initial_snapshot
                .as_ref()
                .map(InitialDynFilterSnapshot::encoded_payload_bytes)
                .unwrap_or(0)
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

impl InitialDynFilterSnapshot {
    pub fn new(payload: DynFilterPayload, generation: u64, is_complete: bool) -> Self {
        Self {
            payload,
            generation,
            is_complete,
        }
    }

    pub fn encoded_payload_bytes(&self) -> usize {
        match &self.payload {
            DynFilterPayload::Datafusion(bytes) => bytes.len(),
        }
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
        let json: serde_json::Value = serde_json::from_str(&encoded).unwrap();
        let decoded = InitialDynFilterRegs::from_extension_value(&encoded).unwrap();

        assert_eq!(
            json["registrations"][0]["child_exprs_datafusion_proto"],
            serde_json::json!(["AQID"])
        );
        assert_eq!(
            json["registrations"][1]["child_exprs_datafusion_proto"],
            serde_json::json!(["BAU="])
        );
        assert_eq!(decoded, regs);
    }

    #[test]
    fn initial_dyn_filter_regs_json_round_trip_with_snapshot() {
        let regs = InitialDynFilterRegs::new(vec![
            InitialDynFilterReg::new("filter-a", vec![vec![1, 2, 3]]).with_initial_snapshot(
                InitialDynFilterSnapshot::new(DynFilterPayload::Datafusion(vec![4, 5, 6]), 7, true),
            ),
        ]);

        let encoded = regs.to_extension_value().unwrap();
        let json: serde_json::Value = serde_json::from_str(&encoded).unwrap();
        let decoded = InitialDynFilterRegs::from_extension_value(&encoded).unwrap();

        assert_eq!(
            json["registrations"][0]["child_exprs_datafusion_proto"],
            serde_json::json!(["AQID"])
        );
        assert_eq!(
            json["registrations"][0]["initial_snapshot"]["payload"],
            serde_json::json!({"kind":"datafusion","payload":"BAUG"})
        );
        assert_eq!(decoded, regs);
        assert_eq!(
            decoded.regs[0]
                .initial_snapshot
                .as_ref()
                .unwrap()
                .generation,
            7
        );
        assert!(
            decoded.regs[0]
                .initial_snapshot
                .as_ref()
                .unwrap()
                .is_complete
        );
    }

    #[test]
    fn initial_dyn_filter_reg_json_defaults_missing_snapshot_to_none() {
        let decoded = InitialDynFilterRegs::from_extension_value(
            r#"{"registrations":[{"filter_id":"filter-a","child_exprs_datafusion_proto":["AQID"]}]}"#,
        )
        .unwrap();

        assert_eq!(decoded.regs.len(), 1);
        assert!(decoded.regs[0].initial_snapshot.is_none());
    }

    #[test]
    fn initial_dyn_filter_reg_encoded_registration_bytes_include_snapshot_payload() {
        let reg = InitialDynFilterReg::new("filter-a", vec![vec![1, 2, 3], vec![4]])
            .with_initial_snapshot(InitialDynFilterSnapshot::new(
                DynFilterPayload::Datafusion(vec![5, 6]),
                2,
                false,
            ));

        assert_eq!(reg.encoded_child_expr_bytes(), 4);
        assert_eq!(reg.encoded_registration_bytes(), 6);
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
    fn initial_dyn_filter_regs_from_extension_value_validates_default_bounds() {
        let value = r#"{"registrations":[{"filter_id":"filter-a","child_exprs_datafusion_proto":["AQ=="]},{"filter_id":"filter-a","child_exprs_datafusion_proto":["Ag=="]}]}"#;

        let err = InitialDynFilterRegs::from_extension_value(value).unwrap_err();

        assert!(err.to_string().contains("duplicate filter_id 'filter-a'"));
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

        assert!(err.contains("6 total encoded registration bytes"));
    }

    #[test]
    fn initial_dyn_filter_regs_validate_bounds_rejects_snapshot_bytes_over_limit() {
        let regs = InitialDynFilterRegs::new(vec![
            InitialDynFilterReg::new("filter-a", vec![vec![1]]).with_initial_snapshot(
                InitialDynFilterSnapshot::new(
                    DynFilterPayload::Datafusion(vec![2, 3, 4]),
                    2,
                    false,
                ),
            ),
        ]);

        let err = regs.validate_bounds(8, 3).unwrap_err();

        assert!(err.contains("4 total encoded registration bytes"));
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
