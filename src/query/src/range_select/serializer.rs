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
use std::cmp::Ordering;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::Duration;

use datafusion::common::{DFSchema, DataFusionError, Result};
use datafusion_expr::logical_plan::{InvariantLevel, Projection};
use datafusion_expr::{Expr, LogicalPlan, UserDefinedLogicalNode};
use greptime_proto::substrait_extension::{
    AggregateKind, RangeFunctionV1, RangeSelectPartial, RangeSelectPartialV1, range_select_partial,
};
use prost::Message;

use super::plan::{
    PartialRangeWireFunction, PartialRangeWireMetadata, RangeAggregateKind, RangeSelect,
};

pub(crate) const RANGE_SELECT_PARTIAL_NODE_NAME: &str = "RangeSelectPartial";

pub(crate) fn serialize(node: &RangeSelect) -> Result<Vec<u8>> {
    let wire = node.partial_wire_metadata().ok_or_else(|| {
        DataFusionError::Plan("RangeSelect serialization only supports Partial nodes".to_string())
    })?;
    let functions = wire
        .functions()
        .iter()
        .map(|function| {
            Ok(RangeFunctionV1 {
                aggregate: encode_kind(function.kind()) as i32,
                argument_column_index: checked_index(function.argument_index())?,
                range_millis: checked_millis(function.range(), "range")?,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    if functions.is_empty() {
        return Err(DataFusionError::Plan(
            "RangeSelect Partial wire metadata requires at least one function".to_string(),
        ));
    }
    Ok(RangeSelectPartial {
        payload: Some(range_select_partial::Payload::V1(RangeSelectPartialV1 {
            align_millis: checked_millis(wire.align(), "align")?,
            align_to_millis: wire.align_to(),
            time_column_index: checked_index(wire.time_index())?,
            by_column_indices: wire
                .by_indices()
                .iter()
                .map(|index| checked_index(*index))
                .collect::<Result<Vec<_>>>()?,
            range_functions: functions,
        })),
    }
    .encode_to_vec())
}

pub(crate) fn deserialize(bytes: &[u8]) -> Result<Arc<dyn UserDefinedLogicalNode>> {
    Ok(Arc::new(RangeSelectPartialPlaceholder {
        metadata: decode_metadata(bytes)?,
    }))
}

fn decode_metadata(bytes: &[u8]) -> Result<PartialRangeWireMetadata> {
    let payload = RangeSelectPartial::decode(bytes)
        .map_err(|error| {
            DataFusionError::Substrait(format!("Invalid RangeSelect Partial: {error}"))
        })?
        .payload
        .ok_or_else(|| {
            DataFusionError::Substrait("RangeSelect Partial payload is empty".to_string())
        })?;
    let range_select_partial::Payload::V1(v1) = payload;
    let functions = v1
        .range_functions
        .into_iter()
        .map(|function| {
            PartialRangeWireFunction::try_new(
                decode_kind(function.aggregate)?,
                checked_usize(function.argument_column_index, "argument index")?,
                decode_duration(function.range_millis, "range")?,
            )
        })
        .collect::<Result<Vec<_>>>()?;
    let metadata = PartialRangeWireMetadata::try_new(
        decode_duration(v1.align_millis, "align")?,
        v1.align_to_millis,
        checked_usize(v1.time_column_index, "time index")?,
        v1.by_column_indices
            .into_iter()
            .map(|index| checked_usize(index, "BY index"))
            .collect::<Result<Vec<_>>>()?,
        functions,
    )?;
    Ok(metadata)
}

fn checked_millis(duration: Duration, name: &str) -> Result<u64> {
    let millis = duration.as_millis();
    if millis == 0 || millis > i64::MAX as u128 {
        return Err(DataFusionError::Plan(format!(
            "RangeSelect Partial {name} must be in 1..=i64::MAX milliseconds"
        )));
    }
    u64::try_from(millis)
        .map_err(|_| DataFusionError::Plan(format!("RangeSelect Partial {name} overflows u64")))
}

fn decode_duration(millis: u64, name: &str) -> Result<Duration> {
    if millis == 0 || millis > i64::MAX as u64 {
        return Err(DataFusionError::Substrait(format!(
            "RangeSelect Partial {name} must be in 1..=i64::MAX milliseconds"
        )));
    }
    Ok(Duration::from_millis(millis))
}

fn checked_index(index: usize) -> Result<u64> {
    u64::try_from(index)
        .map_err(|_| DataFusionError::Plan("RangeSelect Partial index overflows u64".to_string()))
}

fn checked_usize(index: u64, name: &str) -> Result<usize> {
    usize::try_from(index).map_err(|_| {
        DataFusionError::Substrait(format!("RangeSelect Partial {name} overflows usize"))
    })
}

fn encode_kind(kind: RangeAggregateKind) -> AggregateKind {
    match kind {
        RangeAggregateKind::Min => AggregateKind::Min,
        RangeAggregateKind::Max => AggregateKind::Max,
        RangeAggregateKind::Sum => AggregateKind::Sum,
        RangeAggregateKind::Count => AggregateKind::Count,
        RangeAggregateKind::Avg => AggregateKind::Avg,
    }
}

fn decode_kind(raw: i32) -> Result<RangeAggregateKind> {
    match AggregateKind::try_from(raw).map_err(|_| {
        DataFusionError::Substrait("Unknown RangeSelect Partial aggregate kind".to_string())
    })? {
        AggregateKind::Min => Ok(RangeAggregateKind::Min),
        AggregateKind::Max => Ok(RangeAggregateKind::Max),
        AggregateKind::Sum => Ok(RangeAggregateKind::Sum),
        AggregateKind::Count => Ok(RangeAggregateKind::Count),
        AggregateKind::Avg => Ok(RangeAggregateKind::Avg),
        AggregateKind::Unspecified => Err(DataFusionError::Substrait(
            "Unspecified RangeSelect Partial aggregate kind".to_string(),
        )),
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct RangeSelectPartialPlaceholder {
    metadata: PartialRangeWireMetadata,
}

impl PartialOrd for RangeSelectPartialPlaceholder {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.metadata.partial_cmp(&other.metadata)
    }
}

impl UserDefinedLogicalNode for RangeSelectPartialPlaceholder {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        RANGE_SELECT_PARTIAL_NODE_NAME
    }
    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![]
    }
    fn schema(&self) -> &datafusion_common::DFSchemaRef {
        static EMPTY: std::sync::OnceLock<datafusion_common::DFSchemaRef> =
            std::sync::OnceLock::new();
        EMPTY.get_or_init(|| Arc::new(DFSchema::empty()))
    }
    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }
    fn check_invariants(&self, _check: InvariantLevel) -> Result<()> {
        Ok(())
    }
    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{RANGE_SELECT_PARTIAL_NODE_NAME}")
    }
    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> Result<Arc<dyn UserDefinedLogicalNode>> {
        if !exprs.is_empty() || inputs.len() != 1 {
            return Err(DataFusionError::Plan(
                "RangeSelect Partial placeholder expects one input and no expressions".to_string(),
            ));
        }
        let input = inputs.into_iter().next().ok_or_else(|| {
            DataFusionError::Plan("RangeSelect Partial placeholder is missing input".to_string())
        })?;
        let LogicalPlan::Projection(decoded) = input else {
            return Err(DataFusionError::Plan(
                "RangeSelect Partial placeholder requires a materialization Projection".to_string(),
            ));
        };
        let expected_slots = 1 + self.metadata.by_indices().len() + self.metadata.functions().len();
        if decoded.expr.len() != expected_slots {
            return Err(DataFusionError::Plan(
                "RangeSelect Partial placeholder received an invalid Projection slot count"
                    .to_string(),
            ));
        }
        if self.metadata.time_index() != 0
            || self
                .metadata
                .by_indices()
                .iter()
                .enumerate()
                .any(|(position, index)| *index != position + 1)
            || self
                .metadata
                .functions()
                .iter()
                .enumerate()
                .any(|(position, function)| {
                    function.argument_index() != 1 + self.metadata.by_indices().len() + position
                })
        {
            return Err(DataFusionError::Plan(
                "RangeSelect Partial placeholder received invalid wire slots".to_string(),
            ));
        }
        let mut canonical_exprs = Vec::with_capacity(expected_slots);
        for index in std::iter::once(self.metadata.time_index())
            .chain(self.metadata.by_indices().iter().copied())
        {
            let Expr::Column(column) = &decoded.expr[index] else {
                return Err(DataFusionError::Plan(
                    "RangeSelect Partial placeholder time/BY slot must be a direct column"
                        .to_string(),
                ));
            };
            canonical_exprs.push(
                Expr::Column(column.clone()).alias(decoded.schema.field(index).name().clone()),
            );
        }
        for (position, function) in self.metadata.functions().iter().enumerate() {
            let decoded_expr = &decoded.expr[function.argument_index()];
            let inner = match decoded_expr {
                Expr::Alias(alias) => alias.expr.as_ref().clone(),
                expr => expr.clone(),
            };
            canonical_exprs.push(inner.alias(format!("__range_arg_{position}")));
        }
        let canonical = Projection::try_new(canonical_exprs, decoded.input.clone())?;
        Ok(Arc::new(RangeSelect::try_new_partial_from_wire(
            Arc::new(LogicalPlan::Projection(canonical)),
            self.metadata.clone(),
        )?))
    }
    fn dyn_hash(&self, state: &mut dyn Hasher) {
        let mut state = state;
        self.hash(&mut state);
    }
    fn dyn_eq(&self, other: &dyn UserDefinedLogicalNode) -> bool {
        other.as_any().downcast_ref::<Self>() == Some(self)
    }
    fn dyn_ord(&self, other: &dyn UserDefinedLogicalNode) -> Option<Ordering> {
        other
            .as_any()
            .downcast_ref::<Self>()
            .and_then(|other| self.partial_cmp(other))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bytes(v1: RangeSelectPartialV1) -> Vec<u8> {
        RangeSelectPartial {
            payload: Some(range_select_partial::Payload::V1(v1)),
        }
        .encode_to_vec()
    }

    fn valid_v1() -> RangeSelectPartialV1 {
        RangeSelectPartialV1 {
            align_millis: 1,
            align_to_millis: 0,
            time_column_index: 0,
            by_column_indices: vec![1],
            range_functions: vec![RangeFunctionV1 {
                aggregate: AggregateKind::Avg as i32,
                argument_column_index: 2,
                range_millis: 1,
            }],
        }
    }

    #[test]
    fn strict_metadata_roundtrip_and_malformed_payloads() {
        let decoded = decode_metadata(&bytes(valid_v1())).unwrap();
        assert_eq!(decoded.align(), Duration::from_millis(1));
        assert_eq!(decoded.time_index(), 0);
        assert_eq!(decoded.by_indices(), &[1]);
        assert_eq!(decoded.functions()[0].kind(), RangeAggregateKind::Avg);

        assert!(decode_metadata(&RangeSelectPartial { payload: None }.encode_to_vec()).is_err());
        for aggregate in [i32::MAX, AggregateKind::Unspecified as i32] {
            let mut v1 = valid_v1();
            v1.range_functions[0].aggregate = aggregate;
            assert!(decode_metadata(&bytes(v1)).is_err());
        }
        let mut empty = valid_v1();
        empty.range_functions.clear();
        assert!(decode_metadata(&bytes(empty)).is_err());
        for (align, range) in [
            (0, 1),
            (i64::MAX as u64 + 1, 1),
            (1, 0),
            (1, i64::MAX as u64 + 1),
        ] {
            let mut v1 = valid_v1();
            v1.align_millis = align;
            v1.range_functions[0].range_millis = range;
            assert!(decode_metadata(&bytes(v1)).is_err());
        }
    }
}
