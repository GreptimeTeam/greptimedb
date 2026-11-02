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

use std::sync::Arc;

use arrow::array::{Array, BooleanArray, Float64Array, ListArray, StructArray};
use arrow::datatypes::DataType;

use crate::error::{InvalidSeriesSnafu, Result};
use crate::series::SeriesLabels;
use crate::series::time::TimeAxis;

/// Aggregate whose accumulator is represented by a state frame.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AggregateStateKind {
    Sum,
    Count,
    Avg,
    Min,
    Max,
    Group,
    Stddev,
    Stdvar,
}

impl AggregateStateKind {
    /// Canonical metadata spelling.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Sum => "sum",
            Self::Count => "count",
            Self::Avg => "avg",
            Self::Min => "min",
            Self::Max => "max",
            Self::Group => "group",
            Self::Stddev => "stddev",
            Self::Stdvar => "stdvar",
        }
    }
    /// Parses a canonical aggregate name without treating unknown kinds as another state.
    pub fn parse(text: &str) -> Result<Self> {
        match text {
            "sum" => Ok(Self::Sum),
            "count" => Ok(Self::Count),
            "avg" => Ok(Self::Avg),
            "min" => Ok(Self::Min),
            "max" => Ok(Self::Max),
            "group" => Ok(Self::Group),
            "stddev" => Ok(Self::Stddev),
            "stdvar" => Ok(Self::Stdvar),
            _ => invalid("unknown aggregate state kind"),
        }
    }
    fn components(self) -> &'static [StateComponent] {
        use StateComponent as C;
        match self {
            Self::Sum => &[C::Value, C::Compensation],
            Self::Count => &[C::Count],
            Self::Avg => &[
                C::Value,
                C::Mean,
                C::Compensation,
                C::Count,
                C::IncrementalMean,
            ],
            Self::Min | Self::Max => &[C::Value],
            Self::Group => &[],
            Self::Stddev | Self::Stdvar => &[C::Value, C::Mean, C::Count],
        }
    }
}

/// One named accumulator column.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum StateComponent {
    Value,
    Mean,
    Compensation,
    Count,
    IncrementalMean,
}

impl StateComponent {
    /// Canonical metadata spelling.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Value => "value",
            Self::Mean => "mean",
            Self::Compensation => "compensation",
            Self::Count => "count",
            Self::IncrementalMean => "incremental_mean",
        }
    }
    /// Parses a canonical component name, refusing unknown wire values.
    pub fn parse(text: &str) -> Result<Self> {
        match text {
            "value" => Ok(Self::Value),
            "mean" => Ok(Self::Mean),
            "compensation" => Ok(Self::Compensation),
            "count" => Ok(Self::Count),
            "incremental_mean" => Ok(Self::IncrementalMean),
            _ => invalid("unknown aggregate state component"),
        }
    }
}

/// Values for all accumulator components at one field and grid position.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct AggregateStateValues {
    /// Accumulator value.
    pub value: f64,
    /// Running mean.
    pub mean: f64,
    /// Compensation retained for stable summation.
    pub compensation: f64,
    /// Observation count represented as a floating-point accumulator.
    pub count: f64,
    /// Whether the mean is incremental.
    pub incremental_mean: bool,
}

/// Validated aggregate kind and ordered component layout.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AggregateStateSchema {
    pub kind: AggregateStateKind,
    pub components: Arc<[StateComponent]>,
}

impl AggregateStateSchema {
    /// Creates a schema only when its ordered components exactly describe its kind.
    pub fn try_new(kind: AggregateStateKind, components: Vec<StateComponent>) -> Result<Self> {
        if kind != AggregateStateKind::Group && components.is_empty() {
            return invalid("aggregate state components must not be empty");
        }
        if components.as_slice() != kind.components() {
            return invalid("aggregate state components do not match kind");
        }
        Ok(Self {
            kind,
            components: components.into(),
        })
    }
    /// Parses the aggregate kind and ordered comma-separated component metadata.
    pub fn from_text(kind: &str, components: &str) -> Result<Self> {
        let kind = AggregateStateKind::parse(kind)?;
        let parsed = if components.is_empty() {
            Vec::new()
        } else {
            components
                .split(',')
                .map(StateComponent::parse)
                .collect::<Result<Vec<_>>>()?
        };
        Self::try_new(kind, parsed)
    }
    /// Returns the canonical ordered component metadata text.
    pub fn components_text(&self) -> String {
        self.components
            .iter()
            .map(|component| component.as_str())
            .collect::<Vec<_>>()
            .join(",")
    }
}

/// A group-labelled sparse aggregate-state chunk on a shared time axis.
#[derive(Clone, Debug)]
pub struct StateSeries {
    /// Complete group identity.
    pub metric: SeriesLabels,
    /// Grid positions carried by this chunk.
    pub time: TimeAxis,
    /// Field-major states; `None` denotes a field position without a contribution.
    pub states: Vec<Vec<Option<AggregateStateValues>>>,
}

fn invalid<T>(reason: &str) -> Result<T> {
    InvalidSeriesSnafu {
        reason: reason.to_string(),
    }
    .fail()
}

/// Columnar state fields retained in Arrow form so consumers can read component values in place.
/// Null items are field-local holes, not zero-valued states.
#[derive(Clone, Debug)]
pub struct StateFieldsBatch {
    kind: AggregateStateKind,
    components: Arc<[StateComponent]>,
    fields: Vec<StructArray>,
}

impl StateFieldsBatch {
    /// Validates the ordered component layout once and keeps the row's typed Arrow arrays.
    pub fn try_from_arrow(fields: StructArray, schema: &AggregateStateSchema) -> Result<Self> {
        if fields.num_columns() == 0 {
            return invalid("state fields array has no fields");
        }
        let mut items = Vec::with_capacity(fields.num_columns());
        for (column, field) in fields.columns().iter().zip(fields.fields()) {
            let DataType::List(item) = field.data_type() else {
                return invalid("state field is not a list");
            };
            let DataType::Struct(members) = item.data_type() else {
                return invalid("state list item is not a struct");
            };
            if members.len() != schema.components.len() {
                return invalid("state component count mismatch");
            }
            for (member, expected) in members.iter().zip(schema.components.iter()) {
                if member.name() != expected.as_str() {
                    return invalid("state component order mismatch");
                }
                let ty = if *expected == StateComponent::IncrementalMean {
                    DataType::Boolean
                } else {
                    DataType::Float64
                };
                if member.data_type() != &ty {
                    return invalid("state component type mismatch");
                }
            }
            let list = column
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| invalid_series("state field list type mismatch"))?;
            if list.is_null(0) {
                return invalid("null state field list");
            }
            let item_array = list.value(0);
            let item_struct = item_array
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| invalid_series("state item struct type mismatch"))?;
            items.push(item_struct.clone());
        }
        Ok(Self {
            kind: schema.kind,
            components: schema.components.clone(),
            fields: items,
        })
    }
    /// Number of logical series fields in this row.
    pub fn len(&self) -> usize {
        self.fields.len()
    }
    /// Whether this wrapper contains no series fields.
    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }
    /// The item struct for one field's list, sliced to the decoded row.
    pub fn field(&self, index: usize) -> Option<&StructArray> {
        self.fields.get(index)
    }
    /// Aggregate kind represented by these columns.
    pub fn kind(&self) -> AggregateStateKind {
        self.kind
    }
    /// Declared accumulator components in wire order.
    pub fn components(&self) -> &[StateComponent] {
        &self.components
    }
    /// Returns a numeric component array, or `None` if the component is not declared.
    pub fn field_values(
        &self,
        field: usize,
        component: StateComponent,
    ) -> Result<Option<&Float64Array>> {
        let Some(index) = self.components.iter().position(|value| *value == component) else {
            return Ok(None);
        };
        self.fields
            .get(field)
            .ok_or_else(|| invalid_series("state field index out of range"))?
            .column(index)
            .as_any()
            .downcast_ref::<Float64Array>()
            .map(Some)
            .ok_or_else(|| invalid_series("state float component type mismatch"))
    }
    /// Returns a Boolean component array, or `None` if the component is not declared.
    pub fn field_bool_values(
        &self,
        field: usize,
        component: StateComponent,
    ) -> Result<Option<&BooleanArray>> {
        let Some(index) = self.components.iter().position(|value| *value == component) else {
            return Ok(None);
        };
        self.fields
            .get(field)
            .ok_or_else(|| invalid_series("state field index out of range"))?
            .column(index)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .map(Some)
            .ok_or_else(|| invalid_series("state boolean component type mismatch"))
    }
    /// Reads this field's item validity at one carried position; sibling fields never fill holes.
    pub fn is_present(&self, field: usize, position: usize) -> Result<bool> {
        let items = self
            .fields
            .get(field)
            .ok_or_else(|| invalid_series("state field index out of range"))?;
        if position >= items.len() {
            return invalid("state position out of range");
        }
        Ok(!items.is_null(position))
    }
}

fn invalid_series(reason: &str) -> crate::error::Error {
    InvalidSeriesSnafu {
        reason: reason.to_string(),
    }
    .build()
}
