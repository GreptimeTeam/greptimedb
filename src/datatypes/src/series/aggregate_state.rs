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

use std::mem::size_of;
use std::sync::Arc;

use arrow::array::{Array, BooleanArray, Float64Array, ListArray, StructArray};
use arrow::datatypes::DataType;

use crate::error::{InvalidSeriesSnafu, Result};
use crate::series::SeriesLabels;
use crate::series::histogram::{
    BucketSideView, HistogramBatch, HistogramView, fields as histogram_fields,
};
use crate::series::time::TimeAxis;

/// Aggregate whose accumulator is represented by a state frame.
///
/// Two families share one state vector: the numeric kinds below, whose components are scalar, and
/// the two histogram kinds, whose payload is a native histogram ([`HistogramState`]). A kind's
/// components are fixed by [`AggregateStateKind::components`], so a histogram kind can never declare
/// a numeric component and a sum state can never claim mean mode.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum AggregateStateKind {
    Sum,
    Count,
    Avg,
    Min,
    Max,
    Group,
    Stddev,
    Stdvar,
    /// `sum` over native histograms.
    HistogramSum,
    /// `avg` over native histograms.
    HistogramAvg,
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
            Self::HistogramSum => "histogram_sum",
            Self::HistogramAvg => "histogram_avg",
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
            "histogram_sum" => Ok(Self::HistogramSum),
            "histogram_avg" => Ok(Self::HistogramAvg),
            _ => invalid("unknown aggregate state kind"),
        }
    }
    /// Whether this kind's positions carry a native-histogram payload rather than numeric ones.
    pub const fn is_histogram(self) -> bool {
        matches!(self, Self::HistogramSum | Self::HistogramAvg)
    }
    /// The components this kind's state carries, in canonical order.
    ///
    /// [`AggregateStateSchema::try_new`] accepts exactly this list for this kind, so a producer that
    /// binds a state from this table cannot declare a layout the frame would refuse.
    pub fn components(self) -> &'static [StateComponent] {
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
            // The live histogram, its layout-carrying compensation, then the evidence a merge
            // records. A sum has no observation count and no mean mode.
            Self::HistogramSum => &[
                C::HistogramValue,
                C::HistogramCompensation,
                C::Poisoned,
                C::IncompatibleLayout,
                C::CustomBoundsReconciled,
                C::CounterResetSeen,
                C::NotCounterResetSeen,
            ],
            // An average has no separate mean column: `incremental_mean` says whether the live
            // histogram is a cumulative sum or a running mean.
            Self::HistogramAvg => &[
                C::HistogramValue,
                C::HistogramCompensation,
                C::Count,
                C::IncrementalMean,
                C::Poisoned,
                C::IncompatibleLayout,
                C::CustomBoundsReconciled,
                C::CounterResetSeen,
                C::NotCounterResetSeen,
            ],
        }
    }
}

/// One named accumulator column.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum StateComponent {
    Value,
    Mean,
    Compensation,
    Count,
    IncrementalMean,
    /// The live native histogram of a histogram accumulator.
    HistogramValue,
    /// The compensation that carries [`StateComponent::HistogramValue`]'s bucket layout.
    HistogramCompensation,
    /// The point cannot be answered because its inputs disagreed on their bucket layout.
    Poisoned,
    /// Two inputs carried an exponential layout and a custom-buckets layout at once.
    IncompatibleLayout,
    /// A merge reconciled the custom bucket bounds of two inputs.
    CustomBoundsReconciled,
    /// Some input reported a counter reset.
    CounterResetSeen,
    /// Some input reported that no counter reset happened.
    NotCounterResetSeen,
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
            Self::HistogramValue => "histogram_value",
            Self::HistogramCompensation => "histogram_compensation",
            Self::Poisoned => "poisoned",
            Self::IncompatibleLayout => "incompatible_layout",
            Self::CustomBoundsReconciled => "custom_bounds_reconciled",
            Self::CounterResetSeen => "counter_reset_seen",
            Self::NotCounterResetSeen => "not_counter_reset_seen",
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
            "histogram_value" => Ok(Self::HistogramValue),
            "histogram_compensation" => Ok(Self::HistogramCompensation),
            "poisoned" => Ok(Self::Poisoned),
            "incompatible_layout" => Ok(Self::IncompatibleLayout),
            "custom_bounds_reconciled" => Ok(Self::CustomBoundsReconciled),
            "counter_reset_seen" => Ok(Self::CounterResetSeen),
            "not_counter_reset_seen" => Ok(Self::NotCounterResetSeen),
            _ => invalid("unknown aggregate state component"),
        }
    }
    /// The Arrow item type of this component.
    ///
    /// A histogram component is one native histogram per position, in the representation
    /// [`crate::series::histogram`] already defines for sample frames; the two histogram components
    /// of a state therefore nest the same struct a histogram sample does.
    pub(crate) fn element_type(self) -> DataType {
        match self {
            Self::HistogramValue | Self::HistogramCompensation => {
                DataType::Struct(histogram_fields())
            }
            Self::IncrementalMean
            | Self::Poisoned
            | Self::IncompatibleLayout
            | Self::CustomBoundsReconciled
            | Self::CounterResetSeen
            | Self::NotCounterResetSeen => DataType::Boolean,
            Self::Value | Self::Mean | Self::Compensation | Self::Count => DataType::Float64,
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

/// One position's native-histogram accumulator: its live histogram, the compensation that carries
/// the same bucket layout, and the evidence bits a merge records.
///
/// The fields mirror [`AggregateStateValues`]: every component a histogram kind can declare has a
/// slot, and the ones the kind does not declare stay at their default. `HistogramSum` declares no
/// `count` and no `incremental_mean`, which is what makes a sum state unable to claim mean mode.
#[derive(Clone, Debug)]
pub struct HistogramState {
    /// The live histogram, held as one row of the frame's own histogram representation.
    pub value: Arc<HistogramBatch>,
    /// Compensation retained for a stable sum of the value's buckets.
    ///
    /// It always carries the live value's bucket layout and is never an `empty_like` placeholder: the
    /// kernel's compensated add indexes this histogram by the value's bucket positions.
    pub compensation: Arc<HistogramBatch>,
    /// Number of observations folded in. A `HistogramSum` state leaves it at zero.
    pub count: f64,
    /// Whether an average switched from a running sum to a running mean.
    pub incremental_mean: bool,
    /// Whether the point cannot be answered because its inputs disagreed on their bucket layout.
    pub poisoned: bool,
    /// Whether two inputs carried an exponential layout and a custom-buckets layout at once.
    pub incompatible_layout: bool,
    /// Whether a merge reconciled the custom bucket bounds of two inputs.
    pub custom_bounds_reconciled: bool,
    /// Whether some input reported a counter reset.
    pub counter_reset_seen: bool,
    /// Whether some input reported that no counter reset happened.
    pub not_counter_reset_seen: bool,
}

impl HistogramState {
    /// Checks the compensation invariant: a live value's compensation carries its bucket layout.
    ///
    /// A poisoned position holds an empty placeholder on both sides -- it has no bucket indices to
    /// align and is removed rather than merged -- so it only has to be one histogram per component.
    pub fn validate_layout(&self) -> Result<()> {
        let value = single_histogram(&self.value)?;
        let compensation = single_histogram(&self.compensation)?;
        if self.poisoned {
            return Ok(());
        }
        check_layout(value, compensation)
    }

    /// Reads the evidence bit a Boolean component declares, if this payload records it.
    pub fn bool_component(&self, component: StateComponent) -> Option<bool> {
        match component {
            StateComponent::IncrementalMean => Some(self.incremental_mean),
            StateComponent::Poisoned => Some(self.poisoned),
            StateComponent::IncompatibleLayout => Some(self.incompatible_layout),
            StateComponent::CustomBoundsReconciled => Some(self.custom_bounds_reconciled),
            StateComponent::CounterResetSeen => Some(self.counter_reset_seen),
            StateComponent::NotCounterResetSeen => Some(self.not_counter_reset_seen),
            _ => None,
        }
    }

    /// Heap bytes both histograms retain, counted from the Arrow arrays themselves so a wide bucket
    /// layout is paid for and a narrow one is not.
    pub fn heap_bytes(&self) -> usize {
        self.value
            .as_arrow()
            .get_array_memory_size()
            .saturating_add(self.compensation.as_arrow().get_array_memory_size())
    }
}

/// One grid position's aggregate state: numeric components, or a native-histogram payload.
///
/// A field holds either family at a position, never both, and a kind's component list says which one
/// its positions carry. Keeping both in one vector is what leaves absence a single `None` rather
/// than a second representation of it.
#[derive(Clone, Debug)]
pub enum StatePayload {
    /// Numeric accumulator components, byte-identical to what a float state always carried.
    Float(AggregateStateValues),
    /// A native-histogram accumulator.
    Histogram(Box<HistogramState>),
}

impl StatePayload {
    /// The numeric components, when this position holds numeric state.
    pub fn float(&self) -> Option<&AggregateStateValues> {
        match self {
            Self::Float(values) => Some(values),
            Self::Histogram(_) => None,
        }
    }
    /// The histogram payload, when this position holds histogram state.
    pub fn histogram(&self) -> Option<&HistogramState> {
        match self {
            Self::Float(_) => None,
            Self::Histogram(state) => Some(state),
        }
    }
    /// Reads the numeric component a state item declares. A component this payload's family does not
    /// keep as a scalar is `None`, which is how a writer refuses a payload its kind cannot describe.
    pub fn float_component(&self, component: StateComponent) -> Option<f64> {
        match self {
            Self::Float(values) => match component {
                StateComponent::Value => Some(values.value),
                StateComponent::Mean => Some(values.mean),
                StateComponent::Compensation => Some(values.compensation),
                StateComponent::Count => Some(values.count),
                _ => None,
            },
            Self::Histogram(state) => match component {
                StateComponent::Count => Some(state.count),
                _ => None,
            },
        }
    }
    /// Reads the Boolean component a state item declares, or `None` if this payload does not keep it.
    pub fn bool_component(&self, component: StateComponent) -> Option<bool> {
        match self {
            Self::Float(values) => match component {
                StateComponent::IncrementalMean => Some(values.incremental_mean),
                _ => None,
            },
            Self::Histogram(state) => state.bool_component(component),
        }
    }
    /// Reads the histogram component a state item declares, or `None` if this payload keeps none.
    pub fn histogram_component(&self, component: StateComponent) -> Option<&Arc<HistogramBatch>> {
        let Self::Histogram(state) = self else {
            return None;
        };
        match component {
            StateComponent::HistogramValue => Some(&state.value),
            StateComponent::HistogramCompensation => Some(&state.compensation),
            _ => None,
        }
    }
    /// Heap bytes this payload holds beyond the slot that stores it.
    ///
    /// Numeric components are inline and are paid for by the slot; a histogram payload boxes its
    /// state and both batches own Arrow buffers whose width follows the bucket layout.
    pub fn heap_bytes(&self) -> usize {
        match self {
            Self::Float(_) => 0,
            Self::Histogram(state) => {
                size_of::<HistogramState>().saturating_add(state.heap_bytes())
            }
        }
    }
}

/// The single histogram a state component carries, or an error naming the violation.
fn single_histogram(batch: &HistogramBatch) -> Result<HistogramView<'_>> {
    if batch.len() != 1 {
        return invalid("a histogram state component carries exactly one histogram");
    }
    batch
        .view(0)
        .ok_or_else(|| invalid_series("empty histogram state component"))
}

/// Compares the layout a merge reads, not the wire encoding: span lists that spell the same buckets
/// differently agree, because what the kernel indexes is the expanded bucket index.
fn check_layout(value: HistogramView<'_>, compensation: HistogramView<'_>) -> Result<()> {
    if value.schema != compensation.schema {
        return invalid("histogram compensation schema does not match its value");
    }
    if value.zero_threshold.to_bits() != compensation.zero_threshold.to_bits() {
        return invalid("histogram compensation zero threshold does not match its value");
    }
    if !same_bounds(value.custom_values, compensation.custom_values) {
        return invalid("histogram compensation custom bounds do not match its value");
    }
    if bucket_indices(value.positive) != bucket_indices(compensation.positive)
        || bucket_indices(value.negative) != bucket_indices(compensation.negative)
    {
        return invalid("histogram compensation bucket indices do not match its value");
    }
    Ok(())
}

fn same_bounds(lhs: &[f64], rhs: &[f64]) -> bool {
    lhs.len() == rhs.len()
        && lhs
            .iter()
            .zip(rhs)
            .all(|(lhs, rhs)| lhs.to_bits() == rhs.to_bits())
}

/// The logical bucket indices one side's spans expand to, in ascending order.
fn bucket_indices(side: BucketSideView<'_>) -> Vec<i32> {
    let mut indices = Vec::with_capacity(side.buckets.len());
    let mut cursor = 0i32;
    for (span, (&offset, &length)) in side.span_offsets.iter().zip(side.span_lengths).enumerate() {
        cursor = if span == 0 {
            offset
        } else {
            cursor.saturating_add(offset)
        };
        for index in cursor..cursor.saturating_add(length) {
            indices.push(index);
        }
        cursor = cursor.saturating_add(length);
    }
    indices
}

/// Validated aggregate kind and ordered component layout.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AggregateStateSchema {
    pub kind: AggregateStateKind,
    pub components: Arc<[StateComponent]>,
}

impl AggregateStateSchema {
    /// Creates a schema only when its ordered components exactly describe its kind.
    ///
    /// The comparison is over the whole list, which is what forbids mixing the two payload families
    /// (a histogram kind declaring a numeric component, or the other way round) and what keeps
    /// `HistogramSum` from declaring `incremental_mean`.
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
    pub states: Vec<Vec<Option<StatePayload>>>,
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
                if member.data_type() != &expected.element_type() {
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
    /// Returns the histogram component's per-position column, or `None` if it is not declared.
    ///
    /// One row of this column is one histogram, so a state position's payload is that row as its own
    /// one-row batch.
    pub fn field_histograms(
        &self,
        field: usize,
        component: StateComponent,
    ) -> Result<Option<&StructArray>> {
        let Some(index) = self.components.iter().position(|value| *value == component) else {
            return Ok(None);
        };
        self.fields
            .get(field)
            .ok_or_else(|| invalid_series("state field index out of range"))?
            .column(index)
            .as_any()
            .downcast_ref::<StructArray>()
            .map(Some)
            .ok_or_else(|| invalid_series("state histogram component type mismatch"))
    }
    /// Reads one position's histogram component, or `None` when that position is absent.
    pub fn field_histogram(
        &self,
        field: usize,
        component: StateComponent,
        position: usize,
    ) -> Result<Option<HistogramBatch>> {
        let Some(column) = self.field_histograms(field, component)? else {
            return Ok(None);
        };
        if position >= column.len() {
            return invalid("state position out of range");
        }
        if column.is_null(position) {
            return Ok(None);
        }
        HistogramBatch::try_from_arrow(column.slice(position, 1)).map(Some)
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
