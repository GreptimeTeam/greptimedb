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

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use common_error::ext::BoxedError;
use common_recordbatch::RecordBatch;
use datafusion::arrow::array::{
    Array, ArrayRef, Float64Array, ListArray, StructArray, TimestampMillisecondArray, UInt64Array,
};
use datafusion::arrow::buffer::OffsetBuffer;
use datafusion::arrow::datatypes::DataType;
use datafusion::execution::memory_pool::MemoryReservation;
use datafusion_common::{DataFusionError, Result, ScalarValue};
use datatypes::schema::SchemaRef;
use store_api::region_engine::{ScanProcessor, SeriesBlockProcessor, VisibleSeriesBlock};

use super::grid::TimeGrid;

const STALE: u64 = 0x7ff0_0000_0000_0002;

#[derive(Debug, Clone)]
pub(crate) struct InstantMaxSpec {
    pub schema: SchemaRef,
    pub input_schema: datafusion::arrow::datatypes::SchemaRef,
    pub groups: Vec<usize>,
    pub timestamp: usize,
    pub value: usize,
    pub tsid: usize,
    pub grid: Arc<TimeGrid>,
    pub lookback: i64,
    pub offset: i64,
    /// Sum instead of max; both keep DataFusion's partial-state shape (one Float64).
    pub sum: bool,
}

struct Group {
    labels: Vec<ScalarValue>,
    present: Vec<bool>,
    valid: Vec<bool>,
    accumulated: Vec<f64>,
}

impl Group {
    fn update(&mut self, index: usize, value: Option<f64>, sum: bool) {
        self.present[index] = true;
        if let Some(value) = value {
            if sum {
                // The first value is taken as is so a lone -0.0 keeps its sign, as in DataFusion.
                self.accumulated[index] = if self.valid[index] {
                    self.accumulated[index] + value
                } else {
                    value
                };
            } else if matches!(
                // Match DataFusion's grouped max, including NaN and signed zero.
                value.partial_cmp(&self.accumulated[index]),
                Some(std::cmp::Ordering::Greater) | None
            ) {
                self.accumulated[index] = value;
            }
            self.valid[index] = true;
        }
    }
}

struct Cursor {
    tsid: u64,
    group: usize,
    next: usize,
    latest: Option<(i128, Option<f64>)>,
}

pub(crate) struct InstantMaxProcessor {
    spec: Arc<InstantMaxSpec>,
    index: HashMap<Vec<ScalarValue>, usize>,
    groups: Vec<Group>,
    current: Option<Cursor>,
    memory: MemoryReservation,
    cancelled: Arc<AtomicBool>,
    packed_schema: Option<SchemaRef>,
    direct_output: bool,
}

fn invalid(message: &str) -> DataFusionError {
    DataFusionError::Execution(message.into())
}

fn boxed(error: DataFusionError) -> BoxedError {
    BoxedError::new(crate::error::Error::from(error))
}

impl InstantMaxProcessor {
    pub(crate) fn new(
        spec: Arc<InstantMaxSpec>,
        memory: MemoryReservation,
        cancelled: Arc<AtomicBool>,
    ) -> Result<Self> {
        if spec.lookback < 0 || spec.grid.instants.is_empty() {
            return Err(invalid("Invalid scan aggregation grid"));
        }
        Ok(Self {
            spec,
            index: HashMap::new(),
            groups: Vec::new(),
            current: None,
            memory,
            cancelled,
            packed_schema: None,
            direct_output: false,
        })
    }

    pub(crate) fn new_series(
        spec: Arc<InstantMaxSpec>,
        memory: MemoryReservation,
        cancelled: Arc<AtomicBool>,
    ) -> Result<Self> {
        let mut processor = Self::new(spec, memory, cancelled)?;
        processor.direct_output = true;
        Ok(processor)
    }

    pub(crate) fn with_packed_schema(mut self, schema: SchemaRef) -> Self {
        self.packed_schema = Some(schema);
        self
    }

    fn check_cancelled(&self) -> Result<()> {
        if self.cancelled.load(Ordering::Relaxed) {
            return Err(invalid("Scan aggregation cancelled"));
        }
        Ok(())
    }

    fn emit_before(&mut self, boundary: i128) -> Result<()> {
        let Some(cursor) = &mut self.current else {
            return Ok(());
        };
        let group = &mut self.groups[cursor.group];
        while cursor.next < self.spec.grid.instants.len()
            && i128::from(self.spec.grid.instants[cursor.next]) < boundary
        {
            if cursor.next % 1024 == 0 && self.cancelled.load(Ordering::Relaxed) {
                return Err(invalid("Scan aggregation cancelled"));
            }
            let index = cursor.next;
            let end = i128::from(self.spec.grid.instants[index]);
            if let Some((timestamp, value)) = cursor.latest
                && (timestamp == end || timestamp > end - i128::from(self.spec.lookback))
                && !value.is_some_and(|value| value.to_bits() == STALE)
            {
                group.update(index, value, self.spec.sum);
            }
            cursor.next += 1;
        }
        Ok(())
    }

    fn start_series(&mut self, batch: &RecordBatch, row: usize, tsid: u64) -> Result<()> {
        self.emit_before(i128::MAX)?;
        // Charge temporary scalar extraction before cloning variable-length labels.
        let temporary = self.memory.new_empty();
        let bytes = self
            .spec
            .groups
            .iter()
            .filter(|&&column| column != self.spec.timestamp)
            .try_fold(0usize, |sum, &column| {
                sum.checked_add(batch.column(column).get_array_memory_size())
                    .ok_or_else(|| invalid("Scan label memory size overflow"))
            })?;
        temporary.try_grow(bytes)?;
        let labels = self
            .spec
            .groups
            .iter()
            .filter(|&&column| column != self.spec.timestamp)
            .map(|&column| ScalarValue::try_from_array(batch.column(column), row))
            .collect::<Result<Vec<_>>>()?;
        self.start_values(labels, tsid)
    }

    fn start_values(&mut self, labels: Vec<ScalarValue>, tsid: u64) -> Result<()> {
        let group = if let Some(&group) = self.index.get(&labels) {
            group
        } else {
            let points = self.spec.grid.instants.len();
            let labels_size = labels.iter().map(ScalarValue::size).sum::<usize>();
            let bytes = points
                .checked_mul(10)
                .and_then(|bytes| bytes.checked_add(labels_size.checked_mul(2)?))
                .and_then(|bytes| bytes.checked_add(512))
                .ok_or_else(|| invalid("Scan aggregate memory size overflow"))?;
            self.memory.try_grow(bytes)?;
            let group = self.groups.len();
            self.groups.push(Group {
                labels: labels.clone(),
                present: vec![false; points],
                valid: vec![false; points],
                accumulated: vec![f64::MIN; points],
            });
            self.index.insert(labels, group);
            group
        };
        self.current = Some(Cursor {
            tsid,
            group,
            next: 0,
            latest: None,
        });
        Ok(())
    }

    fn consume(&mut self, batch: &RecordBatch) -> Result<()> {
        self.check_cancelled()?;
        let times = batch
            .column(self.spec.timestamp)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .ok_or_else(|| invalid("Scan aggregation requires millisecond timestamps"))?;
        let values = batch
            .column(self.spec.value)
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| invalid("Scan aggregation requires Float64 samples"))?;
        let tsids = batch
            .column(self.spec.tsid)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| invalid("Scan aggregation requires UInt64 series identifiers"))?;
        if times.null_count() != 0 || tsids.null_count() != 0 {
            return Err(invalid(
                "Null timestamp or series identifier in scan aggregation",
            ));
        }
        for row in 0..batch.num_rows() {
            let tsid = tsids.value(row);
            if self
                .current
                .as_ref()
                .is_none_or(|cursor| cursor.tsid != tsid)
            {
                self.start_series(batch, row, tsid)?;
            }
            let timestamp = i128::from(times.value(row)) + i128::from(self.spec.offset);
            if self
                .current
                .as_ref()
                .and_then(|cursor| cursor.latest)
                .is_some_and(|(previous, _)| timestamp < previous)
            {
                return Err(invalid("Unordered visible samples in scan aggregation"));
            }
            self.emit_before(timestamp)?;
            let value = (!values.is_null(row)).then(|| values.value(row));
            if let Some(cursor) = &mut self.current {
                cursor.latest = Some((timestamp, value));
            }
        }
        Ok(())
    }

    pub(crate) fn finish_series(
        mut self,
    ) -> Result<(Vec<super::scan_range::SeriesGrid>, MemoryReservation)> {
        self.check_cancelled()?;
        self.emit_before(i128::MAX)?;
        let mut output = Vec::new();
        for group in std::mem::take(&mut self.groups) {
            self.check_cancelled()?;
            if !group.valid.iter().any(|&valid| valid) {
                continue;
            }
            self.memory.try_grow(
                group
                    .accumulated
                    .len()
                    .checked_mul(32)
                    .and_then(|bytes| bytes.checked_add(256))
                    .ok_or_else(|| invalid("Series max output size overflow"))?,
            )?;
            let values = group
                .accumulated
                .into_iter()
                .zip(group.valid)
                .map(|(value, valid)| valid.then_some(value))
                .collect();
            output.push(super::scan_range::SeriesGrid {
                labels: group.labels,
                values,
            });
        }
        Ok((output, self.memory))
    }

    fn output(&mut self) -> Result<Vec<RecordBatch>> {
        self.check_cancelled()?;
        self.emit_before(i128::MAX)?;
        if let Some(schema) = &self.packed_schema {
            return self.output_packed(schema);
        }
        let mut batches = Vec::new();
        for group in &self.groups {
            let rows = group.present.iter().filter(|&&present| present).count();
            if rows == 0 {
                continue;
            }
            let per_row = group
                .labels
                .iter()
                .map(ScalarValue::size)
                .sum::<usize>()
                .checked_add((self.spec.groups.len() + 1) * size_of::<ScalarValue>() + 32)
                .ok_or_else(|| invalid("Scan output memory size overflow"))?;
            self.memory.try_grow(
                rows.checked_mul(per_row)
                    .and_then(|bytes| bytes.checked_mul(2))
                    .ok_or_else(|| invalid("Scan output memory size overflow"))?,
            )?;
            let positions = group
                .present
                .iter()
                .enumerate()
                .filter_map(|(index, &present)| present.then_some(index))
                .collect::<Vec<_>>();
            let mut arrays = Vec::with_capacity(self.spec.groups.len() + 1);
            let mut label = 0;
            for &column in &self.spec.groups {
                let array = if column == self.spec.timestamp {
                    ScalarValue::iter_to_array(positions.iter().map(|&index| {
                        ScalarValue::TimestampMillisecond(
                            Some(self.spec.grid.instants[index]),
                            None,
                        )
                    }))?
                } else {
                    let value = &group.labels[label];
                    label += 1;
                    ScalarValue::iter_to_array((0..rows).map(|_| value.clone()))?
                };
                arrays.push(array);
            }
            arrays.push(Arc::new(Float64Array::from_iter(positions.iter().map(
                |&index| group.valid[index].then_some(group.accumulated[index]),
            ))));
            let batch = datafusion::arrow::record_batch::RecordBatch::try_new(
                self.spec.schema.arrow_schema().clone(),
                arrays,
            )?;
            batches.push(RecordBatch::from_df_record_batch(
                self.spec.schema.clone(),
                batch,
            ));
        }
        Ok(batches)
    }

    fn output_packed(&self, schema: &SchemaRef) -> Result<Vec<RecordBatch>> {
        let labels = self.spec.groups.len() - 1;
        let DataType::List(item) = schema.arrow_schema().field(labels).data_type() else {
            return Err(invalid("Packed max requires a list"));
        };
        let DataType::Struct(fields) = item.data_type() else {
            return Err(invalid("Packed max requires paired points"));
        };
        self.memory
            .try_grow(self.spec.grid.instants.len().saturating_mul(8))?;
        let shared_times: ArrayRef = Arc::new(TimestampMillisecondArray::from_iter_values(
            self.spec.grid.instants.iter().copied(),
        ));
        let mut batches = Vec::new();
        for group in &self.groups {
            self.check_cancelled()?;
            let count = group.present.iter().filter(|&&present| present).count();
            if count == 0 {
                continue;
            }
            let end = i32::try_from(count).map_err(|_| invalid("Packed max exceeds list limit"))?;
            let bytes = count
                .checked_mul(64)
                .and_then(|bytes| bytes.checked_add(1024))
                .and_then(|bytes| {
                    group.labels.iter().try_fold(bytes, |bytes, label| {
                        bytes.checked_add(label.size().checked_mul(4)?)
                    })
                })
                .ok_or_else(|| invalid("Packed max output size overflow"))?;
            self.memory.try_grow(bytes)?;
            let positions = group
                .present
                .iter()
                .enumerate()
                .filter_map(|(index, &present)| present.then_some(index))
                .collect::<Vec<_>>();
            let times = if count == self.spec.grid.instants.len() {
                shared_times.clone()
            } else {
                Arc::new(TimestampMillisecondArray::from_iter_values(
                    positions
                        .iter()
                        .map(|&index| self.spec.grid.instants[index]),
                )) as ArrayRef
            };
            let values: ArrayRef =
                Arc::new(Float64Array::from_iter(positions.iter().map(|&index| {
                    group.valid[index].then_some(group.accumulated[index])
                })));
            let points = StructArray::try_new(fields.clone(), vec![times, values], None)?;
            let packed = ListArray::try_new(
                item.clone(),
                OffsetBuffer::new(vec![0, end].into()),
                Arc::new(points),
                None,
            )?;
            let mut columns = group
                .labels
                .iter()
                .map(|label| label.to_array_of_size(1))
                .collect::<Result<Vec<_>>>()?;
            columns.push(Arc::new(packed));
            let batch = datafusion::arrow::record_batch::RecordBatch::try_new(
                schema.arrow_schema().clone(),
                columns,
            )?;
            batches.push(RecordBatch::from_df_record_batch(schema.clone(), batch));
        }
        Ok(batches)
    }

    pub(crate) fn merge_packed(
        &mut self,
        batch: &datafusion::arrow::record_batch::RecordBatch,
    ) -> Result<()> {
        self.check_cancelled()?;
        let labels_count = self.spec.groups.len() - 1;
        let packed = batch
            .column(labels_count)
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| invalid("Invalid packed max list"))?;
        for row in 0..batch.num_rows() {
            if packed.is_null(row) {
                return Err(invalid("Null packed max list"));
            }
            let labels = (0..labels_count)
                .map(|column| ScalarValue::try_from_array(batch.column(column), row))
                .collect::<Result<Vec<_>>>()?;
            self.start_values(labels, 0)?;
            let group = self
                .current
                .take()
                .ok_or_else(|| invalid("Missing packed max group"))?
                .group;
            let points = packed.value(row);
            let points = points
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| invalid("Invalid packed max points"))?;
            let times = points
                .column(0)
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .ok_or_else(|| invalid("Invalid packed max timestamps"))?;
            let values = points
                .column(1)
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| invalid("Invalid packed max values"))?;
            if points.null_count() != 0 || times.null_count() != 0 {
                return Err(invalid("Null packed max point or timestamp"));
            }
            let dense = times.values().as_ref() == self.spec.grid.instants.as_slice();
            for index in 0..points.len() {
                if index % 1024 == 0 {
                    self.check_cancelled()?;
                }
                let position = if dense {
                    index
                } else {
                    self.spec
                        .grid
                        .instants
                        .binary_search(&times.value(index))
                        .map_err(|_| invalid("Packed max timestamp outside grid"))?
                };
                self.groups[group].update(
                    position,
                    values.is_valid(index).then(|| values.value(index)),
                    self.spec.sum,
                );
            }
        }
        Ok(())
    }
}

impl ScanProcessor for InstantMaxProcessor {
    fn schema(&self) -> SchemaRef {
        self.packed_schema
            .as_ref()
            .unwrap_or(&self.spec.schema)
            .clone()
    }

    fn process(&mut self, batch: &RecordBatch) -> std::result::Result<(), BoxedError> {
        self.consume(batch).map_err(boxed)
    }

    fn series_blocks(&mut self) -> Option<&mut dyn SeriesBlockProcessor> {
        Some(self)
    }

    fn finish(&mut self) -> std::result::Result<Vec<RecordBatch>, BoxedError> {
        self.output().map_err(boxed)
    }
}

impl SeriesBlockProcessor for InstantMaxProcessor {
    fn tag_names(&self) -> Option<Vec<String>> {
        let name = |column: usize| self.spec.input_schema.field(column).name().clone();
        Some(
            std::iter::once(self.spec.tsid)
                .chain(
                    self.spec
                        .groups
                        .iter()
                        .copied()
                        .filter(|&column| column != self.spec.timestamp),
                )
                .map(name)
                .collect(),
        )
    }

    fn process_series(
        &mut self,
        tags: &[(String, datatypes::value::Value)],
        blocks: &mut dyn Iterator<Item = std::result::Result<VisibleSeriesBlock, BoxedError>>,
    ) -> std::result::Result<(), BoxedError> {
        self.consume_blocks(
            tags,
            blocks,
            self.packed_schema.is_some()
                || self.direct_output
                || std::env::var_os("GT_RESEARCH_DIRECT_INSTANT_SEEK").is_some(),
        )
        .map_err(boxed)
    }
}

impl InstantMaxProcessor {
    fn consume_blocks(
        &mut self,
        tags: &[(String, datatypes::value::Value)],
        blocks: &mut dyn Iterator<Item = std::result::Result<VisibleSeriesBlock, BoxedError>>,
        seek: bool,
    ) -> Result<()> {
        use datafusion::arrow::datatypes::DataType;
        use datatypes::data_type::ConcreteDataType;
        use datatypes::value::Value;

        fn scalar(value: &Value, datatype: &DataType) -> Result<ScalarValue> {
            if let DataType::Dictionary(key, inner) = datatype {
                return Ok(ScalarValue::Dictionary(
                    key.clone(),
                    Box::new(scalar(value, inner)?),
                ));
            }
            let datatype = ConcreteDataType::try_from(datatype)
                .map_err(|error| DataFusionError::External(Box::new(error)))?;
            value
                .try_to_scalar_value(&datatype)
                .map_err(|error| DataFusionError::External(Box::new(error)))
        }

        self.check_cancelled()?;
        let tag = |column: usize| -> Result<&Value> {
            let name = self.spec.input_schema.field(column).name();
            tags.iter()
                .find(|(key, _)| key == name)
                .map(|(_, value)| value)
                .ok_or_else(|| invalid("Missing direct series tag"))
        };
        let Value::UInt64(tsid) = tag(self.spec.tsid)? else {
            return Err(invalid("Invalid direct series identifier"));
        };
        let tsid = *tsid;
        let labels = self
            .spec
            .groups
            .iter()
            .filter(|&&column| column != self.spec.timestamp)
            .map(|&column| {
                scalar(
                    tag(column)?,
                    self.spec.input_schema.field(column).data_type(),
                )
            })
            .collect::<Result<Vec<_>>>()?;
        let temporary = self.memory.new_empty();
        temporary.try_grow(labels.iter().map(ScalarValue::size).sum())?;
        let mut labels = Some(labels);
        let mut started = false;
        for block in blocks {
            self.check_cancelled()?;
            let block = block.map_err(|error| DataFusionError::External(Box::new(error)))?;
            if block.values.null_count() != 0
                || block.timestamp_step < 0
                || (block.timestamp_step == 0 && block.values.len() > 1)
            {
                return Err(invalid("Invalid visible series block"));
            }
            if block.values.is_empty() {
                continue;
            }
            if !started {
                self.emit_before(i128::MAX)?;
                self.start_values(
                    labels
                        .take()
                        .ok_or_else(|| invalid("Missing series labels"))?,
                    tsid,
                )?;
                started = true;
            }
            if seek {
                self.consume_regular_block(&block)?;
                continue;
            }
            for (index, value) in block.values.iter_values().enumerate() {
                let raw = block
                    .timestamp_step
                    .checked_mul(index as i128)
                    .and_then(|delta| (block.first_timestamp as i128).checked_add(delta))
                    .and_then(|timestamp| i64::try_from(timestamp).ok())
                    .ok_or_else(|| invalid("Direct series timestamp overflow"))?;
                let timestamp = i128::from(raw) + i128::from(self.spec.offset);
                if self
                    .current
                    .as_ref()
                    .and_then(|cursor| cursor.latest)
                    .is_some_and(|(previous, _)| timestamp < previous)
                {
                    return Err(invalid("Unordered visible samples in scan aggregation"));
                }
                self.emit_before(timestamp)?;
                if let Some(cursor) = &mut self.current {
                    cursor.latest = Some((timestamp, Some(value)));
                }
            }
        }
        Ok(())
    }

    fn consume_regular_block(&mut self, block: &VisibleSeriesBlock) -> Result<()> {
        let first = i128::from(block.first_timestamp) + i128::from(self.spec.offset);
        let last_raw = block
            .timestamp_step
            .checked_mul((block.values.len() - 1) as i128)
            .and_then(|delta| i128::from(block.first_timestamp).checked_add(delta))
            .and_then(|timestamp| i64::try_from(timestamp).ok())
            .ok_or_else(|| invalid("Direct series timestamp overflow"))?;
        let last = i128::from(last_raw) + i128::from(self.spec.offset);
        if self
            .current
            .as_ref()
            .and_then(|cursor| cursor.latest)
            .is_some_and(|(previous, _)| first < previous)
        {
            return Err(invalid("Unordered visible samples in scan aggregation"));
        }
        self.emit_before(first)?;
        loop {
            self.check_cancelled()?;
            let cursor = self
                .current
                .as_ref()
                .ok_or_else(|| invalid("Missing direct series cursor"))?;
            let Some(&instant) = self.spec.grid.instants.get(cursor.next) else {
                break;
            };
            let instant = i128::from(instant);
            // Leave the last timestamp pending: the next block can share its boundary.
            if instant >= last {
                break;
            }
            let index = usize::try_from((instant - first) / block.timestamp_step)
                .map_err(|_| invalid("Direct series index overflow"))?;
            if index >= block.values.len() {
                return Err(invalid("Direct series index outside block"));
            }
            let value = block.values.value(index);
            let timestamp = first + block.timestamp_step * index as i128;
            if let Some(cursor) = &mut self.current {
                cursor.latest = Some((timestamp, Some(value)));
            }
            self.emit_before(instant + 1)?;
        }
        if let Some(cursor) = &mut self.current {
            cursor.latest = Some((last, Some(block.values.value(block.values.len() - 1))));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use datafusion::arrow::array::StringArray;
    use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use datafusion::execution::memory_pool::{MemoryConsumer, UnboundedMemoryPool};

    use super::*;

    fn input(tsid: u64, label: &str, times: Vec<i64>, values: Vec<Option<f64>>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("__tsid", DataType::UInt64, false),
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("value", DataType::Float64, true),
            Field::new("label", DataType::Utf8, false),
        ]));
        let rows = times.len();
        let batch = datafusion::arrow::record_batch::RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt64Array::from(vec![tsid; rows])),
                Arc::new(TimestampMillisecondArray::from(times)),
                Arc::new(Float64Array::from(values)),
                Arc::new(StringArray::from(vec![label; rows])),
            ],
        )
        .unwrap();
        RecordBatch::from_df_record_batch(
            Arc::new(datatypes::schema::Schema::try_from(schema).unwrap()),
            batch,
        )
    }

    #[test]
    fn scan_max_regular_seek_matches_sample_walk() {
        let values = (0..106)
            .map(|index| match index % 13 {
                0 => f64::from_bits(STALE),
                1 => f64::NAN,
                2 => -0.0,
                3 => f64::INFINITY,
                4 => f64::NEG_INFINITY,
                _ => (index % 7) as f64,
            })
            .collect::<Vec<_>>();
        for stride in [1, 7, 30] {
            let times = (0..106)
                .map(|index| -35 + stride * index + if index >= 53 { 79 } else { 0 })
                .collect::<Vec<_>>();
            let batch = input(
                1,
                "a",
                times.clone(),
                values.iter().copied().map(Some).collect(),
            );
            let schema = Arc::new(
                datatypes::schema::Schema::try_from(Arc::new(Schema::new(vec![
                    Field::new("label", DataType::Utf8, false),
                    Field::new(
                        "ts",
                        DataType::Timestamp(TimeUnit::Millisecond, None),
                        false,
                    ),
                    Field::new("max", DataType::Float64, true),
                ])))
                .unwrap(),
            );
            for interval in [1, 7, 30, 61] {
                for offset in [-11, 0, 19] {
                    for lookback in [0, 17, 80] {
                        let spec = Arc::new(InstantMaxSpec {
                            schema: schema.clone(),
                            input_schema: batch.schema.arrow_schema().clone(),
                            groups: vec![3, 1],
                            timestamp: 1,
                            value: 2,
                            tsid: 0,
                            grid: Arc::new(TimeGrid::new(-50, 3400, interval).unwrap()),
                            lookback,
                            offset,
                            sum: false,
                        });
                        let create = || {
                            let pool: Arc<dyn datafusion::execution::memory_pool::MemoryPool> =
                                Arc::new(UnboundedMemoryPool::default());
                            InstantMaxProcessor::new(
                                spec.clone(),
                                MemoryConsumer::new("seek-test").register(&pool),
                                Arc::new(AtomicBool::new(false)),
                            )
                            .unwrap()
                        };
                        let output = |mut processor: InstantMaxProcessor| {
                            processor
                                .finish()
                                .unwrap()
                                .into_iter()
                                .flat_map(|batch| {
                                    let times = batch
                                        .column(1)
                                        .as_any()
                                        .downcast_ref::<TimestampMillisecondArray>()
                                        .unwrap();
                                    let values = batch
                                        .column(2)
                                        .as_any()
                                        .downcast_ref::<Float64Array>()
                                        .unwrap();
                                    times
                                        .values()
                                        .iter()
                                        .copied()
                                        .zip(values.iter().map(|value| value.map(f64::to_bits)))
                                        .collect::<Vec<_>>()
                                })
                                .collect::<Vec<_>>()
                        };
                        let mut expected = create();
                        expected.process(&batch).unwrap();
                        let expected = output(expected);
                        for chunk in [1, 3, 53] {
                            let samples = Float64Array::from(values.clone());
                            let mut blocks = [0, 53]
                                .into_iter()
                                .flat_map(|segment| {
                                    (segment..segment + 53)
                                        .step_by(chunk)
                                        .map(move |start| (start, chunk.min(segment + 53 - start)))
                                })
                                .map(|(start, count)| {
                                    Ok(VisibleSeriesBlock {
                                        first_timestamp: times[start],
                                        timestamp_step: i128::from(stride),
                                        values: samples.slice(start, count).into(),
                                    })
                                });
                            let tags = vec![
                                ("__tsid".into(), datatypes::value::Value::UInt64(1)),
                                ("label".into(), datatypes::value::Value::from("a")),
                            ];
                            let mut actual = create();
                            actual.consume_blocks(&tags, &mut blocks, true).unwrap();
                            assert_eq!(
                                expected,
                                output(actual),
                                "stride={stride}, interval={interval}, offset={offset}, lookback={lookback}, chunk={chunk}"
                            );
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn scan_sum_matches_an_instant_selection_oracle() {
        let series: [(u64, &str, Vec<i64>, Vec<Option<f64>>); 5] = [
            (
                1,
                "a",
                vec![0, 10, 20, 30],
                vec![Some(10.0), None, Some(f64::from_bits(STALE)), Some(-0.0)],
            ),
            (
                2,
                "a",
                vec![0, 10, 20, 30],
                vec![Some(f64::NAN), Some(1.0), Some(2.0), Some(0.5)],
            ),
            (3, "a", vec![5], vec![Some(1.25)]),
            (4, "b", vec![15], vec![Some(-0.0)]),
            (5, "c", vec![0], vec![None]),
        ];
        let (lookback, grid) = (10, TimeGrid::new(0, 30, 10).unwrap());
        // Oracle: each series contributes its latest sample within the lookback, stale
        // markers excluded; a group is present when any series is selected.
        let mut expected = BTreeMap::new();
        for (_, label, times, values) in &series {
            for &instant in &grid.instants {
                let Some(row) = times
                    .iter()
                    .rposition(|&t| t <= instant && t > instant - lookback)
                else {
                    continue;
                };
                if values[row].is_some_and(|v| v.to_bits() == STALE) {
                    continue;
                }
                let slot = expected.entry((label.to_string(), instant)).or_insert(None);
                if let Some(value) = values[row] {
                    *slot = Some(slot.map_or(value, |sum: f64| sum + value));
                }
            }
        }
        let schema = Arc::new(
            datatypes::schema::Schema::try_from(Arc::new(Schema::new(vec![
                Field::new("label", DataType::Utf8, false),
                Field::new(
                    "ts",
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                    false,
                ),
                Field::new("sum", DataType::Float64, true),
            ])))
            .unwrap(),
        );
        for chunk in [1, 3] {
            let inputs = series
                .iter()
                .map(|(tsid, label, times, values)| {
                    input(*tsid, label, times.clone(), values.clone())
                })
                .collect::<Vec<_>>();
            let pool: Arc<dyn datafusion::execution::memory_pool::MemoryPool> =
                Arc::new(UnboundedMemoryPool::default());
            let mut processor = InstantMaxProcessor::new(
                Arc::new(InstantMaxSpec {
                    schema: schema.clone(),
                    input_schema: inputs[0].schema.arrow_schema().clone(),
                    groups: vec![3, 1],
                    timestamp: 1,
                    value: 2,
                    tsid: 0,
                    grid: Arc::new(grid.clone()),
                    lookback,
                    offset: 0,
                    sum: true,
                }),
                MemoryConsumer::new("scan-sum-test").register(&pool),
                Arc::new(AtomicBool::new(false)),
            )
            .unwrap();
            for batch in &inputs {
                for start in (0..batch.num_rows()).step_by(chunk) {
                    processor
                        .process(
                            &batch
                                .slice(start, chunk.min(batch.num_rows() - start))
                                .unwrap(),
                        )
                        .unwrap();
                }
            }
            let mut actual = BTreeMap::new();
            for batch in processor.finish().unwrap() {
                let labels = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                let times = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .unwrap();
                let values = batch
                    .column(2)
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap();
                for row in 0..batch.num_rows() {
                    actual.insert(
                        (labels.value(row).to_string(), times.value(row)),
                        (!values.is_null(row)).then(|| values.value(row)),
                    );
                }
            }
            let bits = |map: &BTreeMap<(String, i64), Option<f64>>| {
                map.iter()
                    .map(|(k, v)| (k.clone(), v.map(f64::to_bits)))
                    .collect::<Vec<_>>()
            };
            assert_eq!(bits(&actual), bits(&expected), "chunk {chunk}");
        }
    }

    #[test]
    fn scan_max_keeps_series_state_and_order_across_batches() {
        let inputs = [
            input(
                1,
                "a",
                vec![0, 10, 20, 30],
                vec![Some(10.0), None, Some(f64::from_bits(STALE)), Some(-0.0)],
            ),
            input(
                2,
                "a",
                vec![0, 10, 20, 30],
                vec![Some(f64::NAN), Some(1.0), Some(2.0), Some(0.0)],
            ),
            input(3, "a", vec![0], vec![Some(1.0)]),
            input(4, "b", vec![15], vec![Some(3.0)]),
            input(5, "c", vec![0], vec![None]),
            input(
                6,
                "d",
                vec![-10, 0, 10, 20],
                vec![
                    Some(f64::from_bits(STALE)),
                    Some(4.0),
                    Some(f64::from_bits(STALE)),
                    Some(5.0),
                ],
            ),
        ];
        let schema = Arc::new(
            datatypes::schema::Schema::try_from(Arc::new(Schema::new(vec![
                Field::new("label", DataType::Utf8, false),
                Field::new(
                    "ts",
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                    false,
                ),
                Field::new("max", DataType::Float64, true),
            ])))
            .unwrap(),
        );
        for offset in [0, 5] {
            for (direct, seek) in [(false, false), (true, false), (true, true)] {
                for chunk in [1, 2, 4] {
                    let pool: Arc<dyn datafusion::execution::memory_pool::MemoryPool> =
                        Arc::new(UnboundedMemoryPool::default());
                    let memory = MemoryConsumer::new("scan-max-test").register(&pool);
                    let mut processor = InstantMaxProcessor::new(
                        Arc::new(InstantMaxSpec {
                            schema: schema.clone(),
                            input_schema: inputs[0].schema.arrow_schema().clone(),
                            groups: vec![3, 1],
                            timestamp: 1,
                            value: 2,
                            tsid: 0,
                            grid: Arc::new(TimeGrid::new(0, 30, 10).unwrap()),
                            lookback: 10,
                            offset,
                            sum: false,
                        }),
                        memory,
                        Arc::new(AtomicBool::new(false)),
                    )
                    .unwrap();
                    let mut series_processor = InstantMaxProcessor::new(
                        processor.spec.clone(),
                        MemoryConsumer::new("series-max-test").register(&pool),
                        Arc::new(AtomicBool::new(false)),
                    )
                    .unwrap();
                    for batch in &inputs {
                        series_processor.process(batch).unwrap();
                        let values = batch
                            .column(2)
                            .as_any()
                            .downcast_ref::<Float64Array>()
                            .unwrap();
                        if direct && values.null_count() == 0 {
                            let times = batch
                                .column(1)
                                .as_any()
                                .downcast_ref::<TimestampMillisecondArray>()
                                .unwrap();
                            let tsid = batch
                                .column(0)
                                .as_any()
                                .downcast_ref::<UInt64Array>()
                                .unwrap()
                                .value(0);
                            let label = batch
                                .column(3)
                                .as_any()
                                .downcast_ref::<StringArray>()
                                .unwrap()
                                .value(0);
                            let tags = vec![
                                ("__tsid".to_string(), datatypes::value::Value::UInt64(tsid)),
                                ("label".to_string(), datatypes::value::Value::from(label)),
                            ];
                            let mut blocks = (0..batch.num_rows()).step_by(chunk).map(|start| {
                                Ok(VisibleSeriesBlock {
                                    first_timestamp: times.value(start),
                                    timestamp_step: 10,
                                    values: values
                                        .slice(start, chunk.min(batch.num_rows() - start))
                                        .into(),
                                })
                            });
                            processor.consume_blocks(&tags, &mut blocks, seek).unwrap();
                            continue;
                        }
                        for start in (0..batch.num_rows()).step_by(chunk) {
                            processor
                                .process(
                                    &batch
                                        .slice(start, chunk.min(batch.num_rows() - start))
                                        .unwrap(),
                                )
                                .unwrap();
                        }
                    }
                    let mut actual = BTreeMap::new();
                    for batch in processor.finish().unwrap() {
                        let labels = batch
                            .column(0)
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .unwrap();
                        let times = batch
                            .column(1)
                            .as_any()
                            .downcast_ref::<TimestampMillisecondArray>()
                            .unwrap();
                        let values = batch
                            .column(2)
                            .as_any()
                            .downcast_ref::<Float64Array>()
                            .unwrap();
                        for row in 0..batch.num_rows() {
                            assert!(
                                actual
                                    .insert(
                                        (labels.value(row).to_string(), times.value(row)),
                                        (!values.is_null(row)).then(|| values.value(row).to_bits())
                                    )
                                    .is_none()
                            );
                        }
                    }
                    let expected = if offset == 0 {
                        vec![
                            ("a", 0, Some(1.0_f64)),
                            ("a", 10, Some(1.0)),
                            ("a", 20, Some(2.0)),
                            ("a", 30, Some(-0.0)),
                            ("b", 20, Some(3.0)),
                            ("c", 0, None),
                            ("d", 0, Some(4.0)),
                            ("d", 20, Some(5.0)),
                        ]
                    } else {
                        vec![
                            ("a", 10, Some(1.0_f64)),
                            ("a", 20, Some(1.0)),
                            ("a", 30, Some(2.0)),
                            ("b", 20, Some(3.0)),
                            ("c", 10, None),
                            ("d", 10, Some(4.0)),
                            ("d", 30, Some(5.0)),
                        ]
                    }
                    .into_iter()
                    .map(|(label, time, value)| {
                        ((label.to_string(), time), value.map(f64::to_bits))
                    })
                    .collect::<BTreeMap<_, _>>();
                    let (series, retained) = series_processor.finish_series().unwrap();
                    let mut direct_values = BTreeMap::new();
                    for series in series {
                        let ScalarValue::Utf8(Some(label)) = &series.labels[0] else {
                            panic!("Missing direct max label");
                        };
                        for (point, value) in series.values.into_iter().enumerate() {
                            if let Some(value) = value {
                                assert!(
                                    direct_values
                                        .insert(
                                            (label.clone(), point as i64 * 10),
                                            Some(value.to_bits()),
                                        )
                                        .is_none()
                                );
                            }
                        }
                    }
                    let present = expected
                        .iter()
                        .filter(|(_, value)| value.is_some())
                        .map(|(key, value)| (key.clone(), *value))
                        .collect::<BTreeMap<_, _>>();
                    assert_eq!(present, direct_values);
                    drop(retained);
                    assert_eq!(
                        expected, actual,
                        "offset={offset}, chunk={chunk}, direct={direct}, seek={seek}"
                    );
                }
            }
        }
    }

    #[tokio::test]
    async fn packed_max_merge_matches_datafusion_partial_state_merge() {
        use datafusion::datasource::MemTable;
        use datafusion::prelude::{SessionConfig, SessionContext};

        let inputs = [
            input(
                1,
                "a",
                vec![0, 10, 20, 30],
                vec![Some(f64::NAN), None, Some(-0.0), Some(f64::INFINITY)],
            ),
            input(
                2,
                "a",
                vec![0, 10, 20, 30],
                vec![Some(3.0), Some(f64::from_bits(STALE)), Some(0.0), Some(1.0)],
            ),
            input(
                3,
                "b",
                vec![0, 10, 30],
                vec![None, Some(f64::NEG_INFINITY), Some(7.0)],
            ),
            input(4, "c", vec![20], vec![Some(5.0)]),
        ];
        let timestamp_type = DataType::Timestamp(TimeUnit::Millisecond, None);
        let schema = Arc::new(
            datatypes::schema::Schema::try_from(Arc::new(Schema::new(vec![
                Field::new("label", DataType::Utf8, false),
                Field::new("ts", timestamp_type.clone(), false),
                Field::new("value", DataType::Float64, true),
            ])))
            .unwrap(),
        );
        let packed_schema = Arc::new(
            datatypes::schema::Schema::try_from(Arc::new(Schema::new(vec![
                Field::new("label", DataType::Utf8, false),
                Field::new(
                    "points",
                    DataType::List(Arc::new(Field::new(
                        "item",
                        DataType::Struct(
                            vec![
                                Field::new("time", timestamp_type, false),
                                Field::new("value", DataType::Float64, true),
                            ]
                            .into(),
                        ),
                        false,
                    ))),
                    false,
                ),
            ])))
            .unwrap(),
        );
        let pool: Arc<dyn datafusion::execution::memory_pool::MemoryPool> =
            Arc::new(UnboundedMemoryPool::default());
        let cancelled = Arc::new(AtomicBool::new(false));
        let spec = Arc::new(InstantMaxSpec {
            schema: schema.clone(),
            input_schema: inputs[0].schema.arrow_schema().clone(),
            groups: vec![3, 1],
            timestamp: 1,
            value: 2,
            tsid: 0,
            grid: Arc::new(TimeGrid::new(0, 40, 10).unwrap()),
            lookback: 10,
            offset: 0,
            sum: false,
        });
        let create = || {
            InstantMaxProcessor::new(
                spec.clone(),
                MemoryConsumer::new("packed-max-test").register(&pool),
                cancelled.clone(),
            )
            .unwrap()
        };
        let mut merge = create().with_packed_schema(packed_schema.clone());
        let mut states = Vec::new();
        for partition in [[0, 2], [1, 3]] {
            let mut ordinary = create();
            let mut packed = create().with_packed_schema(packed_schema.clone());
            for index in partition {
                ordinary.process(&inputs[index]).unwrap();
                packed.process(&inputs[index]).unwrap();
            }
            states.extend(
                ordinary
                    .finish()
                    .unwrap()
                    .into_iter()
                    .map(RecordBatch::into_df_record_batch),
            );
            for batch in packed.finish().unwrap() {
                merge.merge_packed(batch.df_record_batch()).unwrap();
            }
        }
        let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
        ctx.register_table(
            "states",
            Arc::new(MemTable::try_new(schema.arrow_schema().clone(), vec![states]).unwrap()),
        )
        .unwrap();
        let expected = ctx
            .sql("SELECT label, ts, max(value) FROM states GROUP BY label, ts")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let mut reference = BTreeMap::new();
        for batch in expected {
            let labels = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let times = batch
                .column(1)
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap();
            let values = batch
                .column(2)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            for row in 0..batch.num_rows() {
                reference.insert(
                    (labels.value(row).to_owned(), times.value(row)),
                    values.is_valid(row).then(|| values.value(row).to_bits()),
                );
            }
        }
        let mut actual = BTreeMap::new();
        for batch in merge.finish().unwrap() {
            let label = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0);
            let lists = batch
                .column(1)
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap();
            let points = lists.value(0);
            let points = points.as_any().downcast_ref::<StructArray>().unwrap();
            let times = points
                .column(0)
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap();
            let values = points
                .column(1)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            for row in 0..points.len() {
                assert!(
                    actual
                        .insert(
                            (label.to_owned(), times.value(row)),
                            values.is_valid(row).then(|| values.value(row).to_bits())
                        )
                        .is_none()
                );
            }
        }
        assert_eq!(reference, actual);
        cancelled.store(true, Ordering::Relaxed);
        assert!(merge.finish().is_err());
        drop(merge);
        assert_eq!(pool.reserved(), 0);
    }
}
