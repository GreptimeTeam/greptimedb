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
use std::sync::atomic::{AtomicBool, Ordering};

use common_error::ext::BoxedError;
use common_recordbatch::RecordBatch;
use datafusion::arrow::array::{
    Array, ArrayRef, DictionaryArray, Float64Array, ListArray, StructArray,
    TimestampMillisecondArray, UInt32Array, UInt64Array,
};
use datafusion::arrow::buffer::OffsetBuffer;
use datafusion::arrow::datatypes::{DataType, Field, Schema, UInt32Type};
use datafusion::execution::memory_pool::MemoryReservation;
use datafusion::physical_expr::PhysicalExpr;
use datafusion_common::{DataFusionError, Result, ScalarValue};
use datatypes::schema::SchemaRef;
use store_api::region_engine::{ScanProcessor, SeriesBlockProcessor, VisibleSeriesBlock};

use super::scan_range_aggregate::{RangeAggregate, RangeAggregateSpec};
use crate::research_series_grid::{grid::TimeGrid, range};

#[derive(Debug, Clone)]
pub(crate) enum RangeOutput {
    Timestamp,
    Value,
    Tag(usize),
}

#[derive(
    Debug,
    Clone,
    Copy,
    Default,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    serde::Serialize,
    serde::Deserialize,
)]
pub(crate) enum RangeFunction {
    Max,
    #[default]
    Rate,
    Increase,
    Irate,
    Instant,
}

impl RangeFunction {
    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::Max => "max_over_time",
            Self::Rate => "rate",
            Self::Increase => "increase",
            Self::Irate => "irate",
            Self::Instant => "instant",
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct PackedRangeSpec {
    pub batch_output: bool,
    pub schema: SchemaRef,
    pub labels: Vec<usize>,
    pub expression: Arc<dyn PhysicalExpr>,
}

#[derive(Debug, Clone)]
pub(crate) struct RangeSpec {
    pub aggregate: Option<Arc<RangeAggregateSpec>>,
    pub packed: Option<Arc<PackedRangeSpec>>,
    pub function: RangeFunction,
    pub schema: SchemaRef,
    pub input_schema: datafusion::arrow::datatypes::SchemaRef,
    pub output: Vec<RangeOutput>,
    pub timestamp: usize,
    pub value: usize,
    pub tsid: usize,
    pub grid: Arc<TimeGrid>,
    pub window: i64,
    pub offset: i64,
}

struct PendingSeries {
    tsid: u64,
    labels: Vec<ScalarValue>,
    times: Vec<i64>,
    values: Vec<f64>,
    previous: Option<i64>,
    memory: MemoryReservation,
}

struct DensePackedOutput {
    labels: Vec<Vec<ScalarValue>>,
    values: Vec<Option<f64>>,
    offsets: Vec<i32>,
}

impl DensePackedOutput {
    fn new(labels: usize) -> Self {
        Self {
            labels: (0..labels).map(|_| Vec::new()).collect(),
            values: Vec::new(),
            offsets: vec![0],
        }
    }
}

pub(crate) struct SeriesGrid {
    pub labels: Vec<ScalarValue>,
    pub values: Vec<Option<f64>>,
}

pub(crate) struct RangeProcessor {
    spec: Arc<RangeSpec>,
    aggregate: Option<RangeAggregate>,
    grid_timestamps: Option<Arc<TimestampMillisecondArray>>,
    packed_keys: Option<[UInt32Array; 2]>,
    pending: Option<PendingSeries>,
    output: Vec<RecordBatch>,
    series_output: Option<Vec<SeriesGrid>>,
    dense_packed: Option<DensePackedOutput>,
    scratch: MemoryReservation,
    memory: MemoryReservation,
    cancelled: Arc<AtomicBool>,
}

fn invalid(message: &str) -> DataFusionError {
    DataFusionError::Execution(message.into())
}

fn boxed(error: DataFusionError) -> BoxedError {
    BoxedError::new(crate::error::Error::from(error))
}

impl RangeProcessor {
    pub(crate) fn new(
        spec: Arc<RangeSpec>,
        memory: MemoryReservation,
        cancelled: Arc<AtomicBool>,
    ) -> Result<Self> {
        Self::new_inner(spec, memory, cancelled, false)
    }

    pub(crate) fn new_series(
        spec: Arc<RangeSpec>,
        memory: MemoryReservation,
        cancelled: Arc<AtomicBool>,
    ) -> Result<Self> {
        if spec.packed.is_none() {
            return Err(invalid(
                "Series output requires a packed fragment specification",
            ));
        }
        Self::new_inner(spec, memory, cancelled, true)
    }

    fn new_inner(
        spec: Arc<RangeSpec>,
        memory: MemoryReservation,
        cancelled: Arc<AtomicBool>,
        direct_output: bool,
    ) -> Result<Self> {
        if spec.grid.instants.is_empty()
            || (spec.function == RangeFunction::Instant
                && (spec.window < 0 || spec.aggregate.is_some()))
            || (spec.function != RangeFunction::Instant
                && (spec.window <= 0
                    || spec
                        .grid
                        .instants
                        .windows(2)
                        .any(|p| i128::from(p[1]) - i128::from(p[0]) < i128::from(spec.window))))
        {
            return Err(invalid("Direct range requires disjoint positive windows"));
        }
        let grid_timestamps = if !direct_output && spec.packed.is_some() {
            memory.try_grow(
                spec.grid
                    .instants
                    .len()
                    .checked_mul(8)
                    .and_then(|bytes| bytes.checked_add(256))
                    .ok_or_else(|| invalid("Packed grid size overflow"))?,
            )?;
            Some(Arc::new(TimestampMillisecondArray::from(
                spec.grid.instants.clone(),
            )))
        } else {
            None
        };
        let dense_packed = if !direct_output
            && std::env::var_os("GT_RESEARCH_BATCH_PACKED_OUTPUT").is_some()
            && spec.aggregate.is_none()
            && let Some(packed) = &spec.packed
            && packed.batch_output
            && matches!(packed.schema.arrow_schema().field(packed.labels.len()).data_type(),
                DataType::List(item) if item.data_type() == &DataType::Float64)
        {
            memory.try_grow(
                packed
                    .labels
                    .len()
                    .checked_mul(64)
                    .and_then(|bytes| bytes.checked_add(128))
                    .ok_or_else(|| invalid("Packed label size overflow"))?,
            )?;
            Some(DensePackedOutput::new(packed.labels.len()))
        } else {
            None
        };
        Ok(Self {
            dense_packed,
            packed_keys: (!direct_output && spec.packed.is_some())
                .then(|| [UInt32Array::from(vec![0]), UInt32Array::from(vec![None])]),
            grid_timestamps,
            aggregate: spec
                .aggregate
                .as_ref()
                .map(|aggregate| {
                    RangeAggregate::new(aggregate.clone(), spec.grid.clone(), memory.new_empty())
                })
                .transpose()?,
            spec,
            pending: None,
            output: Vec::new(),
            series_output: direct_output.then(Vec::new),
            scratch: memory.new_empty(),
            memory,
            cancelled,
        })
    }

    fn check_cancelled(&self) -> Result<()> {
        if self.cancelled.load(Ordering::Relaxed) {
            return Err(invalid("Scan range cancelled"));
        }
        Ok(())
    }

    fn emit(&mut self, labels: Vec<ScalarValue>, values: Vec<Option<f64>>) -> Result<()> {
        self.check_cancelled()?;
        if let Some(aggregate) = &mut self.aggregate {
            return aggregate.update(&labels, values);
        }
        if self.series_output.is_some() {
            return self.emit_series(labels, values);
        }
        if self.spec.packed.is_some() {
            return self.emit_packed(labels, values);
        }
        let rows = values.iter().filter(|v| v.is_some()).count();
        if rows == 0 {
            return Ok(());
        }
        let bytes = rows
            .checked_mul(64)
            .and_then(|n| n.checked_add(self.spec.output.len().checked_mul(1024)?))
            .and_then(|base| {
                labels.iter().try_fold(base, |sum, value| {
                    // A dictionary repeats keys, not the scalar payload, for each row.
                    let bytes = if matches!(value, ScalarValue::Dictionary(_, _)) {
                        rows.checked_mul(32)?
                            .checked_add(value.size().checked_mul(2)?)?
                    } else {
                        rows.checked_mul(value.size().checked_add(64)?)?
                            .checked_mul(2)?
                    };
                    sum.checked_add(bytes)
                })
            })
            .ok_or_else(|| invalid("Range output size overflow"))?;
        self.memory.try_grow(bytes)?;
        let positions: Vec<_> = values
            .iter()
            .enumerate()
            .filter_map(|(i, v)| v.is_some().then_some(i))
            .collect();
        let mut label = 0;
        let mut arrays = Vec::with_capacity(self.spec.output.len());
        for output in &self.spec.output {
            let array: datafusion::arrow::array::ArrayRef = match output {
                RangeOutput::Timestamp => Arc::new(TimestampMillisecondArray::from_iter_values(
                    positions.iter().map(|&i| self.spec.grid.instants[i]),
                )),
                RangeOutput::Value => Arc::new(Float64Array::from_iter(
                    positions.iter().map(|&i| values[i]),
                )),
                RangeOutput::Tag(_) => {
                    let value = labels
                        .get(label)
                        .ok_or_else(|| invalid("Missing range output label"))?;
                    label += 1;
                    value.to_array_of_size(rows)?
                }
            };
            arrays.push(array);
        }
        let batch = datafusion::arrow::record_batch::RecordBatch::try_new(
            self.spec.schema.arrow_schema().clone(),
            arrays,
        )?;
        self.output.push(RecordBatch::from_df_record_batch(
            self.spec.schema.clone(),
            batch,
        ));
        Ok(())
    }

    fn emit_packed(&mut self, labels: Vec<ScalarValue>, values: Vec<Option<f64>>) -> Result<()> {
        let spec = self
            .spec
            .packed
            .as_ref()
            .ok_or_else(|| invalid("Missing packed range specification"))?;
        let present = values.iter().filter(|value| value.is_some()).count();
        if present == 0 {
            return Ok(());
        }
        let schema = spec.schema.arrow_schema();
        let DataType::List(item) = schema.field(spec.labels.len()).data_type() else {
            return Err(invalid("Packed output requires a list"));
        };
        let dense = item.data_type() == &DataType::Float64;
        let count = if dense { values.len() } else { present };
        let end = i32::try_from(count).map_err(|_| invalid("Packed series exceeds list limit"))?;
        let bytes = count
            .checked_mul(64)
            .and_then(|n| n.checked_add(1024))
            .and_then(|n| {
                labels
                    .iter()
                    .try_fold(n, |n, label| n.checked_add(label.size().checked_mul(4)?))
            })
            .ok_or_else(|| invalid("Packed range size overflow"))?;
        self.memory.try_grow(bytes)?;
        if let Some(output) = &mut self.dense_packed {
            let end = output
                .values
                .len()
                .checked_add(values.len())
                .and_then(|count| i32::try_from(count).ok())
                .ok_or_else(|| invalid("Packed batch exceeds list limit"))?;
            for (column, &index) in output.labels.iter_mut().zip(&spec.labels) {
                column.push(
                    labels
                        .get(index)
                        .ok_or_else(|| invalid("Missing packed output label"))?
                        .clone(),
                );
            }
            output.values.extend(values);
            output.offsets.push(end);
            return Ok(());
        }
        let points: ArrayRef = if dense {
            let input = datafusion::arrow::record_batch::RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new(
                    "rate",
                    DataType::Float64,
                    true,
                )])),
                vec![Arc::new(Float64Array::from(values))],
            )?;
            spec.expression.evaluate(&input)?.into_array(count)?
        } else {
            let DataType::Struct(fields) = item.data_type() else {
                return Err(invalid(
                    "Packed output requires paired points or a dense grid",
                ));
            };
            let times: ArrayRef = if count == values.len() {
                self.grid_timestamps
                    .clone()
                    .ok_or_else(|| invalid("Missing packed timestamp grid"))?
            } else {
                Arc::new(TimestampMillisecondArray::from_iter_values(
                    values.iter().enumerate().filter_map(|(index, value)| {
                        value.is_some().then_some(self.spec.grid.instants[index])
                    }),
                ))
            };
            let input = datafusion::arrow::record_batch::RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new(
                    "rate",
                    DataType::Float64,
                    false,
                )])),
                vec![Arc::new(Float64Array::from_iter_values(
                    values.into_iter().flatten(),
                ))],
            )?;
            let values = spec.expression.evaluate(&input)?.into_array(count)?;
            Arc::new(StructArray::try_new(
                fields.clone(),
                vec![times, values],
                None,
            )?)
        };
        let packed = ListArray::try_new(
            item.clone(),
            OffsetBuffer::new(vec![0, end].into()),
            points,
            None,
        )?;
        let mut columns = spec
            .labels
            .iter()
            .map(|&index| {
                let value = labels
                    .get(index)
                    .ok_or_else(|| invalid("Missing packed output label"))?;
                if let ScalarValue::Dictionary(key_type, value) = value
                    && key_type.as_ref() == &DataType::UInt32
                {
                    // ScalarValue's dictionary conversion locks a process-wide cache.
                    // Keep these tiny, immutable key buffers local to each processor.
                    let keys = self
                        .packed_keys
                        .as_ref()
                        .ok_or_else(|| invalid("Missing packed dictionary keys"))?;
                    return Ok(Arc::new(DictionaryArray::<UInt32Type>::try_new(
                        keys[usize::from(value.is_null())].clone(),
                        value.to_array_of_size(1)?,
                    )?) as ArrayRef);
                }
                value.to_array_of_size(1)
            })
            .collect::<Result<Vec<_>>>()?;
        columns.push(Arc::new(packed));
        let batch = datafusion::arrow::record_batch::RecordBatch::try_new(schema.clone(), columns)?;
        self.output.push(RecordBatch::from_df_record_batch(
            spec.schema.clone(),
            batch,
        ));
        Ok(())
    }

    fn emit_series(&mut self, labels: Vec<ScalarValue>, values: Vec<Option<f64>>) -> Result<()> {
        if values.iter().all(Option::is_none) {
            return Ok(());
        }
        let spec = self
            .spec
            .packed
            .as_ref()
            .ok_or_else(|| invalid("Missing series output specification"))?;
        let bytes = labels
            .iter()
            .try_fold(
                values
                    .len()
                    .checked_mul(64)
                    .ok_or_else(|| invalid("Series output size overflow"))?,
                |size, label| size.checked_add(label.size().checked_mul(4)?),
            )
            .and_then(|size| size.checked_add(256))
            .ok_or_else(|| invalid("Series output size overflow"))?;
        self.memory.try_grow(bytes)?;
        let labels = spec
            .labels
            .iter()
            .map(|&index| {
                labels
                    .get(index)
                    .cloned()
                    .ok_or_else(|| invalid("Missing series output label"))
            })
            .collect::<Result<Vec<_>>>()?;
        let input = datafusion::arrow::record_batch::RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "rate",
                DataType::Float64,
                true,
            )])),
            vec![Arc::new(Float64Array::from(values))],
        )?;
        let array = spec
            .expression
            .evaluate(&input)?
            .into_array(input.num_rows())?;
        let values = array
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| invalid("Series expression requires Float64 output"))?
            .iter()
            .collect();
        self.series_output
            .as_mut()
            .ok_or_else(|| invalid("Series output is disabled"))?
            .push(SeriesGrid { labels, values });
        Ok(())
    }

    pub(crate) fn finish_series(mut self) -> Result<(Vec<SeriesGrid>, MemoryReservation)> {
        self.check_cancelled()?;
        if self.series_output.is_none() {
            return Err(invalid("Series output is disabled"));
        }
        self.flush_pending()?;
        let output = if let Some(aggregate) = &mut self.aggregate {
            let output = aggregate.finish_series()?;
            let bytes = output
                .iter()
                .try_fold(0usize, |size, series| {
                    let size = size
                        .checked_add(series.values.capacity().checked_mul(16)?)?
                        .checked_add(256)?;
                    series.labels.iter().try_fold(size, |size, value| {
                        size.checked_add(value.size().checked_mul(2)?)
                    })
                })
                .ok_or_else(|| invalid("Series aggregate output size overflow"))?;
            // Transfer retained output accounting before dropping the aggregate.
            self.memory.try_grow(bytes)?;
            output
        } else {
            self.series_output
                .take()
                .ok_or_else(|| invalid("Series output is disabled"))?
        };
        Ok((output, self.memory))
    }

    fn finish_dense_packed(&mut self) -> Result<Option<Vec<RecordBatch>>> {
        let Some(output) = self.dense_packed.take() else {
            return Ok(None);
        };
        if output.offsets.len() == 1 {
            return Ok(Some(Vec::new()));
        }
        let spec = self
            .spec
            .packed
            .as_ref()
            .ok_or_else(|| invalid("Missing packed range specification"))?;
        let schema = spec.schema.arrow_schema();
        let DataType::List(item) = schema.field(spec.labels.len()).data_type() else {
            return Err(invalid("Packed output requires a list"));
        };
        let count = output.values.len();
        let input = datafusion::arrow::record_batch::RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "rate",
                DataType::Float64,
                true,
            )])),
            vec![Arc::new(Float64Array::from(output.values))],
        )?;
        // Fragment formulas contain only pointwise Float64 arithmetic, so their
        // evaluation may span series without changing null or NaN semantics.
        let values = spec.expression.evaluate(&input)?.into_array(count)?;
        let packed = ListArray::try_new(
            item.clone(),
            OffsetBuffer::new(output.offsets.into()),
            values,
            None,
        )?;
        let mut columns = output
            .labels
            .into_iter()
            .map(ScalarValue::iter_to_array)
            .collect::<Result<Vec<_>>>()?;
        columns.push(Arc::new(packed));
        let batch = datafusion::arrow::record_batch::RecordBatch::try_new(schema.clone(), columns)?;
        common_telemetry::debug!(
            series = batch.num_rows(),
            "Research dense packed batch emitted"
        );
        Ok(Some(vec![RecordBatch::from_df_record_batch(
            spec.schema.clone(),
            batch,
        )]))
    }

    fn flush_pending(&mut self) -> Result<()> {
        let Some(pending) = self.pending.take() else {
            return Ok(());
        };
        let scratch = self.memory.new_empty();
        scratch.try_grow(
            pending
                .times
                .len()
                .checked_mul(64)
                .and_then(|n| n.checked_add(self.spec.grid.instants.len().checked_mul(128)?))
                .ok_or_else(|| invalid("Range scratch size overflow"))?,
        )?;
        let values = range::evaluate(
            self.spec.function.name(),
            &self.spec.grid,
            pending.times,
            pending.values,
            self.spec.window,
            self.spec.offset,
        )?;
        self.emit(pending.labels, values)
    }

    fn consume(&mut self, batch: &RecordBatch) -> Result<()> {
        self.check_cancelled()?;
        let times = batch
            .column(self.spec.timestamp)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .ok_or_else(|| invalid("Range scan requires millisecond timestamps"))?;
        let values = batch
            .column(self.spec.value)
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| invalid("Range scan requires Float64 values"))?;
        let tsids = batch
            .column(self.spec.tsid)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| invalid("Range scan requires UInt64 series identifiers"))?;
        if times.null_count() != 0 || tsids.null_count() != 0 {
            return Err(invalid("Null range scan identity"));
        }
        for row in 0..batch.num_rows() {
            if row % 1024 == 0 {
                self.check_cancelled()?;
            }
            if self
                .pending
                .as_ref()
                .is_none_or(|p| p.tsid != tsids.value(row))
            {
                self.flush_pending()?;
                let memory = self.memory.new_empty();
                let bytes = self
                    .spec
                    .output
                    .iter()
                    .try_fold(256usize, |sum, output| {
                        sum.checked_add(match output {
                            RangeOutput::Tag(i) => batch.column(*i).get_array_memory_size(),
                            _ => 0,
                        })
                    })
                    .ok_or_else(|| invalid("Range label size overflow"))?;
                memory.try_grow(bytes)?;
                let labels = self
                    .spec
                    .output
                    .iter()
                    .filter_map(|output| match output {
                        RangeOutput::Tag(i) => {
                            Some(ScalarValue::try_from_array(batch.column(*i), row))
                        }
                        _ => None,
                    })
                    .collect::<Result<Vec<_>>>()?;
                self.pending = Some(PendingSeries {
                    tsid: tsids.value(row),
                    labels,
                    times: Vec::new(),
                    values: Vec::new(),
                    previous: None,
                    memory,
                });
            }
            let pending = self
                .pending
                .as_mut()
                .ok_or_else(|| invalid("Missing range series"))?;
            let timestamp = times.value(row);
            if pending
                .previous
                .is_some_and(|previous| timestamp < previous)
            {
                return Err(invalid("Unordered range samples"));
            }
            pending.previous = Some(timestamp);
            if values.is_valid(row) || self.spec.function == RangeFunction::Instant {
                pending.memory.try_grow(64)?;
                pending.times.push(timestamp);
                // Packed selector consumers omit null values, but a latest null
                // must still suppress older samples at subsequent instants.
                pending.values.push(if values.is_valid(row) {
                    values.value(row)
                } else {
                    f64::from_bits(0x7ff0_0000_0000_0002)
                });
            }
        }
        Ok(())
    }
}

impl ScanProcessor for RangeProcessor {
    fn schema(&self) -> SchemaRef {
        if let Some(packed) = &self.spec.packed {
            return packed.schema.clone();
        }
        self.spec.aggregate.as_ref().map_or_else(
            || self.spec.schema.clone(),
            |aggregate| aggregate.schema.clone(),
        )
    }
    fn process(&mut self, batch: &RecordBatch) -> std::result::Result<(), BoxedError> {
        self.consume(batch).map_err(boxed)
    }
    fn series_blocks(&mut self) -> Option<&mut dyn SeriesBlockProcessor> {
        Some(self)
    }
    fn finish(&mut self) -> std::result::Result<Vec<RecordBatch>, BoxedError> {
        if self.series_output.is_some() {
            return Err(boxed(invalid("Series output must be finished as series")));
        }
        self.check_cancelled().map_err(boxed)?;
        self.flush_pending().map_err(boxed)?;
        if let Some(aggregate) = &mut self.aggregate {
            return if let Some(packed) = &self.spec.packed {
                aggregate.finish_grid(packed.schema.clone()).map_err(boxed)
            } else {
                aggregate.finish().map_err(boxed)
            };
        }
        if let Some(output) = self.finish_dense_packed().map_err(boxed)? {
            return Ok(output);
        }
        if self
            .spec
            .packed
            .as_ref()
            .is_some_and(|packed| packed.batch_output)
            && self.output.len() > 1
        {
            let bytes = self
                .output
                .iter()
                .try_fold(0usize, |bytes, batch| {
                    bytes.checked_add(batch.df_record_batch().get_array_memory_size())
                })
                .and_then(|bytes| bytes.checked_mul(2))
                .ok_or_else(|| boxed(invalid("Packed batch size overflow")))?;
            self.memory
                .try_grow(bytes)
                .map_err(|error| boxed(error.into()))?;
            let schema = self.schema();
            let batch = datafusion::arrow::compute::concat_batches(
                schema.arrow_schema(),
                self.output.iter().map(RecordBatch::df_record_batch),
            )
            .map_err(|error| boxed(error.into()))?;
            self.output.clear();
            return Ok(vec![RecordBatch::from_df_record_batch(schema, batch)]);
        }
        Ok(std::mem::take(&mut self.output))
    }
}

impl SeriesBlockProcessor for RangeProcessor {
    fn tag_names(&self) -> Option<Vec<String>> {
        Some(
            self.spec
                .output
                .iter()
                .filter_map(|output| match output {
                    RangeOutput::Tag(index) => {
                        Some(self.spec.input_schema.field(*index).name().clone())
                    }
                    _ => None,
                })
                .collect(),
        )
    }

    fn range_max_request(&self) -> Option<store_api::region_engine::RangeMaxRequest> {
        (self.spec.function == RangeFunction::Max && self.spec.window == 300_000).then(|| {
            store_api::region_engine::RangeMaxRequest {
                instants: self.spec.grid.instants.clone(),
                window: self.spec.window,
                offset: self.spec.offset,
            }
        })
    }

    fn process_series(
        &mut self,
        tags: &[(String, datatypes::value::Value)],
        blocks: &mut dyn Iterator<Item = std::result::Result<VisibleSeriesBlock, BoxedError>>,
    ) -> std::result::Result<(), BoxedError> {
        (|| -> Result<()> {
            self.check_cancelled()?;
            self.flush_pending()?;
            let label_inputs = self.spec.output.iter().filter_map(|output| {
                let RangeOutput::Tag(index) = output else {
                    return None;
                };
                let field = self.spec.input_schema.field(*index);
                Some(
                    tags.iter()
                        .find(|(name, _)| name == field.name())
                        .map(|(_, value)| (field, value))
                        .ok_or_else(|| invalid("Missing direct range label")),
                )
            });
            let grid_bytes = self
                .spec
                .grid
                .instants
                .len()
                .checked_mul(32)
                .ok_or_else(|| invalid("Range scratch size overflow"))?;
            let scratch_bytes = label_inputs.clone().try_fold(grid_bytes, |bytes, label| {
                let (_, value) = label?;
                value
                    .as_value_ref()
                    .data_size()
                    .checked_add(256)
                    .and_then(|n| n.checked_mul(2))
                    .and_then(|n| bytes.checked_add(n))
                    .ok_or_else(|| invalid("Range label size overflow"))
            })?;
            // Reserve every label's conversion scratch before allocating any scalars.
            // Per-label reservations contend on the pool's shared consumer registry.
            if scratch_bytes > self.scratch.size() {
                self.scratch.try_grow(scratch_bytes - self.scratch.size())?;
            }
            let mut labels = Vec::new();
            let mut scalar_bytes = 0usize;
            for label in label_inputs {
                let (field, value) = label?;
                let (datatype, dictionary) = match field.data_type() {
                    datafusion::arrow::datatypes::DataType::Dictionary(key, inner) => {
                        (inner.as_ref(), Some(key.clone()))
                    }
                    datatype => (datatype, None),
                };
                let datatype = datatypes::data_type::ConcreteDataType::try_from(datatype)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
                let scalar = value
                    .try_to_scalar_value(&datatype)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
                scalar_bytes = scalar
                    .size()
                    .checked_mul(2)
                    .and_then(|n| scalar_bytes.checked_add(n))
                    .ok_or_else(|| invalid("Range label size overflow"))?;
                labels.push(match dictionary {
                    Some(key) => ScalarValue::Dictionary(key, Box::new(scalar)),
                    None => scalar,
                });
            }
            let scratch_bytes = scratch_bytes
                .checked_add(scalar_bytes)
                .ok_or_else(|| invalid("Range scratch size overflow"))?;
            if scratch_bytes > self.scratch.size() {
                self.scratch.try_grow(scratch_bytes - self.scratch.size())?;
            }
            let reset_memory = self.memory.new_empty();
            let values = match self.spec.function {
                function @ (RangeFunction::Max | RangeFunction::Instant) => {
                    range::evaluate_selection_blocks(
                        function,
                        &self.spec.grid,
                        self.spec.window,
                        self.spec.offset,
                        blocks,
                        &self.cancelled,
                    )?
                }
                function => range::evaluate_counter_blocks(
                    function,
                    &self.spec.grid,
                    self.spec.window,
                    self.spec.offset,
                    blocks,
                    &self.cancelled,
                    &reset_memory,
                )?,
            };
            self.emit(labels, values)
        })()
        .map_err(boxed)
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::StringArray;
    use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryConsumer, MemoryPool};
    use datatypes::value::Value;

    use super::*;

    fn input(tsid: u64, label: &str, times: Vec<i64>, values: Vec<Option<f64>>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("id", DataType::UInt64, false),
            Field::new("v", DataType::Float64, true),
            Field::new("label", DataType::Utf8, true),
        ]));
        let rows = times.len();
        let batch = datafusion::arrow::record_batch::RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(TimestampMillisecondArray::from(times)),
                Arc::new(UInt64Array::from(vec![tsid; rows])),
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

    fn create(batch: &RecordBatch, pool: &Arc<GreedyMemoryPool>) -> RangeProcessor {
        let schema = Arc::new(Schema::new(vec![
            Field::new("label", DataType::Utf8, true),
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("max", DataType::Float64, true),
        ]));
        let memory_pool: Arc<dyn MemoryPool> = pool.clone();
        RangeProcessor::new(
            Arc::new(RangeSpec {
                aggregate: None,
                packed: None,
                function: RangeFunction::Max,
                schema: Arc::new(datatypes::schema::Schema::try_from(schema).unwrap()),
                input_schema: batch.schema.arrow_schema().clone(),
                output: vec![
                    RangeOutput::Tag(3),
                    RangeOutput::Timestamp,
                    RangeOutput::Value,
                ],
                timestamp: 0,
                tsid: 1,
                value: 2,
                grid: Arc::new(TimeGrid::new(10, 30, 10).unwrap()),
                window: 10,
                offset: 0,
            }),
            MemoryConsumer::new("range-test").register(&memory_pool),
            Arc::new(AtomicBool::new(false)),
        )
        .unwrap()
    }

    #[test]
    fn packed_scan_keeps_sparse_points_and_shares_dense_timestamps() {
        use datafusion::physical_expr::expressions::Column;

        for batch_output in [false, true] {
            let source = input(
                1,
                "a",
                vec![1, 11, 21],
                vec![Some(5.0), Some(6.0), Some(9.0)],
            );
            let pool = Arc::new(GreedyMemoryPool::new(1 << 20));
            let mut spec = create(&source, &pool).spec.as_ref().clone();
            let point = Field::new(
                "item",
                DataType::Struct(
                    vec![
                        Field::new(
                            "time",
                            DataType::Timestamp(TimeUnit::Millisecond, None),
                            false,
                        ),
                        Field::new("value", DataType::Float64, false),
                    ]
                    .into(),
                ),
                false,
            );
            let schema = Arc::new(Schema::new(vec![
                Field::new("label", DataType::Utf8, true),
                Field::new("points", DataType::List(Arc::new(point)), false),
            ]));
            spec.packed = Some(Arc::new(PackedRangeSpec {
                batch_output,
                schema: Arc::new(datatypes::schema::Schema::try_from(schema).unwrap()),
                labels: vec![0],
                expression: Arc::new(Column::new("rate", 0)),
            }));
            let memory_pool: Arc<dyn MemoryPool> = pool.clone();
            let mut processor = RangeProcessor::new(
                Arc::new(spec),
                MemoryConsumer::new("packed-test").register(&memory_pool),
                Arc::new(AtomicBool::new(false)),
            )
            .unwrap();
            processor.process(&source).unwrap();
            processor
                .process(&input(
                    2,
                    "b",
                    vec![1, 11, 21],
                    vec![Some(2.0), Some(3.0), Some(4.0)],
                ))
                .unwrap();
            processor
                .process(&input(3, "c", vec![1, 21], vec![Some(7.0), Some(8.0)]))
                .unwrap();
            let batches = processor.finish().unwrap();
            assert_eq!(batches.len(), if batch_output { 1 } else { 3 });
            let mut pointers = Vec::new();
            for (index, (batch, row)) in batches
                .iter()
                .flat_map(|batch| (0..batch.num_rows()).map(move |row| (batch, row)))
                .enumerate()
            {
                let list = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<ListArray>()
                    .unwrap();
                let points = list.value(row);
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
                assert_eq!(
                    batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<datafusion::arrow::array::StringArray>()
                        .unwrap()
                        .value(row),
                    ["a", "b", "c"][index]
                );
                assert_eq!(
                    values.values().as_ref(),
                    [&[5.0, 6.0, 9.0][..], &[2.0, 3.0, 4.0][..], &[7.0, 8.0][..]][index]
                );
                if index < 2 {
                    assert_eq!(times.values().as_ref(), &[10, 20, 30]);
                    pointers.push(times.values().as_ptr());
                } else {
                    assert_eq!(times.values().as_ref(), &[10, 30]);
                    assert_eq!(values.values().as_ref(), &[7.0, 8.0]);
                }
            }
            if !batch_output {
                assert_eq!(pointers[0], pointers[1]);
            }
            drop(batches);
            drop(processor);
            assert_eq!(pool.reserved(), 0);
        }
    }

    #[test]
    fn counter_processor_matches_fallback_with_nulls_and_series_boundaries() {
        let times = vec![1, 3, 5, 9, 11, 14, 18];
        let values = vec![
            Some(20.0),
            Some(2.0),
            None,
            Some(8.0),
            Some(100.0),
            Some(1.0),
            Some(5.0),
        ];
        let batch = input(1, "a", times.clone(), values.clone());
        for function in [
            RangeFunction::Rate,
            RangeFunction::Increase,
            RangeFunction::Irate,
        ] {
            for direct in [false, true] {
                let pool = Arc::new(GreedyMemoryPool::new(1 << 20));
                let mut processor = create(&batch, &pool);
                Arc::get_mut(&mut processor.spec).unwrap().function = function;
                assert!(processor.range_max_request().is_none());
                if direct {
                    let mut blocks = times.iter().zip(&values).filter_map(|(&t, v)| {
                        v.map(|v| {
                            Ok(VisibleSeriesBlock {
                                first_timestamp: t,
                                timestamp_step: 0,
                                values: Float64Array::from(vec![v]).into(),
                            })
                        })
                    });
                    processor
                        .process_series(&[("label".into(), Value::from("a"))], &mut blocks)
                        .unwrap();
                } else {
                    for (&t, &v) in times.iter().zip(&values) {
                        processor.process(&input(1, "a", vec![t], vec![v])).unwrap();
                    }
                }
                processor
                    .process(&input(2, "b", vec![2, 8], vec![Some(1.0), Some(7.0)]))
                    .unwrap();
                let output = processor.finish().unwrap();
                assert_eq!(output.len(), 2);
                let (ts, vs): (Vec<_>, Vec<_>) = times
                    .iter()
                    .zip(&values)
                    .filter_map(|(&t, v)| v.map(|v| (t, v)))
                    .unzip();
                let expected =
                    range::evaluate(function.name(), &processor.spec.grid, ts, vs, 10, 0).unwrap();
                let actual = output[0]
                    .column(2)
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap();
                assert_eq!(
                    actual
                        .values()
                        .iter()
                        .map(|v| v.to_bits())
                        .collect::<Vec<_>>(),
                    expected
                        .into_iter()
                        .flatten()
                        .map(f64::to_bits)
                        .collect::<Vec<_>>()
                );
                assert_eq!(
                    ScalarValue::try_from_array(output[1].column(0), 0).unwrap(),
                    ScalarValue::Utf8(Some("b".into()))
                );
                drop(output);
                drop(processor);
                assert_eq!(pool.reserved(), 0);
            }
        }
    }

    #[test]
    fn dense_packed_batch_preserves_labels_nulls_and_pointwise_values() {
        use datafusion::physical_expr::expressions::{BinaryExpr, Column, Literal};
        use datafusion_expr::Operator;

        for scaled in [false, true] {
            for batched in [false, true] {
                let first = input(1, "a", vec![5], vec![Some(7.0)]);
                let pool = Arc::new(GreedyMemoryPool::new(256 * 1024));
                let mut spec = create(&first, &pool).spec.as_ref().clone();
                let schema = Arc::new(Schema::new(vec![
                    Field::new(
                        "label",
                        DataType::Dictionary(Box::new(DataType::UInt32), Box::new(DataType::Utf8)),
                        true,
                    ),
                    Field::new(
                        "points",
                        DataType::List(Arc::new(Field::new("item", DataType::Float64, true))),
                        false,
                    ),
                ]));
                let column = Arc::new(Column::new("rate", 0));
                let expression: Arc<dyn PhysicalExpr> = if scaled {
                    Arc::new(BinaryExpr::new(
                        column,
                        Operator::Multiply,
                        Arc::new(Literal::new(ScalarValue::Float64(Some(2.0)))),
                    ))
                } else {
                    column
                };
                spec.packed = Some(Arc::new(PackedRangeSpec {
                    batch_output: true,
                    schema: Arc::new(datatypes::schema::Schema::try_from(schema).unwrap()),
                    labels: vec![0],
                    expression,
                }));
                let memory_pool: Arc<dyn MemoryPool> = pool.clone();
                let mut direct = RangeProcessor::new_series(
                    Arc::new(spec.clone()),
                    MemoryConsumer::new("direct-series-test").register(&memory_pool),
                    Arc::new(AtomicBool::new(false)),
                )
                .unwrap();
                let mut processor = RangeProcessor::new(
                    Arc::new(spec),
                    MemoryConsumer::new("dense-packed-test").register(&memory_pool),
                    Arc::new(AtomicBool::new(false)),
                )
                .unwrap();
                processor.dense_packed = batched.then(|| DensePackedOutput::new(1));
                let labels = [Some("a"), None, Some("a"), Some("")];
                let values = [
                    vec![Some(1.0), None, Some(-0.0)],
                    vec![None, Some(2.0), None],
                    vec![Some(f64::NAN), Some(f64::INFINITY), Some(4.0)],
                    vec![Some(3.0), Some(2.0), Some(1.0)],
                ];
                for (label, values) in labels.iter().zip(&values) {
                    direct
                        .emit(
                            vec![ScalarValue::Dictionary(
                                Box::new(DataType::UInt32),
                                Box::new(ScalarValue::Utf8(label.map(str::to_owned))),
                            )],
                            values.clone(),
                        )
                        .unwrap();
                    processor
                        .emit(
                            vec![ScalarValue::Dictionary(
                                Box::new(DataType::UInt32),
                                Box::new(ScalarValue::Utf8(label.map(str::to_owned))),
                            )],
                            values.clone(),
                        )
                        .unwrap();
                }
                processor
                    .emit(
                        vec![ScalarValue::Dictionary(
                            Box::new(DataType::UInt32),
                            Box::new(ScalarValue::Utf8(Some("absent".into()))),
                        )],
                        vec![None; 3],
                    )
                    .unwrap();
                direct
                    .emit(
                        vec![ScalarValue::Utf8(Some("absent".into()))],
                        vec![None; 3],
                    )
                    .unwrap();
                let (series, retained) = direct.finish_series().unwrap();
                assert_eq!(series.len(), 4);
                for (row, series) in series.iter().enumerate() {
                    assert_eq!(
                        series.labels,
                        vec![ScalarValue::Dictionary(
                            Box::new(DataType::UInt32),
                            Box::new(ScalarValue::Utf8(labels[row].map(str::to_owned))),
                        )]
                    );
                    for (actual, expected) in series.values.iter().zip(&values[row]) {
                        let expected =
                            expected.map(|value| if scaled { value * 2.0 } else { value });
                        match (*actual, expected) {
                            (Some(actual), Some(expected)) if expected.is_nan() => {
                                assert!(actual.is_nan())
                            }
                            (Some(actual), Some(expected)) => {
                                assert_eq!(actual.to_bits(), expected.to_bits())
                            }
                            (None, None) => {}
                            pair => panic!("Direct series validity changed: {pair:?}"),
                        }
                    }
                }
                let batches = processor.finish().unwrap();
                assert_eq!(batches.len(), 1);
                let batch = &batches[0];
                assert_eq!(batch.num_rows(), 4);
                let lists = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<ListArray>()
                    .unwrap();
                for row in 0..4 {
                    assert_eq!(
                        ScalarValue::try_from_array(batch.column(0), row).unwrap(),
                        ScalarValue::Dictionary(
                            Box::new(DataType::UInt32),
                            Box::new(ScalarValue::Utf8(labels[row].map(str::to_owned)))
                        )
                    );
                    let array = lists.value(row);
                    let actual = array.as_any().downcast_ref::<Float64Array>().unwrap();
                    assert_eq!(actual.len(), 3);
                    for (actual, expected) in actual.iter().zip(&values[row]) {
                        let expected = expected.map(|v| if scaled { v * 2.0 } else { v });
                        match (actual, expected) {
                            (Some(a), Some(b)) if b.is_nan() => assert!(a.is_nan()),
                            (Some(a), Some(b)) => assert_eq!(a.to_bits(), b.to_bits()),
                            (None, None) => {}
                            pair => panic!("Point validity changed: {pair:?}"),
                        }
                    }
                }
                assert!(processor.finish().unwrap().is_empty());
                drop(batches);
                drop(processor);
                assert!(pool.reserved() > 0);
                drop(series);
                drop(retained);
                assert_eq!(pool.reserved(), 0);
            }
        }
    }

    #[test]
    fn packed_dictionary_labels_preserve_nulls_and_reuse_keys() {
        use datafusion::physical_expr::expressions::Column;

        let first = input(1, "a", vec![5], vec![Some(7.0)]);
        let pool = Arc::new(GreedyMemoryPool::new(256 * 1024));
        let mut spec = create(&first, &pool).spec.as_ref().clone();
        let dictionary = DataType::Dictionary(Box::new(DataType::UInt32), Box::new(DataType::Utf8));
        let points = DataType::Struct(
            vec![
                Field::new(
                    "time",
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                    false,
                ),
                Field::new("value", DataType::Float64, false),
            ]
            .into(),
        );
        let schema = Arc::new(Schema::new(vec![
            Field::new("label", dictionary, true),
            Field::new(
                "points",
                DataType::List(Arc::new(Field::new("item", points, false))),
                false,
            ),
        ]));
        spec.packed = Some(Arc::new(PackedRangeSpec {
            batch_output: false,
            schema: Arc::new(datatypes::schema::Schema::try_from(schema).unwrap()),
            labels: vec![0],
            expression: Arc::new(Column::new("value", 0)),
        }));
        let memory_pool: Arc<dyn MemoryPool> = pool.clone();
        let mut processor = RangeProcessor::new(
            Arc::new(spec),
            MemoryConsumer::new("packed-label-test").register(&memory_pool),
            Arc::new(AtomicBool::new(false)),
        )
        .unwrap();
        let labels = [Some("a"), None, Some(""), Some("b")];
        for label in labels {
            processor
                .emit(
                    vec![ScalarValue::Dictionary(
                        Box::new(DataType::UInt32),
                        Box::new(ScalarValue::Utf8(label.map(str::to_owned))),
                    )],
                    vec![Some(7.0); 3],
                )
                .unwrap();
        }
        let batches = processor.finish().unwrap();
        let mut keys = Vec::new();
        for (batch, label) in batches.iter().zip(labels) {
            let array = batch
                .column(0)
                .as_any()
                .downcast_ref::<DictionaryArray<UInt32Type>>()
                .unwrap();
            assert_eq!(array.null_count(), usize::from(label.is_none()));
            assert_eq!(
                ScalarValue::try_from_array(batch.column(0), 0).unwrap(),
                ScalarValue::Dictionary(
                    Box::new(DataType::UInt32),
                    Box::new(ScalarValue::Utf8(label.map(str::to_owned)))
                )
            );
            if label.is_some() {
                keys.push(array.keys().values().as_ptr());
            }
        }
        assert!(keys.windows(2).all(|pair| pair[0] == pair[1]));
        drop(batches);
        drop(processor);
        assert_eq!(pool.reserved(), 0);
    }

    #[test]
    fn direct_label_scratch_obeys_pool_limit_and_releases_on_error() {
        let source = input(1, "", vec![5], vec![Some(7.0)]);
        let labels = ["a".repeat(32 * 1024), "b".repeat(32 * 1024)];
        for (limit, succeeds) in [(64 * 1024, false), (4 * 1024 * 1024, true)] {
            let pool = Arc::new(GreedyMemoryPool::new(limit));
            let mut processor = create(&source, &pool);
            let spec = Arc::get_mut(&mut processor.spec).unwrap();
            let mut fields = spec.input_schema.fields().to_vec();
            fields.push(Arc::new(Field::new("other", DataType::Utf8, true)));
            spec.input_schema = Arc::new(Schema::new(fields));
            let mut fields = spec.schema.arrow_schema().fields().to_vec();
            fields.insert(1, Arc::new(Field::new("other", DataType::Utf8, true)));
            spec.schema = Arc::new(
                datatypes::schema::Schema::try_from(Arc::new(Schema::new(fields))).unwrap(),
            );
            spec.output.insert(1, RangeOutput::Tag(4));
            let mut blocks = std::iter::once(Ok(VisibleSeriesBlock {
                first_timestamp: 5,
                timestamp_step: 0,
                values: Float64Array::from(vec![7.0]).into(),
            }));
            let result = processor.process_series(
                &[
                    ("label".into(), Value::from(labels[0].as_str())),
                    ("other".into(), Value::from(labels[1].as_str())),
                ],
                &mut blocks,
            );
            if succeeds {
                result.unwrap();
                let output = processor.finish().unwrap();
                assert_eq!(output.len(), 1);
                for (column, label) in labels.iter().enumerate() {
                    assert_eq!(
                        ScalarValue::try_from_array(output[0].column(column), 0).unwrap(),
                        ScalarValue::Utf8(Some(label.clone()))
                    );
                }
            } else {
                assert!(result.is_err());
                assert_eq!(blocks.len(), 1);
                assert!(processor.finish().unwrap().is_empty());
                assert_eq!(pool.reserved(), 0);
            }
            drop(processor);
            assert_eq!(pool.reserved(), 0);
        }
    }

    #[test]
    fn dictionary_range_output_fits_bounded_pool() {
        let first = input(1, "a", vec![5], vec![Some(7.0)]);
        let pool = Arc::new(GreedyMemoryPool::new(256 * 1024));
        let mut processor = create(&first, &pool);
        let spec = Arc::get_mut(&mut processor.spec).unwrap();
        let dictionary = DataType::Dictionary(Box::new(DataType::UInt32), Box::new(DataType::Utf8));
        let mut fields = spec.input_schema.fields().to_vec();
        fields[3] = Arc::new(Field::new("label", dictionary.clone(), true));
        spec.input_schema = Arc::new(Schema::new(fields));
        let mut fields = spec.schema.arrow_schema().fields().to_vec();
        fields[0] = Arc::new(Field::new("label", dictionary, true));
        spec.schema =
            Arc::new(datatypes::schema::Schema::try_from(Arc::new(Schema::new(fields))).unwrap());
        spec.grid = Arc::new(TimeGrid::new(10, 10_000, 10).unwrap());
        let mut blocks = std::iter::once(Ok(VisibleSeriesBlock {
            first_timestamp: 5,
            timestamp_step: 10,
            values: Float64Array::from(vec![7.0; 1000]).into(),
        }));
        processor
            .process_series(&[("label".into(), Value::from("a"))], &mut blocks)
            .unwrap();
        let output = processor.finish().unwrap();
        assert_eq!(output.len(), 1);
        assert_eq!(output[0].num_rows(), 1000);
        for row in [0, 999] {
            assert_eq!(
                ScalarValue::try_from_array(output[0].column(0), row).unwrap(),
                ScalarValue::Dictionary(
                    Box::new(DataType::UInt32),
                    Box::new(ScalarValue::Utf8(Some("a".into())))
                )
            );
            assert_eq!(
                ScalarValue::try_from_array(output[0].column(2), row).unwrap(),
                ScalarValue::Float64(Some(7.0))
            );
        }
        drop(output);
        drop(processor);
        assert_eq!(pool.reserved(), 0);
    }

    #[test]
    fn range_consumer_preserves_series_boundaries_and_releases_memory() {
        let times = [1, 5, 10, 11, 15, 20];
        let values = [
            f64::NAN,
            2.0,
            f64::from_bits(0x7ff0_0000_0000_0002),
            -0.0,
            0.0,
            f64::NAN,
        ];
        let first = input(
            1,
            "a",
            times.to_vec(),
            values.iter().copied().map(Some).collect(),
        );
        for direct in [false, true] {
            let pool = Arc::new(GreedyMemoryPool::new(1 << 20));
            let mut processor = create(&first, &pool);
            if direct {
                let mut blocks = times.iter().zip(values).map(|(&t, v)| {
                    Ok(VisibleSeriesBlock {
                        first_timestamp: t,
                        timestamp_step: 0,
                        values: Float64Array::from(vec![v]).into(),
                    })
                });
                processor
                    .process_series(&[("label".into(), Value::from("a"))], &mut blocks)
                    .unwrap();
            } else {
                for (&t, v) in times.iter().zip(values) {
                    processor
                        .process(&input(1, "a", vec![t], vec![Some(v)]))
                        .unwrap();
                }
                // A null sample must not remove the other samples in its range.
                processor
                    .process(&input(1, "a", vec![21], vec![None]))
                    .unwrap();
            }
            processor
                .process(&input(2, "b", vec![5], vec![Some(7.0)]))
                .unwrap();
            let output = processor.finish().unwrap();
            assert_eq!(output.len(), 2);
            let actual = |batch: &RecordBatch| {
                batch
                    .column(2)
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap()
                    .values()
                    .iter()
                    .map(|v| v.to_bits())
                    .collect::<Vec<_>>()
            };
            assert_eq!(
                actual(&output[0]),
                vec![2.0f64.to_bits(), (-0.0f64).to_bits()]
            );
            assert_eq!(actual(&output[1]), vec![7.0f64.to_bits()]);
            assert!(pool.reserved() > 0);
            drop(output);
            drop(processor);
            assert_eq!(pool.reserved(), 0);
        }
        let pool = Arc::new(GreedyMemoryPool::new(1));
        let mut processor = create(&first, &pool);
        assert!(processor.process(&first).is_err());
        drop(processor);
        assert_eq!(pool.reserved(), 0);
    }
}
