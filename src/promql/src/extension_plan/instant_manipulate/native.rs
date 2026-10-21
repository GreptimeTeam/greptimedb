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

use datafusion::arrow::array::{ArrayRef, Float64Array, StringArray, TimestampMillisecondArray};
use datafusion::arrow::datatypes::{DataType, SchemaRef, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datatypes::series::MetricName;
use datatypes::series::raw::RawTimestamps;
use datatypes::series::samples::{SampleRef, Samples, SeriesFields, SeriesSchema};

#[cfg(test)]
#[path = "native_tests.rs"]
mod tests;

use crate::extension_plan::instant_manipulate::InstantManipulate;
use crate::instant_eval::select_instants;

#[derive(Clone)]
enum Column {
    Time,
    Field(usize),
    Tag(String),
}

/// Preflighted mapping from a complete typed series to the original query schema.
pub(crate) struct NativeInstantMapping {
    schema: SchemaRef,
    series_schema: Arc<SeriesSchema>,
    columns: Vec<Column>,
    fields: Vec<usize>,
    start: i64,
    end: i64,
    step: i64,
    lookback: i64,
}

impl NativeInstantMapping {
    /// Returns `None` when the node/schema is outside the bounded native subset.
    pub(crate) fn try_new(
        node: &InstantManipulate,
        schema: SchemaRef,
        series_schema: Arc<SeriesSchema>,
        logical_tags: &[String],
    ) -> DataFusionResult<Option<Self>> {
        if node.offset != 0 || node.unfix.is_some() || node.start >= node.end {
            return Ok(None);
        }
        if node.interval <= 0 {
            return Err(DataFusionError::Execution(
                "InstantManipulate interval must be positive".into(),
            ));
        }
        if node.lookback_delta < 0 {
            return Err(DataFusionError::Execution(
                "InstantManipulate lookback must be non-negative".into(),
            ));
        }
        if series_schema.eval_grid.is_some() {
            return Ok(None);
        }

        let mut names = std::collections::HashSet::new();
        for field in series_schema.fields.iter() {
            if !names.insert(field.name.as_ref()) {
                return Ok(None);
            }
        }
        if logical_tags.iter().any(|name| !names.insert(name.as_str())) {
            return Ok(None);
        }

        let stale_field_names = node.staleness_field_columns().collect::<Vec<_>>();
        if stale_field_names.iter().any(|name| {
            series_schema
                .fields
                .iter()
                .all(|field| field.name.as_ref() != *name)
        }) {
            return Ok(None);
        }

        let mut columns = Vec::with_capacity(schema.fields().len());
        let mut seen_fields = vec![false; series_schema.fields.len()];
        let mut seen_tags = vec![false; logical_tags.len()];
        let mut seen_time = false;
        for field in schema.fields() {
            let name = field.name();
            let column = if name == node.time_index_column.as_str() {
                if seen_time
                    || field.data_type() != &DataType::Timestamp(TimeUnit::Millisecond, None)
                {
                    return Ok(None);
                }
                seen_time = true;
                Column::Time
            } else if let Some(index) = series_schema
                .fields
                .iter()
                .position(|v| v.name.as_ref() == name)
            {
                if seen_fields[index] || field.data_type() != &DataType::Float64 {
                    return Ok(None);
                }
                seen_fields[index] = true;
                Column::Field(index)
            } else if let Some(index) = logical_tags.iter().position(|tag| tag == name) {
                if seen_tags[index] || !is_string_type(field.data_type()) {
                    return Ok(None);
                }
                seen_tags[index] = true;
                Column::Tag(name.clone())
            } else {
                return Ok(None);
            };
            columns.push(column);
        }
        if !seen_time || seen_fields.iter().any(|seen| !seen) || seen_tags.iter().any(|seen| !seen)
        {
            return Ok(None);
        }

        let fields = stale_field_names
            .iter()
            .filter_map(|name| {
                series_schema
                    .fields
                    .iter()
                    .position(|field| field.name.as_ref() == *name)
            })
            .collect();
        Ok(Some(Self {
            schema,
            series_schema,
            columns,
            fields,
            start: node.start,
            end: node.end,
            step: node.interval,
            lookback: node.lookback_delta,
        }))
    }

    /// The original output schema retained by this mapping.
    pub(crate) fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Estimates the clipped evaluation grid for complete source timestamp bounds.
    ///
    /// This count is what the caller reserves the workspace of a selection for before it selects,
    /// so nothing but the reservation bounds the grid: a grid too long to index is refused as
    /// exhausted rather than by a point limit of its own.
    pub(crate) fn estimated_points(&self, first: i64, last: i64) -> DataFusionResult<usize> {
        let first = first as i128;
        let last = last as i128;
        let useful_last = if self.lookback == 0 {
            last
        } else {
            last + self.lookback as i128 - 1
        };
        let query_start = self.start as i128;
        let query_end = self.end as i128;
        let max_start = first.max(query_start);
        let min_end = useful_last.min(query_end);
        let (aligned_start, aligned_end) = if max_start > min_end {
            (1_i128, 0_i128)
        } else {
            (
                query_start + (max_start - query_start) / self.step as i128 * self.step as i128,
                query_end - (query_end - min_end) / self.step as i128 * self.step as i128,
            )
        };
        let points = if aligned_end >= aligned_start {
            (aligned_end - aligned_start) / self.step as i128 + 1
        } else {
            0
        };
        usize::try_from(points).map_err(|_| {
            DataFusionError::ResourcesExhausted("native instant point count overflow".into())
        })
    }

    /// Selects instants against typed samples, then maps only selected values to Arrow.
    pub(crate) fn evaluate(
        &self,
        metric: &MetricName,
        time: &RawTimestamps,
        fields: &SeriesFields,
    ) -> DataFusionResult<RecordBatch> {
        if fields.len() != self.series_schema.fields.len() {
            return Err(DataFusionError::Execution(
                "native instant field count mismatch".into(),
            ));
        }
        let checked_fields = self
            .fields
            .iter()
            .map(|i| fields.as_slice()[*i].clone())
            .collect::<Vec<_>>();
        let checked_fields = if checked_fields.is_empty() {
            vec![Samples::from_float(Float64Array::new_null(time.len()))]
        } else {
            checked_fields
        };
        let checked = SeriesFields::try_from_vec(checked_fields)
            .map_err(|e| DataFusionError::Execution(e.to_string()))?;
        let (indices, timestamps) = select_instants(
            time,
            &checked,
            self.start..=self.end,
            self.step,
            self.lookback,
            0,
        )?;
        if indices.is_empty() {
            return Ok(RecordBatch::new_empty(self.schema.clone()));
        }

        let arrays: DataFusionResult<Vec<ArrayRef>> = self
            .columns
            .iter()
            .enumerate()
            .map(|(output_index, column)| match column {
                Column::Time => {
                    Ok(Arc::new(TimestampMillisecondArray::from(timestamps.clone())) as ArrayRef)
                }
                Column::Field(index) => {
                    let source = fields.get(*index).ok_or_else(|| {
                        DataFusionError::Execution("native instant field missing".into())
                    })?;
                    let mut samples = source.iter();
                    let mut cursor = 0usize;
                    let mut selected_index = None;
                    let mut selected_value = None;
                    let mut values = Vec::with_capacity(indices.len());
                    for index in &indices {
                        let index = *index as usize;
                        if selected_index != Some(index) {
                            while cursor <= index {
                                selected_value = Some(
                                    match samples.next().ok_or_else(|| {
                                        DataFusionError::Execution(
                                        "native instant sample iterator ended before selected index"
                                            .into(),
                                    )
                                    })? {
                                        SampleRef::Float(value) => Some(value),
                                        SampleRef::Missing => None,
                                        SampleRef::Histogram(_) => {
                                            return Err(DataFusionError::Execution(
                                                "native instant expected Float64 sample".into(),
                                            ));
                                        }
                                    },
                                );
                                selected_index = Some(cursor);
                                cursor += 1;
                            }
                        }
                        values.push(selected_value.ok_or_else(|| {
                            DataFusionError::Execution(
                                "native instant selected sample was not initialized".into(),
                            )
                        })?);
                    }
                    Ok(Arc::new(Float64Array::from(values)) as ArrayRef)
                }
                Column::Tag(name) => {
                    let values = std::iter::repeat_with(|| metric.get(name))
                        .take(indices.len())
                        .collect::<StringArray>();
                    let target = self.schema.field(output_index).data_type();
                    let cast = datafusion::arrow::compute::cast(&values, target)?;
                    Ok(cast)
                }
            })
            .collect();
        Ok(RecordBatch::try_new(self.schema.clone(), arrays?)?)
    }
}

fn is_string_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    ) || matches!(data_type, DataType::Dictionary(key, value) if key.as_ref() == &DataType::UInt32 && value.as_ref() == &DataType::Utf8)
}
