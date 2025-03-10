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

//! Prometheus remote write support.
//!
//! The Prometheus remote write format is a Parquet representation of the Prometheus remote write protocol.
//! - Each Line contains a single timeseries.
//! - Each series only occurs once.

use std::collections::HashMap;
use std::sync::Arc;

use api::prom_store::remote::{Label, TimeSeries};
use api::v1::OpType;
use arrow_array::{
    Array, BinaryArray, Float64Array, TimestampMillisecondArray, UInt64Array, UInt8Array,
};
use datatypes::value::ValueRef;
use futures::{AsyncBufReadExt, StreamExt};
use metric_engine::row_modifier::TsidGenerator;
use mito2::error::ReadParquetSnafu;
use mito2::read::{Batch, BatchBuilder, BatchReader};
use mito2::row_converter::SparsePrimaryKeyCodec;
use object_store::{FuturesAsyncReader, ObjectStore, Reader};
use parquet::arrow::async_reader::ParquetRecordBatchStream;
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use parquet_opendal::AsyncReader;
use prost::Message;
use snafu::{OptionExt, ResultExt};
use store_api::metadata::RegionMetadataRef;
use store_api::storage::consts::ReservedColumnId;
use store_api::storage::{ColumnId, SequenceNumber};
use table::metadata::TableId;

use crate::error::{
    DecodeSnafu, IoSnafu, JsonSnafu, MissingColumnSnafu, MissingMetricNameSnafu, MissingTableSnafu,
    MitoSnafu, ObjectStoreSnafu, Result,
};
use crate::table::TableMetadataHelper;

const METRIC_NAME_LABEL: &str = "__name__";
const GREPTIME_VALUE: &str = "greptime_value";

/// A reader that reads remote write file, sorts and outputs timeseries in the primary key order.
pub struct RemoteWriteReader {
    /// Timeseries sorted by primary key in reverse order.
    /// So we can pop the series.
    series: Vec<(Vec<u8>, TimeSeries)>,
    /// Converter for converting timeseries to batches.
    converter: SeriesConverter,
}

impl RemoteWriteReader {
    /// Creates a new [`RemoteWriteReader`] from a object store.
    /// It reads and sorts timeseries.
    pub async fn open(
        operator: ObjectStore,
        path: &str,
        catalog: &str,
        schema: &str,
        metadata: RegionMetadataRef,
        table_helper: TableMetadataHelper,
    ) -> Result<Self> {
        let codec = SparsePrimaryKeyCodec::new(&metadata);
        let value_id = metadata
            .column_by_name(GREPTIME_VALUE)
            .context(MissingColumnSnafu {
                column_name: GREPTIME_VALUE,
            })?
            .column_id;
        let encoder = PrimaryKeyEncoder {
            catalog: catalog.to_string(),
            schema: schema.to_string(),
            metadata,
            table_helper,
            table_ids: HashMap::new(),
            codec,
        };
        let converter = SeriesConverter {
            value_id,
            override_sequence: None,
        };
        let mut sorter = TimeSeriesSorter::new(encoder);

        let mut reader = TimeSeriesParquetReader::new(operator, path).await?;
        while let Some(series) = reader.next_series().await? {
            sorter.push(series).await?;
        }
        let mut series = sorter.sort();
        series.reverse();

        Ok(Self { series, converter })
    }

    /// Sets the sequence number of the batch.
    /// Otherwise, the sequence number will be 0.
    pub fn with_sequence(mut self, sequence: SequenceNumber) -> Self {
        self.converter.override_sequence = Some(sequence);
        self
    }

    fn next_series(&mut self) -> Option<(Vec<u8>, TimeSeries)> {
        self.series.pop()
    }
}

#[async_trait::async_trait]
impl BatchReader for RemoteWriteReader {
    async fn next_batch(&mut self) -> mito2::error::Result<Option<Batch>> {
        let Some((pk, series)) = self.next_series() else {
            return Ok(None);
        };

        self.converter.convert(pk, series).map(Some)
    }
}

struct SeriesConverter {
    /// Column id for the value field.
    value_id: ColumnId,
    /// Sequence number of the batch.
    override_sequence: Option<SequenceNumber>,
}

impl SeriesConverter {
    /// Builds a batch from a primary key and a time series.
    fn convert(&self, primary_key: Vec<u8>, series: TimeSeries) -> mito2::error::Result<Batch> {
        let num_rows = series.samples.len();
        let op_types = vec![OpType::Put as u8; num_rows];
        // TODO(yingwen): Should we use 0 or 1 as default?
        let sequences = vec![self.override_sequence.unwrap_or(0); num_rows];
        let mut timestamps = Vec::with_capacity(num_rows);
        let mut values = Vec::with_capacity(num_rows);
        for sample in series.samples {
            timestamps.push(sample.timestamp);
            values.push(sample.value);
        }

        let mut builder = BatchBuilder::new(primary_key);
        builder
            .timestamps_array(Arc::new(TimestampMillisecondArray::from(timestamps)))?
            .sequences_array(Arc::new(UInt64Array::from(sequences)))?
            .op_types_array(Arc::new(UInt8Array::from(op_types)))?
            .push_field_array(self.value_id, Arc::new(Float64Array::from(values)))?;
        builder.build()
    }
}

/// Prometheus remote write NDJSON reader.
pub struct TimeSeriesReader {
    reader: FuturesAsyncReader,
    buffer: String,
}

impl TimeSeriesReader {
    /// Creates a new [`TimeSeriesReader`] from a [`Reader`].
    pub async fn new(reader: Reader) -> Result<Self> {
        let reader = reader
            .into_futures_async_read(..)
            .await
            .context(ObjectStoreSnafu)?;

        Ok(Self {
            reader,
            buffer: String::new(),
        })
    }

    /// Reads the next timeseries from the reader.
    pub async fn next_series(&mut self) -> Result<Option<TimeSeries>> {
        self.buffer.clear();
        self.reader
            .read_line(&mut self.buffer)
            .await
            .context(IoSnafu)?;
        if self.buffer.is_empty() {
            return Ok(None);
        }

        let time_series = serde_json::from_str(&self.buffer).context(JsonSnafu)?;
        Ok(Some(time_series))
    }
}

/// Prometheus remote write parquet reader.
pub struct TimeSeriesParquetReader {
    path: String,
    stream: ParquetRecordBatchStream<AsyncReader>,
    /// Is the stream EOF.
    eof: bool,
    /// Current binary array.
    array: Option<BinaryArray>,
    /// Current row in the record batch.
    /// Only valid when the record batch is Some.
    row_index: usize,
}

impl TimeSeriesParquetReader {
    /// Creates a new instance of `TimeSeriesParquetReader`.
    pub async fn new(object_store: ObjectStore, path: &str) -> Result<Self> {
        let reader = object_store
            .reader_with(path)
            .await
            .context(ObjectStoreSnafu)?;
        let content_len = object_store
            .stat(path)
            .await
            .context(ObjectStoreSnafu)?
            .content_length();
        let reader = AsyncReader::new(reader, content_len).with_prefetch_footer_size(512 * 1024);
        let stream = ParquetRecordBatchStreamBuilder::new(reader)
            .await
            .context(ReadParquetSnafu { path })
            .context(MitoSnafu)?
            .build()
            .context(ReadParquetSnafu { path })
            .context(MitoSnafu)?;

        Ok(Self {
            path: path.to_string(),
            stream,
            eof: false,
            array: None,
            row_index: 0,
        })
    }

    /// Reads the next timeseries from the reader.
    pub async fn next_series(&mut self) -> Result<Option<TimeSeries>> {
        if self.eof {
            return Ok(None);
        }

        if let Some(array) = &self.array {
            if self.row_index >= array.len() {
                self.row_index = 0;
                self.array = None;
            }
        }

        if self.array.is_none() {
            let Some(record_batch) = self
                .stream
                .next()
                .await
                .transpose()
                .context(ReadParquetSnafu { path: &self.path })
                .context(MitoSnafu)?
            else {
                self.eof = true;
                return Ok(None);
            };
            let array = record_batch
                .column(0)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .context(MissingColumnSnafu {
                    column_name: "remote write json column",
                })?
                .clone();
            assert!(!array.is_empty());
            self.array = Some(array);
        }

        let array = self.array.as_ref().unwrap();
        let value = array.value(self.row_index);
        let time_series = TimeSeries::decode(value).context(DecodeSnafu)?;
        self.row_index += 1;

        Ok(Some(time_series))
    }
}

/// Encoder to encode labels into primary key.
struct PrimaryKeyEncoder {
    /// Catalog name.
    catalog: String,
    /// Schema name.
    schema: String,
    /// The metadata of the physical region.
    metadata: RegionMetadataRef,
    /// Helper to get table metadata.
    table_helper: TableMetadataHelper,
    /// Cached table name to table id.
    table_ids: HashMap<String, TableId>,
    /// Primary key encoder.
    codec: SparsePrimaryKeyCodec,
}

impl PrimaryKeyEncoder {
    /// Encodes the primary key for the given labels.
    /// It'll sort the labels by name before encoding.
    async fn encode_primary_key(
        &mut self,
        labels: &mut Vec<Label>,
        key_buf: &mut Vec<u8>,
    ) -> Result<()> {
        if !labels.is_sorted_by(|left, right| left.name <= right.name) {
            labels.sort_unstable_by(|left, right| left.name.cmp(&right.name));
        }

        // Gets the table id from the label.
        let name_label = labels
            .iter()
            .find(|label| label.name == METRIC_NAME_LABEL)
            .context(MissingMetricNameSnafu)?;
        let table_id = match self.table_ids.get(&name_label.name) {
            Some(id) => *id,
            None => {
                let table_info = self
                    .table_helper
                    .get_table(&self.catalog, &self.schema, &name_label.value)
                    .await?
                    .context(MissingTableSnafu {
                        table_name: &name_label.value,
                    })?;
                let id = table_info.table_info.ident.table_id;
                self.table_ids.insert(name_label.name.clone(), id);

                id
            }
        };
        // Computes the tsid for the given labels.
        let mut generator = TsidGenerator::default();
        for label in &*labels {
            if label.name != METRIC_NAME_LABEL {
                generator.write_label(&label.name, &label.value);
            }
        }
        let tsid = generator.finish();

        key_buf.clear();
        let internal_columns = [
            (ReservedColumnId::table_id(), ValueRef::UInt32(table_id)),
            (ReservedColumnId::tsid(), ValueRef::UInt64(tsid)),
        ];
        self.codec
            .encode_to_vec(internal_columns.into_iter(), key_buf)
            .context(MitoSnafu)?;
        let label_iter = labels.iter().filter_map(|label| {
            if label.name == METRIC_NAME_LABEL {
                return None;
            }

            let column_id = self.metadata.column_by_name(&label.name)?.column_id;
            Some((column_id, ValueRef::String(&label.value)))
        });
        self.codec
            .encode_to_vec(label_iter, key_buf)
            .context(MitoSnafu)?;

        Ok(())
    }
}

/// Prom timeseries sorter.
/// Sorts timeseries by the primary key.
struct TimeSeriesSorter {
    /// Timeseries to sort.
    series: Vec<(Vec<u8>, TimeSeries)>,
    /// Key encoder.
    encoder: PrimaryKeyEncoder,
}

impl TimeSeriesSorter {
    /// Creates a new sorter.
    fn new(encoder: PrimaryKeyEncoder) -> Self {
        Self {
            series: Vec::new(),
            encoder,
        }
    }

    /// Push a timeseries to the sorter.
    async fn push(&mut self, mut series: TimeSeries) -> Result<()> {
        let mut key_buf = Vec::new();
        self.encoder
            .encode_primary_key(&mut series.labels, &mut key_buf)
            .await?;
        self.series.push((key_buf, series));

        Ok(())
    }

    /// Sorts the requests.
    /// Returns the sorted timeseries and the primary key.
    fn sort(&mut self) -> Vec<(Vec<u8>, TimeSeries)> {
        self.series
            .sort_unstable_by(|left, right| left.0.cmp(&right.0));

        std::mem::take(&mut self.series)
    }
}
