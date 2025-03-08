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

//! Prometheus remote write JSON support.
//!
//! The Prometheus remote write JSON format is a NDJSON representation of the Prometheus remote write protocol.
//! - Each Line contains a single timeseries.
//! - Each series only occurs once.

use std::collections::HashMap;

use api::prom_store::remote::{Label, TimeSeries};
use datatypes::value::ValueRef;
use futures::AsyncBufReadExt;
use metric_engine::row_modifier::TsidGenerator;
use mito2::read::{Batch, BatchReader};
use mito2::row_converter::SparsePrimaryKeyCodec;
use object_store::{FuturesAsyncReader, ObjectStore, Reader};
use snafu::{OptionExt, ResultExt};
use store_api::metadata::RegionMetadataRef;
use store_api::storage::consts::ReservedColumnId;
use table::metadata::TableId;

use crate::error::{
    IoSnafu, JsonSnafu, MissingMetricNameSnafu, MissingTableSnafu, MitoSnafu, ObjectStoreSnafu,
    Result,
};
use crate::table::TableMetadataHelper;

const METRIC_NAME_LABEL: &str = "__name__";

/// A reader that reads remote write file, sorts and outputs timeseries in the primary key order.
struct RemoteWriteReader {
    /// Timeseries sorted by primary key.
    series: Vec<(Vec<u8>, TimeSeries)>,
    /// Current index in the series.
    index: usize,
}

impl RemoteWriteReader {
    /// Creates a new [`RemoteWriteReader`] from a object store.
    /// It reads and sorts timeseries.
    pub async fn open(
        operator: ObjectStore,
        path: &str,
        catalog: String,
        schema: String,
        metadata: RegionMetadataRef,
        table_helper: TableMetadataHelper,
    ) -> Result<Self> {
        let codec = SparsePrimaryKeyCodec::new(&metadata);
        let encoder = PrimaryKeyEncoder {
            catalog,
            schema,
            metadata,
            table_helper,
            table_ids: HashMap::new(),
            codec,
        };
        let mut sorter = TimeSeriesSorter::new(encoder);

        let reader = operator.reader(path).await.context(ObjectStoreSnafu)?;
        let mut reader = TimeSeriesReader::new(reader).await?;
        while let Some(series) = reader.next_series().await? {
            sorter.push(series).await?;
        }
        let series = sorter.sort();

        Ok(Self { series, index: 0 })
    }
}

#[async_trait::async_trait]
impl BatchReader for RemoteWriteReader {
    async fn next_batch(&mut self) -> mito2::error::Result<Option<Batch>> {
        todo!()
    }
}

/// Prometheus remote write NDJSON reader.
struct TimeSeriesReader {
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
                    .get_table(&self.catalog, &self.schema, &name_label.name)
                    .await
                    .context(MissingTableSnafu)?;
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
