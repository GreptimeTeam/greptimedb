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

//! Parquet writer.

use std::sync::Arc;

use common_datasource::file_format::parquet::BufferedWriter;
use common_telemetry::debug;
use common_time::Timestamp;
use object_store::ObjectStore;
use parquet::basic::{ColumnOrder, Compression, Encoding, ZstdLevel};
use parquet::file::metadata::{FileMetaData, KeyValue, ParquetMetaData, RowGroupMetaData};
use parquet::file::properties::{WriterProperties, WriterPropertiesBuilder};
use parquet::format::{ColumnOrder as TColumnOrder, FileMetaData as TFileMetaData};
use parquet::schema::types;
use parquet::schema::types::{from_thrift, ColumnPath, SchemaDescPtr, SchemaDescriptor};
use snafu::ResultExt;
use store_api::metadata::RegionMetadataRef;
use store_api::storage::consts::SEQUENCE_COLUMN_NAME;

use crate::error::{InvalidMetadataSnafu, Result, WriteBufferSnafu};
use crate::read::{Batch, Source};
use crate::sst::parquet::format::WriteFormat;
use crate::sst::parquet::{SstInfo, WriteOptions, PARQUET_METADATA_KEY};

/// Parquet SST writer.
pub struct ParquetWriter {
    /// SST output file path.
    file_path: String,
    /// Input data source.
    source: Source,
    /// Region metadata of the source and the target SST.
    metadata: RegionMetadataRef,
    object_store: ObjectStore,
}

impl ParquetWriter {
    /// Creates a new parquet SST writer.
    pub fn new(
        file_path: String,
        metadata: RegionMetadataRef,
        source: Source,
        object_store: ObjectStore,
    ) -> ParquetWriter {
        ParquetWriter {
            file_path,
            source,
            metadata,
            object_store,
        }
    }

    /// Iterates source and writes all rows to Parquet file.
    ///
    /// Returns the [SstInfo] if the SST is written.
    pub async fn write_all(&mut self, opts: &WriteOptions) -> Result<Option<SstInfo>> {
        let json = self.metadata.to_json().context(InvalidMetadataSnafu)?;
        let key_value_meta = KeyValue::new(PARQUET_METADATA_KEY.to_string(), json);

        // TODO(yingwen): Find and set proper column encoding for internal columns: op type and tsid.
        let props_builder = WriterProperties::builder()
            .set_key_value_metadata(Some(vec![key_value_meta]))
            .set_compression(Compression::ZSTD(ZstdLevel::default()))
            .set_encoding(Encoding::PLAIN)
            .set_max_row_group_size(opts.row_group_size);

        let props_builder = Self::customize_column_config(props_builder, &self.metadata);
        let writer_props = props_builder.build();

        let write_format = WriteFormat::new(self.metadata.clone());
        let mut buffered_writer = BufferedWriter::try_new(
            self.file_path.clone(),
            self.object_store.clone(),
            write_format.arrow_schema(),
            Some(writer_props),
            opts.write_buffer_size.as_bytes() as usize,
        )
        .await
        .context(WriteBufferSnafu)?;

        let mut stats = SourceStats::default();
        while let Some(batch) = self.source.next_batch().await? {
            stats.update(&batch);
            let arrow_batch = write_format.convert_batch(&batch)?;

            buffered_writer
                .write(&arrow_batch)
                .await
                .context(WriteBufferSnafu)?;
        }

        if stats.num_rows == 0 {
            debug!(
                "No data written, try to stop the writer: {}",
                self.file_path
            );

            buffered_writer.close().await.context(WriteBufferSnafu)?;
            return Ok(None);
        }

        let (file_meta, file_size) = buffered_writer.close().await.context(WriteBufferSnafu)?;

        // Safety: num rows > 0 so we must have min/max.
        let time_range = stats.time_range.unwrap();

        // convert file_meta to ParquetMetaData
        let parquet_metadata = convert_t_file_meta_to_parquet_meta_data(file_meta);

        // object_store.write will make sure all bytes are written or an error is raised.
        Ok(Some(SstInfo {
            time_range,
            file_size,
            num_rows: stats.num_rows,
            file_metadata: parquet_metadata.ok(),
        }))
    }

    /// Customizes per-column config according to schema and maybe column cardinality.
    fn customize_column_config(
        builder: WriterPropertiesBuilder,
        region_metadata: &RegionMetadataRef,
    ) -> WriterPropertiesBuilder {
        let ts_col = ColumnPath::new(vec![region_metadata
            .time_index_column()
            .column_schema
            .name
            .clone()]);
        let seq_col = ColumnPath::new(vec![SEQUENCE_COLUMN_NAME.to_string()]);

        builder
            .set_column_encoding(seq_col.clone(), Encoding::DELTA_BINARY_PACKED)
            .set_column_dictionary_enabled(seq_col, false)
            .set_column_encoding(ts_col.clone(), Encoding::DELTA_BINARY_PACKED)
            .set_column_dictionary_enabled(ts_col, false)
    }
}

#[derive(Default)]
struct SourceStats {
    /// Number of rows fetched.
    num_rows: usize,
    /// Time range of fetched batches.
    time_range: Option<(Timestamp, Timestamp)>,
}

impl SourceStats {
    fn update(&mut self, batch: &Batch) {
        if batch.is_empty() {
            return;
        }

        self.num_rows += batch.num_rows();
        // Safety: batch is not empty.
        let (min_in_batch, max_in_batch) = (
            batch.first_timestamp().unwrap(),
            batch.last_timestamp().unwrap(),
        );
        if let Some(time_range) = &mut self.time_range {
            time_range.0 = time_range.0.min(min_in_batch);
            time_range.1 = time_range.1.max(max_in_batch);
        } else {
            self.time_range = Some((min_in_batch, max_in_batch));
        }
    }
}

// refer to https://github.com/apache/arrow-rs/blob/7e134f4d277c0b62c27529fc15a4739de3ad0afd/parquet/src/file/footer.rs#L72-L90
/// Convert [TFileMetaData] to [ParquetMetaData]
fn convert_t_file_meta_to_parquet_meta_data(
    t_file_metadata: TFileMetaData,
) -> Result<ParquetMetaData> {
    let schema = from_thrift(&t_file_metadata.schema)?;
    let schema_desc_ptr = Arc::new(SchemaDescriptor::new(schema));
    let row_groups: Vec<_> = t_file_metadata
        .row_groups
        .into_iter()
        .map(|rg| RowGroupMetaData::from_thrift(schema_desc_ptr.clone(), rg)?)
        .collect();

    let column_orders = parse_column_orders(t_file_metadata.column_orders, &schema_desc_ptr);

    let file_metadata = FileMetaData::new(
        t_file_metadata.version,
        t_file_metadata.num_rows,
        t_file_metadata.created_by,
        t_file_metadata.key_value_metadata,
        schema_desc_ptr,
        column_orders,
    );

    Ok(ParquetMetaData::new(file_metadata, row_groups))
}

// Port from https://github.com/apache/arrow-rs/blob/7e134f4d277c0b62c27529fc15a4739de3ad0afd/parquet/src/file/footer.rs#L106-L137
/// Parses column orders from Thrift definition.
/// If no column orders are defined, returns `None`.
fn parse_column_orders(
    t_column_orders: Option<Vec<TColumnOrder>>,
    schema_descr: &SchemaDescriptor,
) -> Option<Vec<ColumnOrder>> {
    match t_column_orders {
        Some(orders) => {
            // Should always be the case
            assert_eq!(
                orders.len(),
                schema_descr.num_columns(),
                "Column order length mismatch"
            );
            let mut res = Vec::new();
            for (i, column) in schema_descr.columns().iter().enumerate() {
                match orders[i] {
                    TColumnOrder::TYPEORDER(_) => {
                        let sort_order = ColumnOrder::get_sort_order(
                            column.logical_type(),
                            column.converted_type(),
                            column.physical_type(),
                        );
                        res.push(ColumnOrder::TYPE_DEFINED_ORDER(sort_order));
                    }
                }
            }
            Some(res)
        }
        None => None,
    }
}
