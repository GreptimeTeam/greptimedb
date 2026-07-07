// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");

//! Generic Parquet per-column write overrides.

use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

use datatypes::arrow::record_batch::RecordBatch;
use parquet::basic::{Compression, Encoding, ZstdLevel};
use parquet::file::metadata::KeyValue;
use parquet::file::properties::{WriterProperties, WriterPropertiesBuilder};
use parquet::schema::types::ColumnPath;
use store_api::metadata::RegionMetadataRef;
use store_api::storage::RegionId;

/// Operation kind for generic Parquet write policy.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ParquetWriteOperation {
    /// Memtable flush.
    Flush,
    /// Compaction/rewrite.
    Compaction,
    /// Other write path.
    #[default]
    Other,
}

/// Generic context for one Parquet SST write.
#[derive(Debug, Clone)]
pub struct ParquetWritePolicyContext {
    pub region_id: RegionId,
    pub metadata: RegionMetadataRef,
    pub operation: ParquetWriteOperation,
}

/// Decision after observing a pre-open batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParquetWritePolicyDecision {
    /// Keep buffering/sampling before opening the Parquet writer.
    Pending,
    /// Open the Parquet writer with these options.
    Ready(ParquetWriteOptions),
}

/// A per-write policy session. Implementations must be generic from OSS' point of view.
pub trait ParquetWritePolicySession: Send + Debug {
    /// Returns options that are ready before inspecting any batch.
    fn initial_options(&mut self) -> Option<ParquetWriteOptions> {
        None
    }

    /// Observes one converted Arrow batch before the Parquet writer is opened.
    fn observe_batch(&mut self, batch: &RecordBatch) -> ParquetWritePolicyDecision;
}

/// Provider registered through [`common_base::Plugins`].
pub trait ParquetWritePolicyProvider: Send + Sync + Debug {
    /// Starts a per-write session. Return `None` for no-op/default writer properties.
    fn create_session(
        &self,
        context: ParquetWritePolicyContext,
    ) -> Option<Box<dyn ParquetWritePolicySession>>;
}

pub type ParquetWritePolicyProviderRef = Arc<dyn ParquetWritePolicyProvider>;

/// Per-column Parquet write options.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ParquetColumnWriteOptions {
    /// Optional column encoding override.
    pub encoding: Option<Encoding>,
    /// Optional dictionary setting override.
    pub dictionary_enabled: Option<bool>,
}

impl ParquetColumnWriteOptions {
    /// Overrides the column encoding.
    pub fn with_encoding(mut self, encoding: Encoding) -> Self {
        self.encoding = Some(encoding);
        self
    }

    /// Overrides whether dictionary encoding is enabled.
    pub fn with_dictionary_enabled(mut self, enabled: bool) -> Self {
        self.dictionary_enabled = Some(enabled);
        self
    }
}

/// Generic Parquet write options shared by SST writers.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ParquetWriteOptions {
    /// Column path/name to Parquet write overrides.
    pub column_options: HashMap<String, ParquetColumnWriteOptions>,
}

impl ParquetWriteOptions {
    /// Adds/updates an override for a top-level column name.
    pub fn with_column_options(
        mut self,
        column: impl Into<String>,
        options: ParquetColumnWriteOptions,
    ) -> Self {
        self.column_options.insert(column.into(), options);
        self
    }

    /// Applies generic column overrides to an existing builder.
    pub fn apply_to_builder(
        &self,
        mut builder: WriterPropertiesBuilder,
    ) -> WriterPropertiesBuilder {
        for (column, options) in &self.column_options {
            let column_path = ColumnPath::new(vec![column.clone()]);
            if let Some(encoding) = options.encoding {
                builder = builder.set_column_encoding(column_path.clone(), encoding);
            }
            if let Some(enabled) = options.dictionary_enabled {
                builder = builder.set_column_dictionary_enabled(column_path, enabled);
            }
        }
        builder
    }
}

/// Builds the default Mito SST Parquet writer properties and applies generic overrides.
pub fn sst_writer_properties_builder_with_metadata(
    key_value_meta: Option<Vec<KeyValue>>,
    row_group_size: usize,
    parquet_options: &ParquetWriteOptions,
) -> WriterPropertiesBuilder {
    let builder = WriterProperties::builder()
        .set_key_value_metadata(key_value_meta)
        .set_compression(Compression::ZSTD(ZstdLevel::default()))
        .set_encoding(Encoding::PLAIN)
        .set_max_row_group_row_count(Some(row_group_size))
        .set_column_index_truncate_length(None)
        .set_statistics_truncate_length(None);
    parquet_options.apply_to_builder(builder)
}

/// Builds the default Mito SST Parquet writer properties and applies generic overrides.
pub fn sst_writer_properties(
    key_value_meta: KeyValue,
    row_group_size: usize,
    parquet_options: &ParquetWriteOptions,
    customize: impl FnOnce(WriterPropertiesBuilder) -> WriterPropertiesBuilder,
) -> WriterProperties {
    customize(sst_writer_properties_builder_with_metadata(
        Some(vec![key_value_meta]),
        row_group_size,
        parquet_options,
    ))
    .build()
}

/// Builds the default Mito SST Parquet writer properties with optional metadata.
pub fn sst_writer_properties_with_metadata(
    key_value_meta: Option<Vec<KeyValue>>,
    row_group_size: usize,
    parquet_options: &ParquetWriteOptions,
    customize: impl FnOnce(WriterPropertiesBuilder) -> WriterPropertiesBuilder,
) -> WriterProperties {
    customize(sst_writer_properties_builder_with_metadata(
        key_value_meta,
        row_group_size,
        parquet_options,
    ))
    .build()
}
