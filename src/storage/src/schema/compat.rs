//! Utilities for resolving schema compatibility problems.

use datatypes::schema::{ColumnSchema, SchemaRef};
use datatypes::vectors::VectorRef;
use snafu::{ensure, OptionExt, ResultExt};

use crate::error::{self, Result};
use crate::read::Batch;
use crate::schema::StoreSchema;

/// Make schema compatible to write to target with another schema.
pub trait CompatWrite {
    /// Makes the schema of `self` compatible with `dest_schema`.
    ///
    /// For column in `dest_schema` but not in `self`, this method would insert a
    /// vector with default value.
    ///
    /// If there are columns not in `dest_schema`, an error would be returned.
    fn compat_write(&mut self, dest_schema: &SchemaRef) -> Result<()>;
}

/// Checks whether column with `source_schema` could be read as a column with `dest_schema`.
///
/// Returns
/// - `Ok(true)` if `source_schema` is compatible to read using `dest_schema` as schema.
/// - `Ok(false)` if they are considered different columns.
/// - `Err` if there is incompatible issue that could not be resolved.
fn is_source_column_readable(
    source_schema: &ColumnSchema,
    dest_schema: &ColumnSchema,
) -> Result<bool> {
    debug_assert_eq!(source_schema.name, dest_schema.name);
    // TODO(yingwen): Check column id.

    ensure!(
        source_schema.data_type == dest_schema.data_type,
        error::CompatReadSnafu {
            reason: format!(
                "could not read column {} from {:?} type as {:?} type",
                dest_schema.name, source_schema.data_type, dest_schema.data_type
            ),
        }
    );

    ensure!(
        dest_schema.is_nullable() || !source_schema.is_nullable(),
        error::CompatReadSnafu {
            reason: format!(
                "unable to read nullable data for non null column {}",
                dest_schema.name
            ),
        }
    );

    Ok(true)
}

/// Read data in source schema as dest schema.
#[derive(Debug)]
pub struct ReadResolver {
    /// Schema of data source.
    source_schema: StoreSchema,
    /// Schema user expects to read.
    dest_schema: StoreSchema,
    /// For each column in dest schema, stores the index in read result for
    /// this column, or None if the column is not in result.
    ///
    /// This vec would be left empty if `source_version == dest_version`.
    indices_in_result: Vec<Option<usize>>,
    /// For each column in source schema, stores whether we need to read that column. All
    /// columns are needed by default.
    is_needed: Vec<bool>,
    /// End of row key columns in `is_needed`.
    row_key_end: usize,
    /// End of user key columns in `is_needed`.
    user_column_end: usize,
}

impl ReadResolver {
    pub fn new(source_schema: &StoreSchema, dest_schema: &StoreSchema) -> Result<ReadResolver> {
        let (source_version, dest_version) = (source_schema.version(), dest_schema.version());
        if source_version == dest_version {
            debug_assert_eq!(source_schema, dest_schema);

            let is_needed = vec![true; source_schema.num_columns()];

            return Ok(ReadResolver {
                source_schema: source_schema.clone(),
                dest_schema: dest_schema.clone(),
                indices_in_result: Vec::new(),
                is_needed,
                row_key_end: source_schema.row_key_end(),
                user_column_end: source_schema.user_column_end(),
            });
        }

        let mut indices_in_result = vec![None; dest_schema.num_columns()];
        let mut is_needed = vec![true; source_schema.num_columns()];
        let mut row_key_end = 0;
        // Number of value columns.
        let mut num_values = 0;
        // Number of columns in result from source data.
        let mut num_columns_in_result = 0;

        for (idx, source_column) in source_schema.schema().column_schemas().iter().enumerate() {
            // For each column in source schema, check whether we need to read it.
            if let Some(dest_idx) = dest_schema
                .schema()
                .column_index_by_name(&source_column.name)
            {
                let dest_column = &dest_schema.schema().column_schemas()[dest_idx];
                // Check whether we could read this column.
                if is_source_column_readable(source_column, dest_column)? {
                    // Mark that this column could be read from source data, since some
                    // columns in source schema would be skipped, we should not use
                    // the source column's index directly.
                    indices_in_result[dest_idx] = Some(num_columns_in_result);
                    num_columns_in_result += 1;

                    if source_schema.is_key_column_index(idx) {
                        // This column is also a key column in source schema.
                        row_key_end += 1;
                    } else if source_schema.is_user_column_index(idx) {
                        // This column is not a key column but a value column.
                        num_values += 1;
                    }
                } else {
                    // This column is not the same column in dest schema, should be fill by default value
                    // instead of reading from source data.
                    is_needed[idx] = false;
                }
            } else {
                // The column is not in `dest_schema`, we don't need to read it.
                is_needed[idx] = false;
            }
        }

        Ok(ReadResolver {
            source_schema: source_schema.clone(),
            dest_schema: dest_schema.clone(),
            indices_in_result,
            is_needed,
            row_key_end,
            user_column_end: row_key_end + num_values,
        })
    }

    /// Returns a bool slice to denote which key column in source is needed.
    #[inline]
    pub fn source_key_needed(&self) -> &[bool] {
        &self.is_needed[..self.row_key_end]
    }

    /// Returns a bool slice to denote which value column in source is needed.
    #[inline]
    pub fn source_value_needed(&self) -> &[bool] {
        &self.is_needed[self.row_key_end..self.user_column_end]
    }

    /// Construct a new [Batch] from row key, value, sequence and op_type.
    ///
    /// # Panics
    /// Panics if input `VectorRef` is empty.
    pub fn batch_from_parts(
        &self,
        row_key_columns: Vec<VectorRef>,
        mut value_columns: Vec<VectorRef>,
        sequences: VectorRef,
        op_types: VectorRef,
    ) -> Result<Batch> {
        // Each vector should has same length, so here we just use the length of `sequence`.
        let num_rows = sequences.len();

        let mut source = row_key_columns;
        // Reserve space for value, sequence and op_type
        source.reserve(value_columns.len() + 2);
        source.append(&mut value_columns);
        // Internal columns are push in sequence, op_type order.
        source.push(sequences);
        source.push(op_types);

        if self.source_schema.version() == self.dest_schema.version() {
            return Ok(Batch::new(source));
        }

        let column_schemas = self.dest_schema.schema().column_schemas();
        let columns = self
            .indices_in_result
            .iter()
            .zip(column_schemas)
            .map(|(index_opt, column_schema)| {
                if let Some(idx) = index_opt {
                    Ok(source[*idx].clone())
                } else {
                    let vector = column_schema
                        .create_default_vector(num_rows)
                        .context(error::CreateDefaultToReadSnafu {
                            column: &column_schema.name,
                        })?
                        .context(error::NoDefaultToReadSnafu {
                            column: &column_schema.name,
                        })?;
                    Ok(vector)
                }
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(Batch::new(columns))
    }

    //
}
