use std::any::Any;
use std::collections::HashMap;

use common_error::prelude::*;
use datatypes::data_type::ConcreteDataType;
use datatypes::schema::SchemaRef;
use datatypes::vectors::VectorRef;
use snafu::ensure;
use store_api::storage::{consts, PutOperation, WriteRequest};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Duplicate column {} in same request", name))]
    DuplicateColumn { name: String, backtrace: Backtrace },

    #[snafu(display("Missing column {} in request", name))]
    MissingColumn { name: String, backtrace: Backtrace },

    #[snafu(display(
        "Type of column {} does not match type in schema, expect {:?}, given {:?}",
        name,
        expect,
        given
    ))]
    TypeMismatch {
        name: String,
        expect: ConcreteDataType,
        given: ConcreteDataType,
        backtrace: Backtrace,
    },

    #[snafu(display("Column {} is not null but input has null", name))]
    HasNull { name: String, backtrace: Backtrace },

    #[snafu(display("Unknown column {}", name))]
    UnknownColumn { name: String, backtrace: Backtrace },
}

pub type Result<T> = std::result::Result<T, Error>;

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        StatusCode::InvalidArguments
    }

    fn backtrace_opt(&self) -> Option<&Backtrace> {
        ErrorCompat::backtrace(self)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Implementation of [WriteRequest].
pub struct WriteBatch {
    schema: SchemaRef,
    batch: Vec<Mutation>,
}

enum Mutation {
    Put(PutData),
}

#[derive(Default)]
pub struct PutData {
    columns: HashMap<String, VectorRef>,
}

impl WriteRequest for WriteBatch {
    type Error = Error;
    type PutOp = PutData;

    fn new(schema: SchemaRef) -> Self {
        Self {
            schema,
            batch: Vec::new(),
        }
    }

    fn put(&mut self, data: PutData) -> Result<()> {
        self.validate_put(&data)?;

        self.batch.push(Mutation::Put(data));
        Ok(())
    }
}

impl WriteBatch {
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn validate_put(&self, data: &PutData) -> Result<()> {
        for column_schema in self.schema.column_schemas() {
            match data.column_by_name(&column_schema.name) {
                Some(col) => {
                    ensure!(
                        col.data_type() == column_schema.data_type,
                        TypeMismatchSnafu {
                            name: &column_schema.name,
                            expect: column_schema.data_type.clone(),
                            given: col.data_type(),
                        }
                    );

                    ensure!(
                        column_schema.is_nullable || col.null_count() == 0,
                        HasNullSnafu {
                            name: &column_schema.name,
                        }
                    );
                }
                None => {
                    ensure!(
                        column_schema.is_nullable,
                        MissingColumnSnafu {
                            name: &column_schema.name,
                        }
                    );
                }
            }
        }

        // Check all columns in data also exists in schema.
        for name in data.columns.keys() {
            ensure!(
                self.schema.column_schema_by_name(name).is_some(),
                UnknownColumnSnafu { name }
            );
        }

        Ok(())
    }
}

impl PutData {
    fn add_column_by_name(&mut self, name: &str, vector: VectorRef) -> Result<()> {
        ensure!(
            !self.columns.contains_key(name),
            DuplicateColumnSnafu { name }
        );

        self.columns.insert(name.to_string(), vector);

        Ok(())
    }

    fn column_by_name(&self, name: &str) -> Option<&VectorRef> {
        self.columns.get(name)
    }
}

impl PutOperation for PutData {
    type Error = Error;

    fn new() -> PutData {
        PutData::default()
    }

    fn with_num_columns(num_columns: usize) -> PutData {
        PutData {
            columns: HashMap::with_capacity(num_columns),
        }
    }

    fn add_key_column(&mut self, name: &str, vector: VectorRef) -> Result<()> {
        self.add_column_by_name(name, vector)
    }

    fn add_version_column(&mut self, vector: VectorRef) -> Result<()> {
        self.add_column_by_name(consts::VERSION_COLUMN_NAME, vector)
    }

    fn add_value_column(&mut self, name: &str, vector: VectorRef) -> Result<()> {
        self.add_column_by_name(name, vector)
    }
}
