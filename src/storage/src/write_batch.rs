use std::collections::HashMap;

use common_error::prelude::*;
use datatypes::vectors::VectorRef;
use snafu::ensure;
use store_api::storage::{consts, PutOperation, WriteRequest};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Duplicate column {} in same request", name))]
    DuplicateColumn { name: String, backtrace: Backtrace },
}

pub type Result<T> = std::result::Result<T, Error>;

/// Implementation of [WriteRequest].
#[derive(Default)]
pub struct WriteBatch {
    batch: Vec<Mutation>,
}

impl WriteRequest for WriteBatch {
    type PutOp = PutData;

    fn new() -> Self {
        Self::default()
    }

    fn put(&mut self, data: PutData) {
        self.batch.push(Mutation::Put(data));
    }
}

enum Mutation {
    Put(PutData),
}

#[derive(Default)]
pub struct PutData {
    columns: HashMap<String, VectorRef>,
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
