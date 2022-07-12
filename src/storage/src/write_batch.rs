use std::any::Any;
use std::collections::{BTreeSet, HashMap};
use std::slice;
use std::time::Duration;

use common_error::prelude::*;
use common_time::RangeMillis;
use datatypes::data_type::ConcreteDataType;
use datatypes::prelude::ScalarVector;
use datatypes::schema::SchemaRef;
use datatypes::vectors::{Int64Vector, VectorRef};
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

    #[snafu(display(
        "Length of column {} not equals to other columns, expect {}, given {}",
        name,
        expect,
        given
    ))]
    LenNotEquals {
        name: String,
        expect: usize,
        given: usize,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Request is too large, max is {}, current is {}",
        MAX_BATCH_SIZE,
        num_rows
    ))]
    RequestTooLarge {
        num_rows: usize,
        backtrace: Backtrace,
    },

    #[snafu(display("Cannot align timestamp: {}", ts))]
    TimestampOverflow { ts: i64 },
}

pub type Result<T> = std::result::Result<T, Error>;

/// Max number of updates of a write batch.
const MAX_BATCH_SIZE: usize = 1_000_000;

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
    mutations: Vec<Mutation>,
    num_rows: usize,
}

impl WriteRequest for WriteBatch {
    type Error = Error;
    type PutOp = PutData;

    fn new(schema: SchemaRef) -> Self {
        Self {
            schema,
            mutations: Vec::new(),
            num_rows: 0,
        }
    }

    fn put(&mut self, data: PutData) -> Result<()> {
        if data.is_empty() {
            return Ok(());
        }

        self.validate_put(&data)?;

        self.add_num_rows(data.num_rows())?;
        self.mutations.push(Mutation::Put(data));

        Ok(())
    }

    // The millisecond of duration must <= i64::MAX
    fn time_ranges(&self, duration: Duration) -> Result<Vec<RangeMillis>> {
        let durations_millis = duration.as_millis() as i64;
        let mut aligned_timestamps: BTreeSet<i64> = BTreeSet::new();

        let ts_col_name = match self.schema.timestamp_column() {
            None => {
                // write batch does not have a timestamp column
                return Ok(Vec::new());
            }
            Some(ts_col) => &ts_col.name,
        };

        for m in &self.mutations {
            match m {
                Mutation::Put(put_data) => {
                    let column = put_data
                        .column_by_name(ts_col_name)
                        .unwrap_or_else(|| panic!("Cannot find column by name: {}", ts_col_name));

                    let ts_vector = column.as_any().downcast_ref::<Int64Vector>().unwrap(); // not expected to fail
                    for ts in ts_vector.iter_data() {
                        match ts {
                            Some(ts) => {
                                let aligned = align_timestamp(ts, durations_millis)
                                    .context(TimestampOverflowSnafu { ts })?;
                                aligned_timestamps.insert(aligned);
                            }
                            None => {}
                        }
                    }
                }
            }
        }

        let ranges = aligned_timestamps
            .iter()
            .map(|t| RangeMillis::new(*t, *t + durations_millis).unwrap())
            .collect::<Vec<_>>();

        Ok(ranges)
    }
}

/// Aligns timestamp to nearest time interval.
/// Negative ts means a timestamp before Unix epoch.
/// If arithmetic overflows, this function returns None.
/// So timestamp within `[i64::MIN, i64::MIN + duration)` is not a valid input.
fn align_timestamp(ts: i64, duration: i64) -> Option<i64> {
    let aligned_ts = if ts < 0 {
        ts.checked_sub(duration - 1)?
    } else {
        ts
    };

    aligned_ts
        .checked_div(duration)
        .and_then(|v| v.checked_mul(duration))
}

// WriteBatch pub methods.
impl WriteBatch {
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    pub fn iter(&self) -> slice::Iter<'_, Mutation> {
        self.mutations.iter()
    }

    pub fn is_empty(&self) -> bool {
        self.mutations.is_empty()
    }
}

pub enum Mutation {
    Put(PutData),
}

#[derive(Default)]
pub struct PutData {
    columns: HashMap<String, VectorRef>,
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
        // TODO(yingwen): Maybe ensure that version column must be a uint64 vector.
        self.add_column_by_name(consts::VERSION_COLUMN_NAME, vector)
    }

    fn add_value_column(&mut self, name: &str, vector: VectorRef) -> Result<()> {
        self.add_column_by_name(name, vector)
    }
}

// PutData pub methods.
impl PutData {
    pub fn column_by_name(&self, name: &str) -> Option<&VectorRef> {
        self.columns.get(name)
    }

    /// Returns number of rows in data.
    pub fn num_rows(&self) -> usize {
        self.columns
            .values()
            .next()
            .map(|col| col.len())
            .unwrap_or(0)
    }

    /// Returns true if no rows in data.
    ///
    /// `PutData` with empty column will also be considered as empty.
    pub fn is_empty(&self) -> bool {
        self.num_rows() == 0
    }
}

impl WriteBatch {
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

    fn add_num_rows(&mut self, len: usize) -> Result<()> {
        let num_rows = self.num_rows + len;
        ensure!(
            num_rows <= MAX_BATCH_SIZE,
            RequestTooLargeSnafu { num_rows }
        );
        self.num_rows = num_rows;
        Ok(())
    }
}

impl<'a> IntoIterator for &'a WriteBatch {
    type Item = &'a Mutation;
    type IntoIter = slice::Iter<'a, Mutation>;

    fn into_iter(self) -> slice::Iter<'a, Mutation> {
        self.iter()
    }
}

impl PutData {
    fn add_column_by_name(&mut self, name: &str, vector: VectorRef) -> Result<()> {
        ensure!(
            !self.columns.contains_key(name),
            DuplicateColumnSnafu { name }
        );

        if let Some(col) = self.columns.values().next() {
            ensure!(
                col.len() == vector.len(),
                LenNotEqualsSnafu {
                    name,
                    expect: col.len(),
                    given: vector.len(),
                }
            );
        }

        self.columns.insert(name.to_string(), vector);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::iter;
    use std::sync::Arc;

    use datatypes::type_id::LogicalTypeId;
    use datatypes::vectors::{BooleanVector, Int32Vector, Int64Vector, UInt64Vector};

    use super::*;
    use crate::test_util::write_batch_util;

    #[test]
    fn test_put_data_basic() {
        let mut put_data = PutData::new();
        assert!(put_data.is_empty());

        let vector1 = Arc::new(Int32Vector::from_slice(&[1, 2, 3, 4, 5]));
        let vector2 = Arc::new(UInt64Vector::from_slice(&[0, 2, 4, 6, 8]));

        put_data.add_key_column("k1", vector1.clone()).unwrap();
        put_data.add_version_column(vector2).unwrap();
        put_data.add_value_column("v1", vector1).unwrap();

        assert_eq!(5, put_data.num_rows());
        assert!(!put_data.is_empty());

        assert!(put_data.column_by_name("no such column").is_none());
        assert!(put_data.column_by_name("k1").is_some());
        assert!(put_data.column_by_name("v1").is_some());
        assert!(put_data
            .column_by_name(consts::VERSION_COLUMN_NAME)
            .is_some());
    }

    #[test]
    fn test_put_data_empty_vector() {
        let mut put_data = PutData::with_num_columns(1);
        assert!(put_data.is_empty());

        let vector1 = Arc::new(Int32Vector::from_slice(&[]));
        put_data.add_key_column("k1", vector1).unwrap();

        assert_eq!(0, put_data.num_rows());
        assert!(put_data.is_empty());
    }

    fn new_test_batch() -> WriteBatch {
        write_batch_util::new_write_batch(
            &[
                ("k1", LogicalTypeId::UInt64, false),
                (consts::VERSION_COLUMN_NAME, LogicalTypeId::UInt64, false),
                ("ts", LogicalTypeId::Int64, false),
                ("v1", LogicalTypeId::Boolean, true),
            ],
            Some(2),
        )
    }

    #[test]
    fn test_write_batch_put() {
        let intv = Arc::new(UInt64Vector::from_slice(&[1, 2, 3]));
        let boolv = Arc::new(BooleanVector::from(vec![true, false, true]));
        let tsv = Arc::new(Int64Vector::from_vec(vec![0, 0, 0]));

        let mut put_data = PutData::new();
        put_data.add_key_column("k1", intv.clone()).unwrap();
        put_data.add_version_column(intv).unwrap();
        put_data.add_value_column("v1", boolv).unwrap();
        put_data.add_key_column("ts", tsv).unwrap();

        let mut batch = new_test_batch();
        assert!(batch.is_empty());
        batch.put(put_data).unwrap();
        assert!(!batch.is_empty());

        let mut iter = batch.iter();
        let Mutation::Put(put_data) = iter.next().unwrap();
        assert_eq!(3, put_data.num_rows());
    }

    fn check_err(err: Error, msg: &str) {
        assert_eq!(StatusCode::InvalidArguments, err.status_code());
        assert!(err.backtrace_opt().is_some());
        assert!(err.to_string().contains(msg));
    }

    #[test]
    fn test_write_batch_too_large() {
        let boolv = Arc::new(BooleanVector::from_iter(
            iter::repeat(Some(true)).take(MAX_BATCH_SIZE + 1),
        ));

        let mut put_data = PutData::new();
        put_data.add_key_column("k1", boolv).unwrap();

        let mut batch =
            write_batch_util::new_write_batch(&[("k1", LogicalTypeId::Boolean, false)], None);
        let err = batch.put(put_data).err().unwrap();
        check_err(err, "Request is too large");
    }

    #[test]
    fn test_put_data_duplicate() {
        let intv = Arc::new(UInt64Vector::from_slice(&[1, 2, 3]));

        let mut put_data = PutData::new();
        put_data.add_key_column("k1", intv.clone()).unwrap();
        let err = put_data.add_key_column("k1", intv).err().unwrap();
        check_err(err, "Duplicate column k1");
    }

    #[test]
    fn test_put_data_different_len() {
        let intv = Arc::new(UInt64Vector::from_slice(&[1, 2, 3]));
        let boolv = Arc::new(BooleanVector::from(vec![true, false]));

        let mut put_data = PutData::new();
        put_data.add_key_column("k1", intv).unwrap();
        let err = put_data.add_value_column("v1", boolv).err().unwrap();
        check_err(err, "Length of column v1 not equals");
    }

    #[test]
    fn test_put_type_mismatch() {
        let boolv = Arc::new(BooleanVector::from(vec![true, false, true]));
        let tsv = Arc::new(Int64Vector::from_vec(vec![0, 0, 0]));

        let mut put_data = PutData::new();
        put_data.add_key_column("k1", boolv).unwrap();
        put_data.add_key_column("ts", tsv).unwrap();

        let mut batch = new_test_batch();
        let err = batch.put(put_data).err().unwrap();
        check_err(err, "Type of column k1 does not match");
    }

    #[test]
    fn test_put_type_has_null() {
        let intv = Arc::new(UInt64Vector::from_iter(&[Some(1), None, Some(3)]));
        let tsv = Arc::new(Int64Vector::from_vec(vec![0, 0, 0]));

        let mut put_data = PutData::new();
        put_data.add_key_column("k1", intv).unwrap();
        put_data.add_key_column("ts", tsv).unwrap();

        let mut batch = new_test_batch();
        let err = batch.put(put_data).err().unwrap();
        check_err(err, "Column k1 is not null");
    }

    #[test]
    fn test_put_missing_column() {
        let boolv = Arc::new(BooleanVector::from(vec![true, false, true]));
        let tsv = Arc::new(Int64Vector::from_vec(vec![0, 0, 0]));

        let mut put_data = PutData::new();
        put_data.add_key_column("v1", boolv).unwrap();
        put_data.add_key_column("ts", tsv).unwrap();
        let mut batch = new_test_batch();
        let err = batch.put(put_data).err().unwrap();
        check_err(err, "Missing column k1");
    }

    #[test]
    fn test_put_unknown_column() {
        let intv = Arc::new(UInt64Vector::from_slice(&[1, 2, 3]));
        let tsv = Arc::new(Int64Vector::from_vec(vec![0, 0, 0]));
        let boolv = Arc::new(BooleanVector::from(vec![true, false, true]));

        let mut put_data = PutData::new();
        put_data.add_key_column("k1", intv.clone()).unwrap();
        put_data.add_version_column(intv).unwrap();
        put_data.add_value_column("v1", boolv.clone()).unwrap();
        put_data.add_key_column("ts", tsv).unwrap();
        put_data.add_value_column("v2", boolv).unwrap();
        let mut batch = new_test_batch();
        let err = batch.put(put_data).err().unwrap();
        check_err(err, "Unknown column v2");
    }

    #[test]
    pub fn test_align_timestamp() {
        let duration_millis = 20;
        let ts = [-21, -20, -19, -1, 0, 5, 15, 19, 20, 21];
        let res = ts.map(|t| align_timestamp(t, duration_millis));
        assert_eq!(res, [-40, -20, -20, -20, 0, 0, 0, 0, 20, 20].map(Some));
    }

    #[test]
    pub fn test_align_timestamp_overflow() {
        assert_eq!(Some(i64::MIN), align_timestamp(i64::MIN, 1));
        assert_eq!(None, align_timestamp(i64::MIN, 2));
    }

    #[test]
    pub fn test_write_batch_time_range() {
        let intv = Arc::new(UInt64Vector::from_slice(&[1, 2, 3, 4, 5, 6]));
        let tsv = Arc::new(Int64Vector::from_vec(vec![-21, -20, -1, 0, 1, 20]));
        let boolv = Arc::new(BooleanVector::from(vec![
            true, false, true, false, false, false,
        ]));

        let mut put_data = PutData::new();
        put_data.add_key_column("k1", intv.clone()).unwrap();
        put_data.add_version_column(intv).unwrap();
        put_data.add_value_column("v1", boolv).unwrap();
        put_data.add_key_column("ts", tsv).unwrap();

        let mut batch = new_test_batch();
        batch.put(put_data).unwrap();

        let duration_millis = 20i64;
        let ranges = batch
            .time_ranges(Duration::from_millis(duration_millis as u64))
            .unwrap();
        assert_eq!(
            [-40, -20, 0, 20].map(|v| RangeMillis::new(v, v + duration_millis).unwrap()),
            ranges.as_slice()
        )
    }
}
