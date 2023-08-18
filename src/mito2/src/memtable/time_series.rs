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

use std::collections::{BTreeMap, Bound};
use std::fmt::{Debug, Formatter};
use std::ops::RangeFull;
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, RwLock};

use api::v1::OpType;
use common_telemetry::info;
use datatypes::arrow;
use datatypes::arrow::row::RowConverter;
use datatypes::data_type::DataType;
use datatypes::prelude::{MutableVector, ScalarVectorBuilder, Vector, VectorRef};
use datatypes::value::ValueRef;
use datatypes::vectors::{
    Helper, UInt64Vector, UInt64VectorBuilder, UInt8Vector, UInt8VectorBuilder,
};
use store_api::metadata::RegionMetadataRef;
use store_api::storage::ScanRequest;

use crate::error;
use crate::memtable::{BoxedBatchIterator, KeyValues, Memtable, MemtableId};
use crate::read::{Batch, BatchBuilder, BatchColumn};
use crate::row_converter::{McmpRowCodec, RowCodec, SortField};

pub struct TimeSeriesMemtable {
    id: MemtableId,
    region_metadata: RegionMetadataRef,
    row_codec: McmpRowCodec,
    series_set: SeriesSet,
    allocated: AtomicUsize,
}

impl TimeSeriesMemtable {
    pub fn new(region_metadata: RegionMetadataRef, id: MemtableId) -> Self {
        let row_codec = McmpRowCodec::new(
            region_metadata
                .primary_key_columns()
                .map(|c| SortField::new(c.column_schema.data_type.clone()))
                .collect(),
        );

        let series_set = SeriesSet::new(region_metadata.clone(), 10);
        Self {
            id,
            region_metadata,
            series_set,
            row_codec,
            allocated: Default::default(),
        }
    }
}

impl Debug for TimeSeriesMemtable {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TimeSeriesMemtable").finish()
    }
}

impl Memtable for TimeSeriesMemtable {
    fn id(&self) -> MemtableId {
        self.id
    }

    fn write(&self, kvs: &KeyValues) -> crate::error::Result<()> {
        for kv in kvs.iter() {
            assert_eq!(kv.num_primary_keys(), self.row_codec.num_fields());
            let primary_key_encoded = self.row_codec.encode(kv.primary_keys()).unwrap();
            let fields = kv.fields().collect();
            let series = self.series_set.get_or_add_series(primary_key_encoded);
            let mut guard = series.write().unwrap();
            guard.push(kv.timestamp(), kv.sequence(), kv.op_type(), fields);
        }
        Ok(())
    }

    fn iter(&self, req: ScanRequest) -> BoxedBatchIterator {
        let _projection = req.projection.map(|p| {
            p.iter()
                .map(|idx| self.region_metadata.column_metadatas[*idx].column_id)
                .collect::<Vec<_>>()
        });

        Box::new(self.series_set.iter_series())
    }
}

type BucketIndex = (u8, usize);

struct SeriesSet {
    region_metadata: RegionMetadataRef,
    bucket_num: u8,
    indices: Arc<RwLock<BTreeMap<Vec<u8>, BucketIndex>>>,
    buckets: Arc<Vec<Bucket>>,
}

impl SeriesSet {
    fn new(region_metadata: RegionMetadataRef, bucket_num: u8) -> Self {
        let buckets = Arc::new(
            (0..bucket_num)
                .map(|_| Bucket::new(region_metadata.clone()))
                .collect(),
        );

        Self {
            region_metadata,
            bucket_num,
            indices: Default::default(),
            buckets,
        }
    }
}

impl SeriesSet {
    fn get_or_add_series(&self, primary_key: Vec<u8>) -> Arc<RwLock<Series>> {
        let mut indices = self.indices.write().unwrap();
        if let Some(idx) = indices.get(&primary_key) {
            self.buckets[idx.0 as usize].get_series(idx.1).unwrap()
        } else {
            let bucket_idx = self.hash_primary_key(&primary_key);
            let bucket = &self.buckets[bucket_idx as usize];
            let s = Arc::new(RwLock::new(Series::new(self.region_metadata.clone())));
            let series_index = bucket.add_series(s.clone());
            // let mut indices = self.indices.write().unwrap();
            indices.insert(primary_key, (bucket_idx, series_index));
            s
        }
    }

    fn iter_series(&self) -> Iter {
        Iter {
            metadata: self.region_metadata.clone(),
            indices: self.indices.clone(),
            buckets: self.buckets.clone(),
            last_key: None,
        }
    }

    fn hash_primary_key(&self, primary_key: &[u8]) -> u8 {
        let x: u32 = primary_key.iter().map(|v| *v as u32).sum();
        (x % self.bucket_num as u32) as u8
    }
}

struct Iter {
    metadata: RegionMetadataRef,
    indices: Arc<RwLock<BTreeMap<Vec<u8>, BucketIndex>>>,
    buckets: Arc<Vec<Bucket>>,
    last_key: Option<Vec<u8>>,
}

impl Iterator for Iter {
    type Item = error::Result<Batch>;

    fn next(&mut self) -> Option<Self::Item> {
        let map = self.indices.read().unwrap();
        let mut range = match &self.last_key {
            None => {
                info!("First range, range full");
                map.range::<Vec<u8>, RangeFull>(..)
            }
            Some(last_key) => {
                info!("range from {:02X?}", last_key);
                map.range::<Vec<u8>, _>((Bound::Excluded(last_key), Bound::Unbounded))
            }
        };

        if let Some((primary_key, bucket)) = range.next() {
            info!("Primary key: {:02X?}", primary_key);
            self.last_key = Some(primary_key.to_vec());
            let Some(b) = self.buckets.get(bucket.0 as usize) else {
                panic!()
            };
            let series = b.get_series(bucket.1).unwrap();
            let values = series.write().unwrap().snapshot();
            Some(Ok(values.to_batch(primary_key, &self.metadata)))
        } else {
            None
        }
    }
}

struct Bucket {
    region_metadata: RegionMetadataRef,
    series: RwLock<Vec<Arc<RwLock<Series>>>>,
}

impl Bucket {
    fn new(region_metadata: RegionMetadataRef) -> Self {
        Self {
            region_metadata,
            series: Default::default(),
        }
    }

    /// Returns the series at given index.
    /// Returns None if series not found.
    #[inline]
    fn get_series(&self, idx: usize) -> Option<Arc<RwLock<Series>>> {
        self.series.read().unwrap().get(idx).cloned()
    }

    /// Adds series to bucket and returns the index inside the bucket.
    #[inline]
    fn add_series(&self, s: Arc<RwLock<Series>>) -> usize {
        let mut series = self.series.write().unwrap();
        let idx = series.len();
        series.push(s);
        idx
    }
}

struct Series {
    region_metadata: RegionMetadataRef,
    active: ValueBuilder,
    frozen: Vec<Values>,
}

impl Series {
    fn new(region_metadata: RegionMetadataRef) -> Self {
        Self {
            region_metadata: region_metadata.clone(),
            active: ValueBuilder::new(region_metadata, 32),
            frozen: vec![],
        }
    }

    /// Pushes a row of values into Series.
    fn push(&mut self, ts: ValueRef, sequence: u64, op_type: OpType, values: Vec<ValueRef>) {
        self.active.push(ts, sequence, op_type as u8, values);
    }

    fn freeze(&mut self) {
        let mut builder = ValueBuilder::new(self.region_metadata.clone(), 32);
        std::mem::swap(&mut self.active, &mut builder);
        self.frozen.push(Values::from(builder));
    }

    fn snapshot(&mut self) -> Values {
        self.freeze();

        let values = self.frozen.clone();
        let total_len: usize = values.iter().map(|v| v.timestamp.len()).sum();
        let mut builder = ValueBuilder::new(self.region_metadata.clone(), total_len);

        for v in values {
            let len = v.timestamp.len();
            builder
                .timestamp
                .extend_slice_of(&*v.timestamp, 0, len)
                .unwrap();
            builder
                .sequence
                .extend_slice_of(&*v.sequence, 0, len)
                .unwrap();

            builder
                .op_type
                .extend_slice_of(&*v.op_type, 0, len)
                .unwrap();

            for (idx, f) in v.fields.iter().enumerate() {
                builder.fields[idx].extend_slice_of(&**f, 0, len).unwrap();
            }
        }

        let values = Values::from(builder);
        self.frozen = vec![values.clone()];

        values
    }
}

struct ValueBuilder {
    region_metadata: RegionMetadataRef,
    timestamp: Box<dyn MutableVector>,
    sequence: UInt64VectorBuilder,
    op_type: UInt8VectorBuilder,
    fields: Vec<Box<dyn MutableVector>>,
}

impl ValueBuilder {
    fn new(region_metadata: RegionMetadataRef, capacity: usize) -> Self {
        let timestamp = region_metadata
            .time_index_column()
            .column_schema
            .data_type
            .create_mutable_vector(capacity);
        let sequence = UInt64VectorBuilder::with_capacity(capacity);
        let op_type = UInt8VectorBuilder::with_capacity(capacity);

        let fields = region_metadata
            .field_columns()
            .map(|c| c.column_schema.data_type.create_mutable_vector(capacity))
            .collect();

        Self {
            region_metadata,
            timestamp,
            sequence,
            op_type,
            fields,
        }
    }

    fn push(&mut self, ts: ValueRef, sequence: u64, op_type: u8, fields: Vec<ValueRef>) {
        debug_assert_eq!(fields.len(), self.fields.len());
        self.timestamp.push_value_ref(ts);
        self.sequence.push_value_ref(ValueRef::UInt64(sequence));
        self.op_type.push_value_ref(ValueRef::UInt8(op_type));
        for (idx, field_value) in fields.into_iter().enumerate() {
            self.fields[idx].push_value_ref(field_value);
        }
    }
}

#[derive(Clone)]
struct Values {
    timestamp: VectorRef,
    sequence: Arc<UInt64Vector>,
    op_type: Arc<UInt8Vector>,
    fields: Vec<VectorRef>,
}

impl Values {
    /// Sorts values in place by `timestamp, sequence, op_type`.
    fn sort_in_place(&mut self) -> error::Result<()> {
        let mut arrays = Vec::with_capacity(3 + self.fields.len());
        arrays.push(self.timestamp.to_arrow_array());
        arrays.push(self.sequence.to_arrow_array());
        arrays.push(self.op_type.to_arrow_array());
        arrays.extend(self.fields.iter().map(|f| f.to_arrow_array()));

        let fields = arrays
            .iter()
            .map(|arr| datatypes::arrow::row::SortField::new(arr.data_type().clone()))
            .collect();
        let mut converter = RowConverter::new(fields).unwrap();
        let rows = converter.convert_columns(&arrays).unwrap();
        let mut sort_pairs = rows.iter().enumerate().collect::<Vec<_>>();
        sort_pairs.sort_unstable_by(|(_, a), (_, b)| a.cmp(b));
        let indices = datatypes::arrow::array::UInt32Array::from_iter_values(
            sort_pairs.iter().map(|(i, _)| *i as u32),
        );

        let res = arrays
            .into_iter()
            .map(|arr| arrow::compute::take(&arr, &indices, None))
            .collect::<arrow::error::Result<Vec<_>>>()
            .unwrap();

        self.timestamp = Helper::try_into_vector(&res[0]).unwrap();
        self.sequence = Arc::new(UInt64Vector::try_from_arrow_array(&res[1]).unwrap());
        self.op_type = Arc::new(UInt8Vector::try_from_arrow_array(&res[2]).unwrap());
        self.fields = Helper::try_into_vectors(&res[3..]).unwrap();
        Ok(())
    }

    pub fn to_batch(&self, primary_key: &[u8], metadata: &RegionMetadataRef) -> Batch {
        let builder = BatchBuilder::with_required_columns(
            primary_key.to_vec(),
            self.timestamp.clone(),
            self.sequence.clone(),
            self.op_type.clone(),
        );

        let fields = metadata
            .field_columns()
            .zip(self.fields.iter())
            .map(|(c, f)| BatchColumn {
                column_id: c.column_id,
                data: f.clone(),
            })
            .collect();

        builder.with_fields(fields).build().unwrap()
    }
}

impl From<ValueBuilder> for Values {
    fn from(mut value: ValueBuilder) -> Self {
        let fields = value
            .fields
            .iter_mut()
            .map(|v| v.to_vector())
            .collect::<Vec<_>>();
        let sequence = Arc::new(value.sequence.finish());
        let op_type = Arc::new(value.op_type.finish());
        let timestamp = value.timestamp.to_vector();

        if cfg!(debug_assertions) {
            debug_assert_eq!(timestamp.len(), sequence.len());
            debug_assert_eq!(timestamp.len(), op_type.len());
            for field in &fields {
                debug_assert_eq!(timestamp.len(), field.len());
            }
        }

        Self {
            timestamp,
            sequence,
            op_type,
            fields,
        }
    }
}

#[cfg(test)]
mod tests {
    use api::helper::ColumnDataTypeWrapper;
    use api::v1::value::ValueData;
    use api::v1::{Row, Rows, SemanticType};
    use common_time::Timestamp;
    use datatypes::prelude::{ConcreteDataType, ScalarVector};
    use datatypes::schema::ColumnSchema;
    use datatypes::value::{OrderedFloat, Value};
    use datatypes::vectors::{
        Float32Vector, Float64Vector, Int64Vector, TimestampMillisecondVector,
    };
    use store_api::metadata::{ColumnMetadata, RegionMetadataBuilder};
    use store_api::storage::RegionId;

    use super::*;

    fn schema_for_test() -> RegionMetadataRef {
        let mut builder = RegionMetadataBuilder::new(RegionId::new(123, 456));
        builder
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new("k0", ConcreteDataType::string_datatype(), false),
                semantic_type: SemanticType::Tag,
                column_id: 0,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new("k1", ConcreteDataType::int64_datatype(), false),
                semantic_type: SemanticType::Tag,
                column_id: 1,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "ts",
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    false,
                ),
                semantic_type: SemanticType::Timestamp,
                column_id: 2,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new("v0", ConcreteDataType::int64_datatype(), true),
                semantic_type: SemanticType::Field,
                column_id: 3,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new("v1", ConcreteDataType::float64_datatype(), true),
                semantic_type: SemanticType::Field,
                column_id: 4,
            })
            .primary_key(vec![0, 1]);
        let region_metadata = builder.build().unwrap();
        Arc::new(region_metadata)
    }

    fn ts_value_ref(val: i64) -> ValueRef<'static> {
        ValueRef::Timestamp(Timestamp::new_millisecond(val))
    }

    fn field_value_ref(v0: i64, v1: f64) -> Vec<ValueRef<'static>> {
        vec![ValueRef::Int64(v0), ValueRef::Float64(OrderedFloat(v1))]
    }

    fn check_values(values: Values, expect: &[(i64, u64, u8, i64, f64)]) {
        let ts = values
            .timestamp
            .as_any()
            .downcast_ref::<TimestampMillisecondVector>()
            .unwrap();

        let v0 = values.fields[0]
            .as_any()
            .downcast_ref::<Int64Vector>()
            .unwrap();
        let v1 = values.fields[1]
            .as_any()
            .downcast_ref::<Float64Vector>()
            .unwrap();
        let read = ts
            .iter_data()
            .zip(values.sequence.iter_data())
            .zip(values.op_type.iter_data())
            .zip(v0.iter_data())
            .zip(v1.iter_data())
            .map(|((((ts, sequence), op_type), v0), v1)| {
                (
                    ts.unwrap().0.value(),
                    sequence.unwrap(),
                    op_type.unwrap(),
                    v0.unwrap(),
                    v1.unwrap(),
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(expect, &read);
    }

    #[test]
    fn test_series() {
        let region_metadata = schema_for_test();
        let mut series = Series::new(region_metadata);
        series.push(ts_value_ref(1), 0, OpType::Put, field_value_ref(1, 10.1));
        series.push(ts_value_ref(2), 0, OpType::Put, field_value_ref(2, 10.2));
        assert_eq!(2, series.active.timestamp.len());
        assert_eq!(0, series.frozen.len());

        let values = series.snapshot();
        check_values(values, &[(1, 0, 1, 1, 10.1), (2, 0, 1, 2, 10.2)]);
        assert_eq!(0, series.active.timestamp.len());
        assert_eq!(1, series.frozen.len());
    }

    fn check_value(values: &Values, expect: Vec<Vec<Value>>) {
        assert_eq!(values.sequence.len(), values.timestamp.len());
        assert_eq!(values.op_type.len(), values.timestamp.len());
        for f in &values.fields {
            assert_eq!(f.len(), values.timestamp.len());
        }

        let mut rows = vec![];
        for idx in 0..values.timestamp.len() {
            let mut row = Vec::with_capacity(values.fields.len() + 3);
            row.push(values.timestamp.get(idx));
            row.push(values.sequence.get(idx));
            row.push(values.op_type.get(idx));
            row.extend(values.fields.iter().map(|f| f.get(idx)));
            rows.push(row);
        }

        assert_eq!(expect.len(), rows.len());
        for (idx, row) in rows.iter().enumerate() {
            assert_eq!(&expect[idx], row);
        }
    }

    #[test]
    fn test_values_sort() {
        let timestamp = Arc::new(TimestampMillisecondVector::from_vec(vec![1, 2, 4, 3]));
        let sequence = Arc::new(UInt64Vector::from_vec(vec![1, 1, 1, 0]));
        let op_type = Arc::new(UInt8Vector::from_vec(vec![1, 1, 1, 1]));

        let fields = vec![Arc::new(Float32Vector::from_vec(vec![1.1, 2.1, 3.3, 4.2])) as Arc<_>];
        let mut values = Values {
            timestamp: timestamp as Arc<_>,
            sequence,
            op_type,
            fields,
        };
        values.sort_in_place().unwrap();

        check_value(
            &values,
            vec![
                vec![
                    Value::Timestamp(Timestamp::new_millisecond(1)),
                    Value::UInt64(1),
                    Value::UInt8(1),
                    Value::Float32(OrderedFloat(1.1)),
                ],
                vec![
                    Value::Timestamp(Timestamp::new_millisecond(2)),
                    Value::UInt64(1),
                    Value::UInt8(1),
                    Value::Float32(OrderedFloat(2.1)),
                ],
                vec![
                    Value::Timestamp(Timestamp::new_millisecond(3)),
                    Value::UInt64(0),
                    Value::UInt8(1),
                    Value::Float32(OrderedFloat(4.2)),
                ],
                vec![
                    Value::Timestamp(Timestamp::new_millisecond(4)),
                    Value::UInt64(1),
                    Value::UInt8(1),
                    Value::Float32(OrderedFloat(3.3)),
                ],
            ],
        )
    }

    fn build_key_values(schema: &RegionMetadataRef) -> KeyValues {
        let column_schema = schema
            .column_metadatas
            .iter()
            .map(|c| api::v1::ColumnSchema {
                column_name: c.column_schema.name.clone(),
                datatype: ColumnDataTypeWrapper::try_from(c.column_schema.data_type.clone())
                    .unwrap()
                    .datatype() as i32,
                semantic_type: c.semantic_type as i32,
            })
            .collect();

        let mutation = api::v1::Mutation {
            op_type: 1,
            sequence: 0,
            rows: Some(Rows {
                schema: column_schema,
                rows: vec![
                    Row {
                        values: vec![
                            api::v1::Value {
                                value_data: Some(ValueData::StringValue("1".to_string())),
                            },
                            api::v1::Value {
                                value_data: Some(ValueData::I64Value(1)),
                            },
                            api::v1::Value {
                                value_data: Some(ValueData::TsMillisecondValue(1)),
                            },
                            api::v1::Value {
                                value_data: Some(ValueData::I64Value(1)),
                            },
                            api::v1::Value {
                                value_data: Some(ValueData::F64Value(1.0)),
                            },
                        ],
                    },
                    Row {
                        values: vec![
                            api::v1::Value {
                                value_data: Some(ValueData::StringValue("1".to_string())),
                            },
                            api::v1::Value {
                                value_data: Some(ValueData::I64Value(1)),
                            },
                            api::v1::Value {
                                value_data: Some(ValueData::TsMillisecondValue(2)),
                            },
                            api::v1::Value {
                                value_data: Some(ValueData::I64Value(2)),
                            },
                            api::v1::Value {
                                value_data: Some(ValueData::F64Value(2.0)),
                            },
                        ],
                    },
                ],
            }),
        };
        KeyValues::new(schema.as_ref(), mutation).unwrap()
    }

    #[test]
    fn test_memtable() {
        common_telemetry::init_default_ut_logging();
        let schema = schema_for_test();
        let kvs = build_key_values(&schema);
        let memtable = TimeSeriesMemtable::new(schema, 42);

        memtable.write(&kvs).unwrap();

        let mut x = memtable.iter(ScanRequest::default());
        let batch = x.next().unwrap().unwrap();
        println!("batch: {:?}", batch);
    }
}
