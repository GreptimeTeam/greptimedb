use std::collections::HashMap;
use std::sync::Arc;

use datatypes::value::Value;
use datatypes::vectors::VectorBuilder;
use datatypes::vectors::VectorRef;
use snafu::OptionExt;
use table::requests::InsertRequest;

use crate::error::FindPartitionColumnSnafu;
use crate::error::FindRegionSnafu;
use crate::error::Result;
use crate::partition::PartitionRule;

pub type RegionId = u64;
pub type ColumnName = String;
pub type ValueList = Vec<Value>;
pub type DistInsertRequest = HashMap<RegionId, InsertRequest>;

pub struct WriteSpliter<'a, P> {
    partition_rule: &'a P,
}

impl<'a, P> WriteSpliter<'a, P>
where
    P: PartitionRule,
{
    pub fn with_patition_rule(rule: &'a P) -> Self {
        Self {
            partition_rule: rule,
        }
    }

    pub fn split(&self, insert: InsertRequest) -> Result<DistInsertRequest> {
        let column_names = self.partition_rule.partition_columns();
        let partition_columns = partition_columns(&insert, &column_names)?;
        let region_map = self.region_map(&partition_columns)?;

        Ok(partition_insert_request(&insert, region_map))
    }

    fn region_map(&self, partition_columns: &[VectorRef]) -> Result<HashMap<RegionId, Vec<usize>>> {
        if partition_columns.is_empty() {
            return Ok(HashMap::default());
        }
        let mut region_map: HashMap<RegionId, Vec<usize>> = HashMap::new();
        let row_count = partition_columns[0].len();
        for idx in 0..row_count {
            let region_id = match self
                .partition_rule
                .find_region(&partition_values(partition_columns, idx))
            {
                Ok(region_id) => region_id,
                Err(e) => {
                    let reason = format!("{:?}", e);
                    return FindRegionSnafu { reason }.fail();
                }
            };
            region_map
                .entry(region_id)
                .or_insert_with(Vec::default)
                .push(idx);
        }
        Ok(region_map)
    }
}

fn partition_columns(insert: &InsertRequest, column_names: &[String]) -> Result<Vec<VectorRef>> {
    let values = &insert.columns_values;
    let mut column_vec = Vec::with_capacity(column_names.len());

    for column_name in column_names {
        let column = values
            .get(column_name)
            .map(Arc::clone)
            .context(FindPartitionColumnSnafu { column_name })?;
        column_vec.push(column);
    }
    Ok(column_vec)
}

fn partition_values(partition_columns: &[VectorRef], idx: usize) -> ValueList {
    partition_columns
        .iter()
        .map(|column| column.get(idx))
        .collect()
}

fn partition_insert_request(
    insert: &InsertRequest,
    region_map: HashMap<RegionId, Vec<usize>>,
) -> DistInsertRequest {
    let mut dist_insert: HashMap<RegionId, HashMap<String, VectorBuilder>> =
        HashMap::with_capacity(region_map.len());

    let column_count = insert.columns_values.len();
    for (column_name, vector) in &insert.columns_values {
        for (region_id, val_idxs) in &region_map {
            let region_insert = dist_insert
                .entry(*region_id)
                .or_insert_with(|| HashMap::with_capacity(column_count));
            let builder = region_insert
                .entry(column_name.to_string())
                .or_insert_with(|| VectorBuilder::new(vector.data_type()));
            val_idxs
                .iter()
                .for_each(|idx| builder.push(&vector.get(*idx)));
        }
    }

    let table_name = &insert.table_name;
    dist_insert
        .into_iter()
        .map(|(region_id, vector_map)| {
            let columns_values = vector_map
                .into_iter()
                .map(|(column_name, mut builder)| (column_name, builder.finish()))
                .collect();
            (
                region_id,
                InsertRequest {
                    table_name: table_name.to_string(),
                    columns_values,
                },
            )
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, result::Result};

    use datatypes::{
        data_type::ConcreteDataType,
        types::{BooleanType, StringType},
        value::Value,
        vectors::VectorBuilder,
    };
    use table::requests::InsertRequest;

    use super::{
        partition_columns, partition_insert_request, partition_values, ColumnName, PartitionRule,
        RegionId, WriteSpliter,
    };

    #[test]
    fn test_writer_spliter() {
        let insert = mock_insert_request();
        let spliter = WriteSpliter::with_patition_rule(&MockPartitionRule);
        let ret = spliter.split(insert).unwrap();

        assert_eq!(2, ret.len());

        let r1_insert = ret.get(&0).unwrap();
        let r2_insert = ret.get(&1).unwrap();

        assert_eq!("demo", r1_insert.table_name);
        assert_eq!("demo", r2_insert.table_name);

        let r1_columns = &r1_insert.columns_values;
        assert_eq!(3, r1_columns.len());
        assert_eq!(
            <i16 as Into<Value>>::into(1),
            r1_columns.get("id").unwrap().get(0)
        );
        assert_eq!(
            <&str as Into<Value>>::into("host1"),
            r1_columns.get("host").unwrap().get(0)
        );
        assert_eq!(
            <bool as Into<Value>>::into(true),
            r1_columns.get("enable_reboot").unwrap().get(0)
        );

        let r2_columns = &r2_insert.columns_values;
        assert_eq!(3, r2_columns.len());
        assert_eq!(
            <i16 as Into<Value>>::into(2),
            r2_columns.get("id").unwrap().get(0)
        );
        assert_eq!(
            <i16 as Into<Value>>::into(3),
            r2_columns.get("id").unwrap().get(1)
        );
        assert_eq!(Value::Null, r2_columns.get("host").unwrap().get(0));
        assert_eq!(
            <&str as Into<Value>>::into("host3"),
            r2_columns.get("host").unwrap().get(1)
        );
        assert_eq!(
            <bool as Into<Value>>::into(false),
            r2_columns.get("enable_reboot").unwrap().get(0)
        );
        assert_eq!(
            <bool as Into<Value>>::into(true),
            r2_columns.get("enable_reboot").unwrap().get(1)
        );
    }

    #[test]
    fn test_partition_insert_request() {
        let insert = mock_insert_request();
        let mut region_map: HashMap<RegionId, Vec<usize>> = HashMap::with_capacity(2);
        region_map.insert(1, vec![2, 0]);
        region_map.insert(2, vec![1]);

        let dist_insert = partition_insert_request(&insert, region_map);

        let r1_insert = dist_insert.get(&1_u64).unwrap();
        assert_eq!("demo", r1_insert.table_name);
        let expected: Value = 3_i16.into();
        assert_eq!(expected, r1_insert.columns_values.get("id").unwrap().get(0));
        let expected: Value = 1_i16.into();
        assert_eq!(expected, r1_insert.columns_values.get("id").unwrap().get(1));
        let expected: Value = "host3".into();
        assert_eq!(
            expected,
            r1_insert.columns_values.get("host").unwrap().get(0)
        );
        let expected: Value = "host1".into();
        assert_eq!(
            expected,
            r1_insert.columns_values.get("host").unwrap().get(1)
        );
        let expected: Value = true.into();
        assert_eq!(
            expected,
            r1_insert
                .columns_values
                .get("enable_reboot")
                .unwrap()
                .get(0)
        );
        let expected: Value = true.into();
        assert_eq!(
            expected,
            r1_insert
                .columns_values
                .get("enable_reboot")
                .unwrap()
                .get(1)
        );

        let r2_insert = dist_insert.get(&2_u64).unwrap();
        assert_eq!("demo", r2_insert.table_name);
        let expected: Value = 2_i16.into();
        assert_eq!(expected, r2_insert.columns_values.get("id").unwrap().get(0));
        assert_eq!(
            Value::Null,
            r2_insert.columns_values.get("host").unwrap().get(0)
        );
        let expected: Value = false.into();
        assert_eq!(
            expected,
            r2_insert
                .columns_values
                .get("enable_reboot")
                .unwrap()
                .get(0)
        );
    }

    #[test]
    fn test_partition_columns() {
        let insert = mock_insert_request();

        let partition_column_names = vec!["host".to_string(), "id".to_string()];
        let columns = partition_columns(&insert, &partition_column_names).unwrap();

        let host_column = columns[0].clone();
        assert_eq!(
            ConcreteDataType::String(StringType),
            host_column.data_type()
        );
        assert_eq!(1, host_column.null_count());

        let id_column = columns[1].clone();
        assert_eq!(ConcreteDataType::int16_datatype(), id_column.data_type());
        assert_eq!(0, id_column.null_count());
    }

    #[test]
    fn test_partition_values() {
        let mut builder = VectorBuilder::new(ConcreteDataType::Boolean(BooleanType));
        builder.push(&true.into());
        builder.push(&false.into());
        builder.push(&true.into());
        let v1 = builder.finish();

        let mut builder = VectorBuilder::new(ConcreteDataType::String(StringType));
        builder.push(&"host1".into());
        builder.push_null();
        builder.push(&"host3".into());
        let v2 = builder.finish();

        let vectors = vec![v1, v2];

        let row_0_vals = partition_values(&vectors, 0);
        let expeted: Vec<Value> = vec![true.into(), "host1".into()];
        assert_eq!(expeted, row_0_vals);

        let row_1_vals = partition_values(&vectors, 1);
        let expeted: Vec<Value> = vec![false.into(), Value::Null];
        assert_eq!(expeted, row_1_vals);

        let row_2_vals = partition_values(&vectors, 2);
        let expeted: Vec<Value> = vec![true.into(), "host3".into()];
        assert_eq!(expeted, row_2_vals);
    }

    fn mock_insert_request() -> InsertRequest {
        let mut columns_values = HashMap::with_capacity(4);
        let mut builder = VectorBuilder::new(ConcreteDataType::Boolean(BooleanType));
        builder.push(&true.into());
        builder.push(&false.into());
        builder.push(&true.into());
        columns_values.insert("enable_reboot".to_string(), builder.finish());

        let mut builder = VectorBuilder::new(ConcreteDataType::String(StringType));
        builder.push(&"host1".into());
        builder.push_null();
        builder.push(&"host3".into());
        columns_values.insert("host".to_string(), builder.finish());

        let mut builder = VectorBuilder::new(ConcreteDataType::int16_datatype());
        builder.push(&1_i16.into());
        builder.push(&2_i16.into());
        builder.push(&3_i16.into());
        columns_values.insert("id".to_string(), builder.finish());

        InsertRequest {
            table_name: "demo".to_string(),
            columns_values,
        }
    }

    struct MockPartitionRule;

    // PARTITION BY LIST COLUMNS(id) (
    //     PARTITION r0 VALUES IN(1),
    //     PARTITION r1 VALUES IN(2, 3),
    // );
    impl PartitionRule for MockPartitionRule {
        type Error = String;

        fn partition_columns(&self) -> Vec<ColumnName> {
            vec!["id".to_string()]
        }

        fn find_region(&self, values: &super::ValueList) -> Result<RegionId, Self::Error> {
            let val = values.get(0).unwrap().to_owned();
            let id_1: Value = 1_i16.into();
            let id_2: Value = 2_i16.into();
            let id_3: Value = 3_i16.into();
            if val == id_1 {
                return Ok(0);
            }
            if val == id_2 || val == id_3 {
                return Ok(1);
            }
            unreachable!()
        }
    }
}
