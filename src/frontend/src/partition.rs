use std::fmt::Debug;

use datatypes::prelude::Value;
use snafu::ensure;

use crate::error::{self, Result};
use crate::spliter::{ColumnName, RegionId, ValueList};

pub trait PartitionRule {
    type Error: Debug;

    fn partition_columns(&self) -> Vec<ColumnName>;

    fn find_region(&self, values: &ValueList) -> std::result::Result<RegionId, Self::Error>;

    // TODO(fys): there maybe other method
}

/// A [PartitionRule] contains all mappings of partition range to region.
///
/// It's generated from create table request, using MySQL's syntax:
///
/// ```SQL
/// CREATE TABLE table_name
/// PARTITION BY RANGE COLUMNS(column_list) (
///     PARTITION region_name VALUES LESS THAN (value_list)[,
///     PARTITION region_name VALUES LESS THAN (value_list)][,
///     ...]
/// )
///
/// column_list:
///     column_name[, column_name][, ...]
///
/// value_list:
///     value[, value][, ...]
/// ```
///
/// Please refer to MySQL's [document](https://dev.mysql.com/doc/refman/8.0/en/partitioning-columns-range.html)
/// for more details.
// FIXME(LFC): no allow
#[allow(unused)]
struct RangePartitionRule {
    column_list: Vec<String>,
    // Does not store the last "MAXVALUE" bound because it can make our binary search in finding regions easier
    // (besides, it's hard to represent "MAXVALUE" in our `Value`).
    // So the length of `value_lists` is one less than `regions`.
    value_lists: Vec<ValueList>,
    regions: Vec<u64>,
}

// FIXME(LFC): no allow
#[allow(unused)]
impl RangePartitionRule {
    /// Creates a new partition rule. `value_lists` must be strictly increased (because we are
    /// using binary search to get a determined result on it).
    ///
    /// # Panics
    ///   Elements in `value_lists` are not strictly increased.
    // FIXME(LFC): Use the metadata of region that are retrieved from Meta to directly create partition rule.
    fn new(column_list: Vec<String>, value_lists: Vec<ValueList>, regions: Vec<u64>) -> Self {
        assert!(
            value_lists.as_slice().windows(2).all(|w| w[0] < w[1]),
            "`value_lists` in partition rule must be strictly increased!"
        );

        // Will be eliminated once we resolve the above "FIXME".
        assert_eq!(
            regions.len(),
            value_lists.len() + 1,
            "Length of `value_lists` must be one less than `regions`"
        );
        assert!(value_lists.iter().all(|x| x.len() == column_list.len()));

        Self {
            column_list,
            value_lists,
            regions,
        }
    }

    fn find_region(&self, region_keys: &Vec<Value>) -> Result<u64> {
        ensure!(
            region_keys.len() == self.column_list.len(),
            error::RegionKeysSizeSnafu {
                expect: self.column_list.len(),
                actual: region_keys.len(),
            }
        );

        // How tuple is compared:
        // (a, b) < (x, y) <= (a < x) || ((a == x) && (b < y))
        Ok(match self.value_lists.binary_search(region_keys) {
            // Because `regions` always has one more value than `value_lists`, it's safe to get `i + 1`.
            Ok(i) => self.regions[i + 1],
            Err(i) => self.regions[i],
        })
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;

    use super::*;

    #[test]
    fn test_find_region() {
        // PARTITION BY RANGE COLUMNS(a) (
        //   PARTITION r1 VALUES LESS THAN ('hz'),
        //   PARTITION r2 VALUES LESS THAN ('sh'),
        //   PARTITION r3 VALUES LESS THAN (MAXVALUE),
        // )
        let rule = RangePartitionRule::new(
            vec!["a".to_string()],
            vec![vec!["hz".into()], vec!["sh".into()]],
            vec![1, 2, 3],
        );
        assert_matches!(
            rule.find_region(&vec!["foo".into(), 1000_i32.into()]),
            Err(error::Error::RegionKeysSize {
                expect: 1,
                actual: 2,
                ..
            })
        );
        assert_matches!(rule.find_region(&vec!["foo".into()]), Ok(1));
        assert_matches!(rule.find_region(&vec!["bar".into()]), Ok(1));
        assert_matches!(rule.find_region(&vec!["hz".into()]), Ok(2));
        assert_matches!(rule.find_region(&vec!["hzz".into()]), Ok(2));
        assert_matches!(rule.find_region(&vec!["sh".into()]), Ok(3));
        assert_matches!(rule.find_region(&vec!["zzzz".into()]), Ok(3));

        // PARTITION BY RANGE COLUMNS(a, b) (
        //   PARTITION r1 VALUES LESS THAN ('hz', 10),
        //   PARTITION r2 VALUES LESS THAN ('hz', 20),
        //   PARTITION r3 VALUES LESS THAN ('sh', 50),
        //   PARTITION r4 VALUES LESS THAN (MAXVALUE, MAXVALUE),
        // )
        let rule = RangePartitionRule::new(
            vec!["a".to_string(), "b".to_string()],
            vec![
                vec!["hz".into(), 10_i32.into()],
                vec!["hz".into(), 20_i32.into()],
                vec!["sh".into(), 50_i32.into()],
            ],
            vec![1, 2, 3, 4],
        );
        assert_matches!(
            rule.find_region(&vec!["foo".into()]),
            Err(error::Error::RegionKeysSize {
                expect: 2,
                actual: 1,
                ..
            })
        );
        assert_matches!(rule.find_region(&vec!["foo".into(), 1_i32.into()]), Ok(1));
        assert_matches!(rule.find_region(&vec!["bar".into(), 11_i32.into()]), Ok(1));
        assert_matches!(rule.find_region(&vec!["hz".into(), 2_i32.into()]), Ok(1));
        assert_matches!(rule.find_region(&vec!["hz".into(), 12_i32.into()]), Ok(2));
        assert_matches!(rule.find_region(&vec!["hz".into(), 22_i32.into()]), Ok(3));
        assert_matches!(rule.find_region(&vec!["hz".into(), 999_i32.into()]), Ok(3));
        assert_matches!(rule.find_region(&vec!["hzz".into(), 1_i32.into()]), Ok(3));
        assert_matches!(rule.find_region(&vec!["hzz".into(), 999_i32.into()]), Ok(3));
        assert_matches!(rule.find_region(&vec!["sh".into(), 49_i32.into()]), Ok(3));
        assert_matches!(rule.find_region(&vec!["sh".into(), 50_i32.into()]), Ok(4));
        assert_matches!(rule.find_region(&vec!["zzzz".into(), 1_i32.into()]), Ok(4));
    }
}
