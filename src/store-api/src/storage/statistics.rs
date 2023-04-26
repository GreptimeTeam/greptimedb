use datatypes::value::Value;
use snafu::ensure;

use crate::error::{ColumnNumberMismatchSnafu, Result};

#[derive(Default, Clone, Debug)]
pub struct SstColumnStatistics {
    pub min: Option<Value>,
    pub max: Option<Value>,
    pub null_count: Option<usize>,
    pub distinct_count: Option<usize>,
}

#[derive(Default, Clone, Debug)]
pub struct SstStatistics {
    /// Projected column statistics. Either empty or the same length as the projected schema.
    pub column_statistics: Vec<SstColumnStatistics>,
    pub row_count: Option<usize>,
}

impl SstStatistics {
    pub fn try_merge(self, other: Self) -> Result<Self> {
        ensure!(
            self.column_statistics.len() == other.column_statistics.len(),
            ColumnNumberMismatchSnafu {
                left: self.column_statistics.len(),
                right: other.column_statistics.len()
            }
        );

        let row_count = sum_options(self.row_count, other.row_count);
        let column_statistics = self
            .column_statistics
            .into_iter()
            .zip(other.column_statistics)
            .map(|(this_column, that_column)| {
                let min = this_column.min.into_iter().chain(that_column.min).min();
                let max = this_column.max.into_iter().chain(that_column.max).max();
                let null_count = sum_options(this_column.null_count, that_column.null_count);
                let distinct_count =
                    sum_options(this_column.distinct_count, that_column.distinct_count);
                SstColumnStatistics {
                    min,
                    max,
                    null_count,
                    distinct_count,
                }
            })
            .collect();

        Ok(Self {
            column_statistics,
            row_count,
        })
    }
}

fn sum_options(lhs: Option<usize>, rhs: Option<usize>) -> Option<usize> {
    match (lhs, rhs) {
        (None, None) => None,
        (None, Some(sum)) | (Some(sum), None) => Some(sum),
        (Some(lhs), Some(rhs)) => Some(lhs + rhs),
    }
}
