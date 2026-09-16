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

use datatypes::arrow::array::{Array, BinaryArray};
use datatypes::arrow::record_batch::RecordBatch;
use snafu::{OptionExt, ensure};

use crate::error::{InvalidRecordBatchSnafu, Result};
use crate::sst::parquet::flat_format::primary_key_column_index;
use crate::sst::parquet::format::PrimaryKeyArray;

/// Iterates consecutive dictionary-key runs, preserving their row order without decoding.
pub(crate) struct PrimaryKeyRuns<'a> {
    keys: &'a [u32],
    values: &'a BinaryArray,
}

impl<'a> PrimaryKeyRuns<'a> {
    pub(crate) fn try_new(batch: &'a RecordBatch) -> Result<Self> {
        let pk = batch
            .column(primary_key_column_index(batch.num_columns()))
            .as_any()
            .downcast_ref::<PrimaryKeyArray>()
            .context(InvalidRecordBatchSnafu {
                reason: "Primary key column is not a dictionary array",
            })?;
        let values = pk.values().as_any().downcast_ref::<BinaryArray>().context(
            InvalidRecordBatchSnafu {
                reason: "Primary key values are not binary array",
            },
        )?;
        ensure!(
            pk.null_count() == 0 && values.null_count() == 0,
            InvalidRecordBatchSnafu {
                reason: "Primary keys must not be null"
            }
        );
        Ok(Self {
            keys: pk.keys().values(),
            values,
        })
    }
}

impl<'a> Iterator for PrimaryKeyRuns<'a> {
    type Item = (&'a [u8], usize);

    fn next(&mut self) -> Option<Self::Item> {
        let &key = self.keys.first()?;
        let count = self
            .keys
            .iter()
            .take_while(|&&current| current == key)
            .count();
        self.keys = &self.keys[count..];
        Some((self.values.value(key as usize), count))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datatypes::arrow::array::{ArrayRef, BinaryDictionaryBuilder, UInt8Array};
    use datatypes::arrow::datatypes::UInt32Type;

    use super::*;

    #[test]
    fn sliced_runs_preserve_order_and_nonconsecutive_keys() {
        let mut keys = BinaryDictionaryBuilder::<UInt32Type>::new();
        for key in ["a", "a", "b", "b", "b", "a", "c"] {
            keys.append(key).unwrap();
        }
        let batch = RecordBatch::try_from_iter([
            ("pk", Arc::new(keys.finish()) as ArrayRef),
            ("seq", Arc::new(UInt8Array::from(vec![0; 7])) as ArrayRef),
            ("op", Arc::new(UInt8Array::from(vec![0; 7])) as ArrayRef),
        ])
        .unwrap();
        let slice = batch.slice(1, 5);
        assert_eq!(
            PrimaryKeyRuns::try_new(&slice).unwrap().collect::<Vec<_>>(),
            vec![
                (b"a".as_slice(), 1),
                (b"b".as_slice(), 3),
                (b"a".as_slice(), 1)
            ]
        );
        assert!(
            PrimaryKeyRuns::try_new(&batch.slice(0, 0))
                .unwrap()
                .next()
                .is_none()
        );
    }
}
