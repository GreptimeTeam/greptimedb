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

use std::sync::Arc;

use common_meta::error::{Result, UnexpectedSnafu};
use partition::partition::RegionMask;
use session::write_progress::WriteProgress;
use snafu::ensure;

/// Row ownership in payload order. Kept in process memory, never encoded in an RPC.
#[derive(Clone, Debug, Default)]
pub struct WriteAttribution {
    spans: Vec<(usize, Option<Arc<WriteProgress>>)>,
}

impl WriteAttribution {
    pub fn new(rows: usize, progress: Option<Arc<WriteProgress>>) -> Self {
        Self {
            spans: vec![(rows, progress)],
        }
    }

    pub fn extend(&mut self, other: Self) {
        self.spans.extend(other.spans);
    }

    pub fn row_count(&self) -> usize {
        self.spans.iter().map(|(rows, _)| rows).sum()
    }

    pub fn is_tracked(&self) -> bool {
        self.spans.iter().any(|(_, progress)| progress.is_some())
    }

    /// Selects ownership using the exact mask applied to the Arrow payload.
    pub fn filter(&self, mask: &RegionMask) -> Result<Self> {
        ensure!(
            self.row_count() == mask.array().len(),
            UnexpectedSnafu {
                err_msg: "Write ownership does not match the partition mask",
            }
        );
        if !self.is_tracked() {
            return Ok(Self::default());
        }
        let mut offset = 0;
        let mut selected = Self::default();
        for (rows, progress) in &self.spans {
            let count = mask.array().slice(offset, *rows).true_count();
            if count > 0 {
                selected.spans.push((count, progress.clone()));
            }
            offset += rows;
        }
        Ok(selected)
    }

    /// Successful writes must acknowledge every input row, including duplicates, as
    /// Mito's `RegionWriteCtx::push_bulk` does. A partial count cannot identify owners.
    pub(crate) fn record(&self, affected_rows: usize) -> Result<()> {
        if !self.is_tracked() {
            return Ok(());
        }
        ensure!(
            affected_rows == self.row_count(),
            UnexpectedSnafu {
                err_msg: format!(
                    "Write affected {affected_rows} rows, expected {}; cannot attribute the result",
                    self.row_count()
                ),
            }
        );
        for (rows, progress) in &self.spans {
            if let Some(progress) = progress {
                progress.add_written(*rows);
            }
        }
        Ok(())
    }

    /// Direct row writes have one owner even when a refreshed route splits them.
    pub(crate) fn for_rows(&self, rows: usize) -> Result<Self> {
        ensure!(
            self.spans.len() <= 1,
            UnexpectedSnafu {
                err_msg: "Cannot repartition shared ownership without a row mask",
            }
        );
        Ok(Self::new(
            rows,
            self.spans.first().and_then(|(_, p)| p.clone()),
        ))
    }
}

#[cfg(test)]
mod tests {
    use datatypes::arrow::array::BooleanArray;

    use super::*;

    #[test]
    fn ownership_survives_filtering_and_repartition() {
        let first = Arc::new(WriteProgress::default());
        let second = Arc::new(WriteProgress::default());
        let mut owners = WriteAttribution::new(3, Some(first.clone()));
        owners.extend(WriteAttribution::new(2, Some(second.clone())));
        let initial = owners
            .filter(&RegionMask::new(
                BooleanArray::from(vec![true, false, true, false, true]),
                3,
            ))
            .unwrap();
        let retry = initial
            .filter(&RegionMask::new(
                BooleanArray::from(vec![false, true, true]),
                2,
            ))
            .unwrap();
        assert!(retry.record(1).is_err());
        assert_eq!(first.written(), 0);
        retry.record(2).unwrap();
        assert_eq!(first.written(), 1);
        assert_eq!(second.written(), 1);
        assert!(owners.for_rows(5).is_err());
        assert!(
            owners
                .filter(&RegionMask::new(BooleanArray::from(vec![true]), 1))
                .is_err()
        );
    }
}
