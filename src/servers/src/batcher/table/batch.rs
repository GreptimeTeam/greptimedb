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
use std::time::Instant;

use arrow::compute::concat_batches;
use arrow::record_batch::RecordBatch;
use operator::error::{ComputeArrowSnafu, Result, UnexpectedSnafu};
use operator::insert::Inserter;
use snafu::ResultExt;
use table::metadata::TableInfoRef;

use crate::batcher::table::flow_notifier::FlowNotifier;
use crate::batcher::table::metrics::{
    FLUSH_DROPPED_ROWS, FLUSH_ELAPSED, FLUSH_FAILURES, FLUSH_ROWS, FLUSH_TOTAL,
};
use crate::batcher::table::pending_batch::{PendingBatch, notify_batches};

/// A detached batch owns every submission until its completion is reported.
pub(in crate::batcher::table) struct Batch {
    pub submissions: Vec<PendingBatch>,
    pub total_rows: usize,
}

pub(in crate::batcher::table) async fn flush_batch(
    batch: Batch,
    inserter: Arc<Inserter>,
    notifier: FlowNotifier,
) {
    let started = Instant::now();
    let result = send_batch(&batch, &inserter).await;
    FLUSH_ELAPSED.observe(started.elapsed().as_secs_f64());
    match result {
        Ok((table, combined)) => {
            FLUSH_TOTAL.inc();
            FLUSH_ROWS.observe(batch.total_rows as f64);
            notify_batches(batch.submissions, Ok(()));
            // Admission is best-effort and only occurs after affected-row validation.
            notifier.notify(table, &combined);
        }
        Err(error) => {
            FLUSH_FAILURES.inc();
            FLUSH_DROPPED_ROWS.inc_by(batch.total_rows as u64);
            notify_batches(batch.submissions, Err(Arc::new(error)));
        }
    }
}

async fn send_batch(batch: &Batch, inserter: &Inserter) -> Result<(TableInfoRef, RecordBatch)> {
    let first = batch.submissions.first().ok_or_else(|| {
        UnexpectedSnafu {
            violated: "cannot execute an empty flush".to_string(),
        }
        .build()
    })?;
    let combined = combine_batches(batch.submissions.iter().map(|pending| &pending.batch))?;
    let affected_rows = inserter
        .flush_bulk_batch(
            first.table_info.clone(),
            combined.clone(),
            first.ctx.clone(),
        )
        .await?;
    validate_affected_rows(batch.total_rows, affected_rows)?;
    Ok((first.table_info.clone(), combined))
}

fn combine_batches<'a>(batches: impl IntoIterator<Item = &'a RecordBatch>) -> Result<RecordBatch> {
    let batches = batches.into_iter().collect::<Vec<_>>();
    let Some(first) = batches.first() else {
        return UnexpectedSnafu {
            violated: "cannot combine an empty flush".to_string(),
        }
        .fail();
    };
    let schema = first.schema();
    if batches.iter().any(|batch| batch.schema() != schema) {
        return UnexpectedSnafu {
            violated: "cannot combine different Arrow schemas".to_string(),
        }
        .fail();
    }
    concat_batches(&schema, batches).context(ComputeArrowSnafu)
}

fn validate_affected_rows(expected: usize, actual: usize) -> Result<()> {
    if expected != actual {
        return UnexpectedSnafu {
            violated: format!("batched write affected {actual} rows, expected {expected}; individual results cannot be attributed"),
        }.fail();
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};

    use crate::batcher::table::batch::*;

    fn batch(name: &str, values: Vec<i32>) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(name, DataType::Int32, false)])),
            vec![Arc::new(Int32Array::from(values))],
        )
        .unwrap()
    }

    #[test]
    fn test_combine_preserves_rows_and_rejects_schema_changes() {
        let first = batch("a", vec![1, 2]);
        let second = batch("a", vec![3]);
        assert_eq!(
            batch("a", vec![1, 2, 3]),
            combine_batches([&first, &second]).unwrap()
        );
        let different = batch("b", vec![4]);
        assert!(combine_batches([&first, &different]).is_err());
        let schema = first.schema();
        let metadata = Schema::new_with_metadata(
            schema.fields().clone(),
            [("version".to_string(), "2".to_string())]
                .into_iter()
                .collect(),
        );
        let changed = RecordBatch::try_new(Arc::new(metadata), first.columns().to_vec()).unwrap();
        assert!(combine_batches([&first, &changed]).is_err());
    }

    #[test]
    fn test_affected_rows_must_match_before_attribution() {
        for (expected, actual, valid) in [(0, 0, true), (3, 3, true), (3, 2, false), (3, 4, false)]
        {
            assert_eq!(valid, validate_affected_rows(expected, actual).is_ok());
        }
    }
}
