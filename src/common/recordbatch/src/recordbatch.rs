use std::sync::Arc;

use datafusion_common::record_batch::RecordBatch as DfRecordBatch;
use datatypes::schema::Schema;

#[derive(Clone, Debug)]
pub struct RecordBatch {
    pub schema: Arc<Schema>,
    pub df_recordbatch: DfRecordBatch,
}
