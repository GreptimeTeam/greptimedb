use datafusion_common::record_batch::RecordBatch as DataFusionRecordBatch;
use datatypes::schema::Schema;

use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct RecordBatch {
    pub schema: Arc<Schema>,
    pub df_recordbatch: DataFusionRecordBatch,
}
