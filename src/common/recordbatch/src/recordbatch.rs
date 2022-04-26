use std::sync::Arc;

use datafusion_common::record_batch::RecordBatch as DataFusionRecordBatch;
use datatypes::schema::Schema;

#[derive(Clone, Debug)]
pub struct RecordBatch {
    pub schema: Arc<Schema>,
    pub df_recordbatch: DataFusionRecordBatch,
}
