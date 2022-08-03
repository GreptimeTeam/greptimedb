use api::v1::{ObjectResult, ResultHeader, SelectResult as GrpcSelectResult};
use common_recordbatch::{util, RecordBatch};
use query::Output;

use crate::error::Result;
use crate::server::grpc::{
    handler::{ERROR, SUCCESS},
    server::PROTOCOL_VERSION,
};

pub(crate) async fn select_result(select_result: Result<Output>) -> ObjectResult {
    let mut object_resp = ObjectResult::default();

    let mut header = ResultHeader {
        version: PROTOCOL_VERSION,
        ..Default::default()
    };

    match select_result {
        Ok(output) => match output {
            Output::AffectedRows(rows) => {
                header.code = SUCCESS;
                header.success = rows as u32;
            }
            Output::RecordBatch(stream) => match util::collect(stream).await {
                Ok(record_batches) => {
                    match convert_record_batches_to_select_result(record_batches) {
                        Ok(select_result) => {
                            header.code = SUCCESS;
                            object_resp.results = select_result.into();
                        }
                        Err(err) => {
                            header.code = ERROR;
                            header.err_msg = err.to_string();
                        }
                    }
                }
                Err(err) => {
                    header.code = ERROR;
                    header.err_msg = err.to_string();
                }
            },
        },
        Err(err) => {
            header.code = ERROR;
            header.err_msg = err.to_string();
        }
    }
    object_resp.header = Some(header);
    object_resp
}

pub(crate) fn convert_record_batches_to_select_result(
    _record_batches: Vec<RecordBatch>,
) -> Result<GrpcSelectResult> {
    todo!()
}
