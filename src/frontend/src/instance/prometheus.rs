use std::collections::HashMap;

use api::prometheus::remote::{
    read_request::ResponseType, Query, QueryResult, ReadRequest, ReadResponse, WriteRequest,
};
use async_trait::async_trait;
use client::ObjectResult;
use client::{Database, Select};
use common_error::prelude::BoxedError;
use common_telemetry::logging;
use prost::Message;
use servers::error::{self, Result as ServerResult};
use servers::http::{BytesResponse, HttpResponse};
use servers::prometheus::{self, Metrics};
use servers::query_handler::PrometheusProtocolHandler;
use snafu::{OptionExt, ResultExt};

use crate::instance::Instance;

const SAMPLES_RESPONSE_TYPE: i32 = ResponseType::Samples as i32;
const STREAM_RESPONSE_TYPE: i32 = ResponseType::StreamedXorChunks as i32;

fn supported_response_type(response_type: i32) -> bool {
    matches!(response_type, SAMPLES_RESPONSE_TYPE | STREAM_RESPONSE_TYPE)
}

/// Negotiating the content type of the remote read response.
///
/// Response types are taken from the list in the FIFO order. If no response type in `accepted_response_types` is
/// implemented by server, error is returned.
/// For request that do not contain `accepted_response_types` field the SAMPLES response type will be used.
fn negotiate_response_type(accepted_response_types: &[i32]) -> ServerResult<ResponseType> {
    if accepted_response_types.is_empty() {
        return Ok(ResponseType::Samples);
    }

    let response_type = accepted_response_types
        .iter()
        .find(|t| supported_response_type(**t))
        .with_context(|| error::NotSupportedSnafu {
            feat: format!(
                "server does not support any of the requested response types: {:?}",
                accepted_response_types
            ),
        })?;

    // It's safe to unwrap here, we known that it should be either SAMPLES_RESPONSE_TYPE
    // or STREAM_RESPONSE_TYPE
    Ok(ResponseType::from_i32(*response_type).unwrap())
}

fn object_result_to_query_result(table_name: &str, object_result: ObjectResult) -> QueryResult {
    let select_result = match object_result {
        ObjectResult::Select(result) => result,
        _ => unreachable!(),
    };

    QueryResult {
        timeseries: prometheus::select_result_to_timeseries(table_name, select_result),
    }
}

async fn handle_remote_queries(
    db: &Database,
    quires: &[Query],
) -> ServerResult<HashMap<String, ObjectResult>> {
    let mut results = HashMap::with_capacity(quires.len());

    for q in quires {
        let (table_name, sql) = prometheus::query_to_sql(q)?;

        logging::debug!(
            "prometheus remote read, table: {}, sql: {}",
            table_name,
            sql
        );

        let object_result = db
            .select(Select::Sql(sql.clone()))
            .await
            .map_err(BoxedError::new)
            .context(error::ExecuteQuerySnafu { query: sql })?;

        results.insert(table_name, object_result);
    }

    Ok(results)
}

#[async_trait]
impl PrometheusProtocolHandler for Instance {
    async fn write(&self, request: WriteRequest) -> ServerResult<()> {
        let exprs = prometheus::write_request_to_insert_exprs(request)?;

        self.db
            .batch_insert(exprs)
            .await
            .map_err(BoxedError::new)
            .context(error::ExecuteInsertSnafu {
                msg: "failed to write prometheus remote request",
            })?;

        Ok(())
    }

    async fn read(&self, request: ReadRequest) -> ServerResult<HttpResponse> {
        let response_type = negotiate_response_type(&request.accepted_response_types)?;

        let results = handle_remote_queries(&self.db, &request.queries).await?;

        match response_type {
            ResponseType::Samples => {
                let query_results = results
                    .into_iter()
                    .map(|(table_name, object_result)| {
                        object_result_to_query_result(&table_name, object_result)
                    })
                    .collect::<Vec<_>>();

                let response = ReadResponse {
                    results: query_results,
                };

                Ok(HttpResponse::Bytes(BytesResponse {
                    content_type: "application/x-protobuf".to_string(),
                    content_encoding: "snappy".to_string(),
                    bytes: response.encode_to_vec(),
                }))
            }
            ResponseType::StreamedXorChunks => {
                unimplemented!()
            }
        }
    }

    async fn inject_metrics(&self, _metrics: Metrics) -> ServerResult<()> {
        unimplemented!();
    }
}
