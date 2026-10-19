// Copyright 2023-2026 GrepTime Inc.
//
// This file is part of the GreptimeDB Enterprise Edition and is licensed under
// the GreptimeDB Enterprise License. You may not use this file except in
// compliance with that license. A copy of the license is available at the root
// of this repository in the file LICENSE-ENTERPRISE.
//
// Unless required by applicable law or agreed to in writing, this software is
// distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied.

use std::sync::Arc;

use api::v1::{greptime_request::Request, query_request::Query};
use common_error::ext::ErrorExt;
use common_grpc::flow_write::{
    Admission, CHILD_KEY, COMMAND_KEY, Command, Reply, TerminalError, wait_completion,
};
use common_query::{Output, OutputData};
use servers::error::{self as server_error, Error as ServerError};
use servers::interceptor::GrpcQueryInterceptor;
use servers::query_handler::grpc::GrpcQueryHandler;
use session::context::QueryContextRef;

use crate::instance::Instance;

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct WritePayload {
    request: Request,
    catalog: String,
    schema: String,
    snapshots: Vec<(u64, u64)>,
    sst_min_sequences: Vec<(u64, u64)>,
    extensions: Vec<(String, String)>,
}

impl WritePayload {
    fn new(request: &Request, ctx: &QueryContextRef) -> Result<Self, String> {
        if !is_finite_insert(request) {
            return Err("tracked flow writes require INSERT or InsertIntoPlan".to_string());
        }
        let mut snapshots: Vec<_> = ctx.snapshots().into_iter().collect();
        snapshots.sort_unstable();
        let mut sst_min_sequences: Vec<_> = ctx.sst_min_sequences().into_iter().collect();
        sst_min_sequences.sort_unstable();
        let mut extensions: Vec<_> = ctx
            .extensions()
            .into_iter()
            .filter(|(key, _)| {
                key != COMMAND_KEY
                    && key != CHILD_KEY
                    && key != session::context::REMOTE_QUERY_ID_EXTENSION_KEY
            })
            .collect();
        extensions.sort_unstable();
        Ok(Self {
            request: request.clone(),
            catalog: ctx.current_catalog().to_string(),
            schema: ctx.current_schema(),
            snapshots,
            sst_min_sequences,
            extensions,
        })
    }
}

fn is_finite_insert(request: &Request) -> bool {
    match request {
        Request::Query(query) => match query.query.as_ref() {
            Some(Query::InsertIntoPlan(_)) => true,
            Some(Query::Sql(sql)) => sql.split_whitespace().next().is_some_and(|statement| {
                statement
                    .trim_end_matches(';')
                    .eq_ignore_ascii_case("INSERT")
            }),
            _ => false,
        },
        _ => false,
    }
}

impl Instance {
    fn intercept_control(
        &self,
        request: &Request,
        ctx: &QueryContextRef,
    ) -> server_error::Result<()> {
        let interceptor = self
            .plugins
            .get::<servers::interceptor::GrpcQueryInterceptorRef<crate::error::Error>>();
        interceptor
            .as_ref()
            .pre_execute(request, ctx.clone())
            .map_err(|error| ServerError::ExecuteQuery {
                source: common_error::ext::BoxedError::new(error),
                location: snafu::Location::new(file!(), line!(), column!()),
            })
    }

    pub(crate) async fn handle_flow_write_request(
        &self,
        request: Request,
        ctx: QueryContextRef,
        encoded_command: String,
    ) -> server_error::Result<Output> {
        let command = Command::decode(&encoded_command).map_err(|e| ServerError::InvalidQuery {
            reason: e.to_string(),
            location: snafu::Location::new(file!(), line!(), column!()),
        })?;
        let reply = match command {
            Command::Open { nonce } => {
                self.intercept_control(&request, &ctx)?;
                if !is_empty_control(&request) {
                    return Err(ServerError::InvalidQuery {
                        reason: "Open requires an empty SQL request".into(),
                        location: snafu::Location::new(file!(), line!(), column!()),
                    });
                }
                Reply::Opened {
                    capability: self
                        .flow_write_registry
                        .open(&nonce)
                        .map_err(registry_error)?,
                }
            }
            Command::Close { capability } => {
                self.intercept_control(&request, &ctx)?;
                if !is_empty_control(&request) {
                    return Err(ServerError::InvalidQuery {
                        reason: "Close requires an empty SQL request".into(),
                        location: snafu::Location::new(file!(), line!(), column!()),
                    });
                }
                self.flow_write_registry
                    .close(&capability)
                    .await
                    .map_err(registry_error)?;
                Reply::Closed
            }
            Command::Write { capability } => {
                let payload = WritePayload::new(&request, &ctx).map_err(|reason| {
                    ServerError::InvalidQuery {
                        reason,
                        location: snafu::Location::new(file!(), line!(), column!()),
                    }
                })?;
                match self
                    .flow_write_registry
                    .admit(&capability, payload)
                    .map_err(registry_error)?
                {
                    Admission::Join(receiver) => {
                        (*wait_completion(receiver).await.map_err(registry_error)?).clone()
                    }
                    Admission::Start(sender) => {
                        let receiver = sender.subscribe();
                        let instance = self.clone();
                        let mut child_ctx = (*ctx).clone();
                        child_ctx.remove_extension(COMMAND_KEY);
                        child_ctx.set_extension(CHILD_KEY, "1");
                        let child_ctx = Arc::new(child_ctx);
                        common_runtime::spawn_global(async move {
                            let terminal = match instance.do_query(request, child_ctx).await {
                                Ok(output) => terminal_reply(output),
                                Err(error) => Reply::Complete {
                                    rows: 0,
                                    cost: 0,
                                    metrics: None,
                                    error: Some(TerminalError {
                                        code: error.status_code() as u32,
                                        message: error.output_msg(),
                                    }),
                                },
                            };
                            let _ = sender.send(Some(Arc::new(terminal)));
                        });
                        (*wait_completion(receiver).await.map_err(registry_error)?).clone()
                    }
                }
            }
        };
        let batches = common_grpc::flow_write::reply_batches(&reply).map_err(|e| {
            ServerError::InvalidQuery {
                reason: e.to_string(),
                location: snafu::Location::new(file!(), line!(), column!()),
            }
        })?;
        Ok(Output::new_with_record_batches(batches))
    }
}

fn is_empty_control(request: &Request) -> bool {
    matches!(request, Request::Query(query) if matches!(&query.query, Some(Query::Sql(sql)) if sql.trim().is_empty()))
}

fn terminal_reply(output: Output) -> Reply {
    let (rows, cost) = output.extract_rows_and_cost();
    let metrics = output
        .meta
        .plan
        .clone()
        .and_then(query::metrics::terminal_recordbatch_metrics_from_plan);
    match output.data {
        OutputData::AffectedRows(_) => Reply::Complete {
            rows,
            cost,
            metrics,
            error: None,
        },
        _ => Reply::Complete {
            rows: 0,
            cost,
            metrics,
            error: Some(TerminalError {
                code: 1001,
                message: "tracked INSERT did not produce finite affected rows".into(),
            }),
        },
    }
}

fn registry_error(error: common_grpc::flow_write::RegistryError) -> ServerError {
    ServerError::InvalidQuery {
        reason: format!("flow write capability rejected: {error:?}"),
        location: snafu::Location::new(file!(), line!(), column!()),
    }
}
