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

//! For single-thread flow worker

use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use enum_as_inner::EnumAsInner;
use hydroflow::scheduled::graph::Hydroflow;
use snafu::{ensure, OptionExt};
use tokio::sync::{broadcast, mpsc, Mutex};

use crate::adapter::error::{Error, FlowAlreadyExistSnafu, InternalSnafu};
use crate::adapter::FlowId;
use crate::compute::{Context, DataflowState, ErrCollector};
use crate::expr::GlobalId;
use crate::plan::TypedPlan;
use crate::repr::{self, DiffRow};

pub type SharedBuf = Arc<Mutex<VecDeque<DiffRow>>>;

type ReqId = usize;

/// Create both worker(`!Send`) and worker handle(`Send + Sync`)
pub fn create_worker<'a>() -> (WorkerHandle, Worker<'a>) {
    let (itc_client, itc_server) = create_inter_thread_call();
    let worker_handle = WorkerHandle {
        itc_client: Mutex::new(itc_client),
    };
    let worker = Worker {
        task_states: BTreeMap::new(),
        itc_server: Arc::new(Mutex::new(itc_server)),
    };
    (worker_handle, worker)
}

/// ActiveDataflowState is a wrapper around `Hydroflow` and `DataflowState`
pub(crate) struct ActiveDataflowState<'subgraph> {
    df: Hydroflow<'subgraph>,
    state: DataflowState,
    err_collector: ErrCollector,
}

impl std::fmt::Debug for ActiveDataflowState<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ActiveDataflowState")
            .field("df", &"<Hydroflow>")
            .field("state", &self.state)
            .field("err_collector", &self.err_collector)
            .finish()
    }
}

impl Default for ActiveDataflowState<'_> {
    fn default() -> Self {
        ActiveDataflowState {
            df: Hydroflow::new(),
            state: DataflowState::default(),
            err_collector: ErrCollector::default(),
        }
    }
}

impl<'subgraph> ActiveDataflowState<'subgraph> {
    /// Create a new render context, assigned with given global id
    pub fn new_ctx<'ctx>(&'ctx mut self, global_id: GlobalId) -> Context<'ctx, 'subgraph>
    where
        'subgraph: 'ctx,
    {
        Context {
            id: global_id,
            df: &mut self.df,
            compute_state: &mut self.state,
            err_collector: self.err_collector.clone(),
            input_collection: Default::default(),
            local_scope: Default::default(),
        }
    }

    pub fn set_current_ts(&mut self, ts: repr::Timestamp) {
        self.state.set_current_ts(ts);
    }

    /// Run all available subgraph
    ///
    /// return true if any subgraph actually executed
    pub fn run_available(&mut self) -> bool {
        self.state.run_available_with_schedule(&mut self.df)
    }
}

#[derive(Debug)]
pub struct WorkerHandle {
    itc_client: Mutex<InterThreadCallClient>,
}

impl WorkerHandle {
    /// create task, return task id
    pub async fn create_flow(&self, create_reqs: Request) -> Result<Option<FlowId>, Error> {
        ensure!(
            matches!(create_reqs, Request::Create { .. }),
            InternalSnafu {
                reason: format!(
                    "Flow Node/Worker itc failed, expect Request::Create, found {create_reqs:?}"
                ),
            }
        );

        let ret = self
            .itc_client
            .lock()
            .await
            .call_blocking(create_reqs)
            .await?;
        ret.into_create().map_err(|ret| {
            InternalSnafu {
                reason: format!(
                    "Flow Node/Worker itc failed, expect Response::Create, found {ret:?}"
                ),
            }
            .build()
        })?
    }

    /// remove task, return task id
    pub async fn remove_flow(&self, flow_id: FlowId) -> Result<bool, Error> {
        let req = Request::Remove { flow_id };
        let ret = self.itc_client.lock().await.call_blocking(req).await?;

        ret.into_remove().map_err(|ret| {
            InternalSnafu {
                reason: format!("Flow Node/Worker failed, expect Response::Remove, found {ret:?}"),
            }
            .build()
        })
    }

    /// trigger running the worker, will not block, and will run the worker parallelly
    ///
    /// will set the current timestamp to `now` for all dataflows before running them
    ///
    /// the returned error is unrecoverable, and the worker should be shutdown/rebooted
    pub async fn run_available(&self, now: repr::Timestamp) -> Result<(), Error> {
        self.itc_client
            .lock()
            .await
            .call_non_blocking(Request::RunAvail { now })
            .await
    }

    pub async fn contains_flow(&self, flow_id: FlowId) -> Result<bool, Error> {
        let req = Request::ContainTask { flow_id };
        let ret = self.itc_client.lock().await.call_blocking(req).await?;

        ret.into_contain_task().map_err(|ret| {
            InternalSnafu {
                reason: format!(
                    "Flow Node/Worker itc failed, expect Response::ContainTask, found {ret:?}"
                ),
            }
            .build()
        })
    }

    /// shutdown the worker
    pub async fn shutdown(&self) -> Result<(), Error> {
        self.itc_client
            .lock()
            .await
            .call_non_blocking(Request::Shutdown)
            .await
    }
}

/// The actual worker that does the work and contain active state
#[derive(Debug)]
pub struct Worker<'subgraph> {
    /// Task states
    pub(crate) task_states: BTreeMap<FlowId, ActiveDataflowState<'subgraph>>,
    itc_server: Arc<Mutex<InterThreadCallServer>>,
}

impl<'s> Worker<'s> {
    #[allow(clippy::too_many_arguments)]
    pub fn create_flow(
        &mut self,
        flow_id: FlowId,
        plan: TypedPlan,
        sink_id: GlobalId,
        sink_sender: mpsc::UnboundedSender<DiffRow>,
        source_ids: &[GlobalId],
        src_recvs: Vec<broadcast::Receiver<DiffRow>>,
        // TODO(discord9): set expire duration for all arrangement and compare to sys timestamp instead
        expire_when: Option<repr::Duration>,
        create_if_not_exist: bool,
        err_collector: ErrCollector,
    ) -> Result<Option<FlowId>, Error> {
        let _ = expire_when;
        let already_exist = self.task_states.contains_key(&flow_id);
        match (already_exist, create_if_not_exist) {
            (true, true) => return Ok(None),
            (true, false) => FlowAlreadyExistSnafu { id: flow_id }.fail()?,
            (false, _) => (),
        };

        let mut cur_task_state = ActiveDataflowState::<'s> {
            err_collector,
            ..Default::default()
        };

        {
            let mut ctx = cur_task_state.new_ctx(sink_id);
            for (source_id, src_recv) in source_ids.iter().zip(src_recvs) {
                let bundle = ctx.render_source(src_recv)?;
                ctx.insert_global(*source_id, bundle);
            }

            let rendered = ctx.render_plan(plan)?;
            ctx.render_unbounded_sink(rendered, sink_sender);
        }
        self.task_states.insert(flow_id, cur_task_state);
        Ok(Some(flow_id))
    }

    /// remove task, return true if a task is removed
    pub fn remove_flow(&mut self, flow_id: FlowId) -> bool {
        self.task_states.remove(&flow_id).is_some()
    }

    /// Run the worker, blocking, until shutdown signal is received
    pub fn run(&mut self) {
        loop {
            let (req_id, req) = if let Some(ret) = self.itc_server.blocking_lock().blocking_recv() {
                ret
            } else {
                common_telemetry::error!(
                    "Worker's itc server has been closed unexpectedly, shutting down worker now."
                );
                break;
            };

            let ret = self.handle_req(req_id, req);
            match ret {
                Ok(Some((id, resp))) => {
                    if let Err(err) = self.itc_server.blocking_lock().resp(id, resp) {
                        common_telemetry::error!(
                            "Worker's itc server has been closed unexpectedly, shutting down worker: {}",
                            err
                        );
                        break;
                    };
                }
                Ok(None) => continue,
                Err(()) => {
                    break;
                }
            }
        }
    }

    /// run with tick acquired from tick manager(usually means system time)
    /// TODO(discord9): better tick management
    pub fn run_tick(&mut self, now: repr::Timestamp) {
        for (_flow_id, task_state) in self.task_states.iter_mut() {
            task_state.set_current_ts(now);
            task_state.run_available();
        }
    }
    /// handle request, return response if any, Err if receive shutdown signal
    ///
    /// return `Err(())` if receive shutdown request
    fn handle_req(&mut self, req_id: ReqId, req: Request) -> Result<Option<(ReqId, Response)>, ()> {
        let ret = match req {
            Request::Create {
                flow_id,
                plan,
                sink_id,
                sink_sender,
                source_ids,
                src_recvs,
                expire_when,
                create_if_not_exist,
                err_collector,
            } => {
                let task_create_result = self.create_flow(
                    flow_id,
                    plan,
                    sink_id,
                    sink_sender,
                    &source_ids,
                    src_recvs,
                    expire_when,
                    create_if_not_exist,
                    err_collector,
                );
                Some((
                    req_id,
                    Response::Create {
                        result: task_create_result,
                    },
                ))
            }
            Request::Remove { flow_id } => {
                let ret = self.remove_flow(flow_id);
                Some((req_id, Response::Remove { result: ret }))
            }
            Request::RunAvail { now } => {
                self.run_tick(now);
                None
            }
            Request::ContainTask { flow_id } => {
                let ret = self.task_states.contains_key(&flow_id);
                Some((req_id, Response::ContainTask { result: ret }))
            }
            Request::Shutdown => return Err(()),
        };
        Ok(ret)
    }
}

#[derive(Debug, EnumAsInner)]
pub enum Request {
    Create {
        flow_id: FlowId,
        plan: TypedPlan,
        sink_id: GlobalId,
        sink_sender: mpsc::UnboundedSender<DiffRow>,
        source_ids: Vec<GlobalId>,
        src_recvs: Vec<broadcast::Receiver<DiffRow>>,
        expire_when: Option<repr::Duration>,
        create_if_not_exist: bool,
        err_collector: ErrCollector,
    },
    Remove {
        flow_id: FlowId,
    },
    /// Trigger the worker to run, useful after input buffer is full
    RunAvail {
        now: repr::Timestamp,
    },
    ContainTask {
        flow_id: FlowId,
    },
    Shutdown,
}

#[derive(Debug, EnumAsInner)]
enum Response {
    Create {
        result: Result<Option<FlowId>, Error>,
        // TODO(discord9): add flow err_collector
    },
    Remove {
        result: bool,
    },
    ContainTask {
        result: bool,
    },
}

fn create_inter_thread_call() -> (InterThreadCallClient, InterThreadCallServer) {
    let (arg_send, arg_recv) = mpsc::unbounded_channel();
    let (ret_send, ret_recv) = mpsc::unbounded_channel();
    let client = InterThreadCallClient {
        call_id: AtomicUsize::new(0),
        arg_sender: arg_send,
        ret_recv,
    };
    let server = InterThreadCallServer {
        arg_recv,
        ret_sender: ret_send,
    };
    (client, server)
}

#[derive(Debug)]
struct InterThreadCallClient {
    call_id: AtomicUsize,
    arg_sender: mpsc::UnboundedSender<(ReqId, Request)>,
    ret_recv: mpsc::UnboundedReceiver<(ReqId, Response)>,
}

impl InterThreadCallClient {
    /// call without expecting responses or blocking
    async fn call_non_blocking(&self, req: Request) -> Result<(), Error> {
        // TODO(discord9): relax memory order later
        let call_id = self.call_id.fetch_add(1, Ordering::SeqCst);
        self.arg_sender
            .send((call_id, req))
            .map_err(from_send_error)
    }

    /// call blocking, and return the result
    async fn call_blocking(&mut self, req: Request) -> Result<Response, Error> {
        // TODO(discord9): relax memory order later
        let call_id = self.call_id.fetch_add(1, Ordering::SeqCst);
        self.arg_sender
            .send((call_id, req))
            .map_err(from_send_error)?;

        // TODO(discord9): better inter thread call impl, i.e. support multiple client(also consider if it's necessary)
        // since one node manger might manage multiple worker, but one worker should only belong to one node manager
        let (ret_call_id, ret) = self
            .ret_recv
            .recv()
            .await
            .context(InternalSnafu { reason: "InterThreadCallClient call_blocking failed, ret_recv has been closed and there are no remaining messages in the channel's buffer" })?;

        ensure!(
            ret_call_id == call_id,
            InternalSnafu {
                reason: "call id mismatch, worker/worker handler should be in sync",
            }
        );
        Ok(ret)
    }
}

#[derive(Debug)]
struct InterThreadCallServer {
    pub arg_recv: mpsc::UnboundedReceiver<(ReqId, Request)>,
    pub ret_sender: mpsc::UnboundedSender<(ReqId, Response)>,
}

impl InterThreadCallServer {
    pub async fn recv(&mut self) -> Option<(usize, Request)> {
        self.arg_recv.recv().await
    }

    pub fn blocking_recv(&mut self) -> Option<(usize, Request)> {
        self.arg_recv.blocking_recv()
    }

    /// Send response back to the client
    pub fn resp(&self, call_id: ReqId, resp: Response) -> Result<(), Error> {
        self.ret_sender
            .send((call_id, resp))
            .map_err(from_send_error)
    }
}

fn from_send_error<T>(err: mpsc::error::SendError<T>) -> Error {
    InternalSnafu {
        reason: format!("InterThreadCallServer resp failed: {}", err),
    }
    .build()
}

#[cfg(test)]
mod test {
    use tokio::sync::oneshot;

    use super::*;
    use crate::expr::Id;
    use crate::plan::Plan;
    use crate::repr::{RelationType, Row};

    #[tokio::test]
    pub async fn test_simple_get_with_worker_and_handle() {
        let (tx, rx) = oneshot::channel();
        let worker_thread_handle = std::thread::spawn(move || {
            let (handle, mut worker) = create_worker();
            tx.send(handle).unwrap();
            worker.run();
        });
        let handle = rx.await.unwrap();
        let src_ids = vec![GlobalId::User(1)];
        let (tx, rx) = broadcast::channel::<DiffRow>(1024);
        let (sink_tx, mut sink_rx) = mpsc::unbounded_channel::<DiffRow>();
        let (flow_id, plan) = (
            1,
            TypedPlan {
                plan: Plan::Get {
                    id: Id::Global(GlobalId::User(1)),
                },
                typ: RelationType::new(vec![]),
            },
        );
        let create_reqs = Request::Create {
            flow_id,
            plan,
            sink_id: GlobalId::User(1),
            sink_sender: sink_tx,
            source_ids: src_ids,
            src_recvs: vec![rx],
            expire_when: None,
            create_if_not_exist: true,
            err_collector: ErrCollector::default(),
        };
        handle.create_flow(create_reqs).await.unwrap();
        tx.send((Row::empty(), 0, 0)).unwrap();
        handle.run_available(0).await.unwrap();
        assert_eq!(sink_rx.recv().await.unwrap().0, Row::empty());
        handle.shutdown().await.unwrap();
        worker_thread_handle.join().unwrap();
    }
}
