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
use std::sync::Arc;

use hydroflow::scheduled::graph::Hydroflow;
use snafu::ResultExt;
use tokio::sync::{broadcast, mpsc, Mutex};

use crate::adapter::error::{Error, EvalSnafu};
use crate::adapter::FlowId;
use crate::compute::{Context, DataflowState, ErrCollector};
use crate::expr::error::InternalSnafu;
use crate::expr::GlobalId;
use crate::plan::TypedPlan;
use crate::repr::{self, DiffRow};

pub type SharedBuf = Arc<Mutex<VecDeque<DiffRow>>>;

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
    ///
    #[allow(clippy::too_many_arguments)]
    pub async fn create_flow(
        &self,
        task_id: FlowId,
        plan: TypedPlan,
        sink_id: GlobalId,
        sink_sender: mpsc::UnboundedSender<DiffRow>,
        source_ids: &[GlobalId],
        src_recvs: Vec<broadcast::Receiver<DiffRow>>,
        expire_when: Option<repr::Duration>,
        create_if_not_exist: bool,
        err_collector: ErrCollector,
    ) -> Result<Option<FlowId>, Error> {
        let req = Request::Create {
            task_id,
            plan,
            sink_id,
            sink_sender,
            source_ids: source_ids.to_vec(),
            src_recvs,
            expire_when,
            create_if_not_exist,
            err_collector,
        };

        let ret = self.itc_client.lock().await.call_blocking(req).await?;
        if let Response::Create {
            result: task_create_result,
        } = ret
        {
            task_create_result
        } else {
            InternalSnafu {
                reason: format!(
                    "Flow Node/Worker itc failed, expect Response::Create, found {ret:?}"
                ),
            }
            .fail()
            .with_context(|_| EvalSnafu {})
        }
    }

    /// remove task, return task id
    pub async fn remove_flow(&self, task_id: FlowId) -> Result<bool, Error> {
        let req = Request::Remove { task_id };
        let ret = self.itc_client.lock().await.call_blocking(req).await?;
        if let Response::Remove { result } = ret {
            Ok(result)
        } else {
            InternalSnafu {
                reason: format!("Flow Node/Worker failed, expect Response::Remove, found {ret:?}"),
            }
            .fail()
            .with_context(|_| EvalSnafu {})
        }
    }

    /// trigger running the worker, will not block, and will run the worker parallelly
    ///
    /// will set the current timestamp to `now` for all dataflows before running them
    pub async fn run_available(&self, now: repr::Timestamp) {
        self.itc_client
            .lock()
            .await
            .call_non_blocking(Request::RunAvail { now })
            .await;
    }

    pub async fn contains_flow(&self, task_id: FlowId) -> Result<bool, Error> {
        let req = Request::ContainTask { task_id };
        let ret = self
            .itc_client
            .lock()
            .await
            .call_blocking(req)
            .await
            .unwrap();
        if let Response::ContainTask {
            result: task_contain_result,
        } = ret
        {
            Ok(task_contain_result)
        } else {
            InternalSnafu {
                reason: format!(
                    "Flow Node/Worker itc failed, expect Response::ContainTask, found {ret:?}"
                ),
            }
            .fail()
            .with_context(|_| EvalSnafu {})
        }
    }

    /// shutdown the worker
    pub async fn shutdown(&self) {
        self.itc_client
            .lock()
            .await
            .call_non_blocking(Request::Shutdown)
            .await;
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
        task_id: FlowId,
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
        if create_if_not_exist {
            // check if the task already exists
            if self.task_states.contains_key(&task_id) {
                return Ok(None);
            }
        }

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

            let rendered = ctx.render_plan(plan.plan)?;
            ctx.render_unbounded_sink(rendered, sink_sender);
        }
        self.task_states.insert(task_id, cur_task_state);
        Ok(Some(task_id))
    }

    /// remove task, return true if a task is removed
    pub fn remove_flow(&mut self, task_id: FlowId) -> bool {
        self.task_states.remove(&task_id).is_some()
    }

    /// Run the worker, blocking, until shutdown signal is received
    pub fn run(&mut self) {
        loop {
            let (req_id, req) = self.itc_server.blocking_lock().blocking_recv().unwrap();

            let ret = self.handle_req(req_id, req);
            match ret {
                Ok(Some((id, resp))) => {
                    self.itc_server.blocking_lock().resp(id, resp);
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
        for (_task_id, task_state) in self.task_states.iter_mut() {
            task_state.set_current_ts(now);
            task_state.run_available();
        }
    }
    /// handle request, return response if any, Err if receive shutdown signal
    fn handle_req(&mut self, req_id: usize, req: Request) -> Result<Option<(usize, Response)>, ()> {
        let ret = match req {
            Request::Create {
                task_id,
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
                    task_id,
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
            Request::Remove { task_id } => {
                let ret = self.remove_flow(task_id);
                Some((req_id, Response::Remove { result: ret }))
            }
            Request::RunAvail { now } => {
                self.run_tick(now);
                None
            }
            Request::ContainTask { task_id } => {
                let ret = self.task_states.contains_key(&task_id);
                Some((req_id, Response::ContainTask { result: ret }))
            }
            Request::Shutdown => return Err(()),
        };
        Ok(ret)
    }
}

#[derive(Debug)]
enum Request {
    Create {
        task_id: FlowId,
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
        task_id: FlowId,
    },
    /// Trigger the worker to run, useful after input buffer is full
    RunAvail {
        now: repr::Timestamp,
    },
    ContainTask {
        task_id: FlowId,
    },
    Shutdown,
}

#[derive(Debug)]
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
        call_id: Arc::new(Mutex::new(0)),
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
    call_id: Arc<Mutex<usize>>,
    arg_sender: mpsc::UnboundedSender<(usize, Request)>,
    ret_recv: mpsc::UnboundedReceiver<(usize, Response)>,
}

impl InterThreadCallClient {
    /// call without expecting responses or blocking
    async fn call_non_blocking(&self, req: Request) {
        let call_id = {
            let mut call_id = self.call_id.lock().await;
            *call_id += 1;
            *call_id
        };
        self.arg_sender.send((call_id, req)).unwrap();
    }
    /// call blocking, and return the result
    async fn call_blocking(&mut self, req: Request) -> Result<Response, Error> {
        let call_id = {
            let mut call_id = self.call_id.lock().await;
            *call_id += 1;
            *call_id
        };
        self.arg_sender.send((call_id, req)).unwrap();
        // TODO(discord9): better inter thread call impl
        let (ret_call_id, ret) = self.ret_recv.recv().await.unwrap();
        if ret_call_id != call_id {
            return InternalSnafu {
                reason: "call id mismatch, worker/worker handler should be in sync",
            }
            .fail()
            .with_context(|_| EvalSnafu {});
        }
        Ok(ret)
    }
}

#[derive(Debug)]
struct InterThreadCallServer {
    pub arg_recv: mpsc::UnboundedReceiver<(usize, Request)>,
    pub ret_sender: mpsc::UnboundedSender<(usize, Response)>,
}

impl InterThreadCallServer {
    pub async fn recv(&mut self) -> Option<(usize, Request)> {
        self.arg_recv.recv().await
    }

    pub fn blocking_recv(&mut self) -> Option<(usize, Request)> {
        self.arg_recv.blocking_recv()
    }

    /// Send response back to the client
    pub fn resp(&self, call_id: usize, resp: Response) {
        self.ret_sender.send((call_id, resp)).unwrap();
    }
}

#[cfg(test)]
mod test {
    use tokio::sync::oneshot;

    use super::*;
    use crate::adapter::FlowTickManager;
    use crate::expr::Id;
    use crate::plan::Plan;
    use crate::repr::{RelationType, Row};
    #[tokio::test]
    pub async fn test_simple_get_with_worker_and_handle() {
        let flow_tick = FlowTickManager::new();
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
        let (task_id, plan) = (
            1,
            TypedPlan {
                plan: Plan::Get {
                    id: Id::Global(GlobalId::User(1)),
                },
                typ: RelationType::new(vec![]),
            },
        );
        handle
            .create_flow(
                task_id,
                plan,
                GlobalId::User(1),
                sink_tx,
                &src_ids,
                vec![rx],
                None,
                true,
                ErrCollector::default(),
            )
            .await
            .unwrap();
        tx.send((Row::empty(), 0, 0)).unwrap();
        handle.run_available(flow_tick.tick()).await;
        assert_eq!(sink_rx.recv().await.unwrap().0, Row::empty());
        handle.shutdown().await;
        worker_thread_handle.join().unwrap();
    }
}
