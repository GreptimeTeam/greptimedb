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

use std::cell::RefCell;
use std::collections::{BTreeMap, VecDeque};
use std::rc::Rc;

use dfir_rs::scheduled::graph::Dfir;
use dfir_rs::scheduled::SubgraphId;
use get_size2::GetSize;

use crate::compute::types::ErrCollector;
use crate::repr::{self, Timestamp};
use crate::utils::{ArrangeHandler, Arrangement};

/// input/output of a dataflow
/// One `ComputeState` manage the input/output/schedule of one `Dfir`
#[derive(Debug, Default)]
pub struct DataflowState {
    /// it is important to use a deque to maintain the order of subgraph here
    /// TODO(discord9): consider dedup? Also not necessary for hydroflow itself also do dedup when schedule
    schedule_subgraph: Rc<RefCell<BTreeMap<Timestamp, VecDeque<SubgraphId>>>>,
    /// Frontier (in sys time) before which updates should not be emitted.
    ///
    /// We *must* apply it to sinks, to ensure correct outputs.
    /// We *should* apply it to sources and imported shared state, because it improves performance.
    /// Which means it's also the current time in temporal filter to get current correct result
    as_of: Rc<RefCell<Timestamp>>,
    /// error collector local to this `ComputeState`,
    /// useful for distinguishing errors from different `Dfir`
    err_collector: ErrCollector,
    /// save all used arrange in this dataflow, since usually there is no delete operation
    /// we can just keep track of all used arrange and schedule subgraph when they need to be updated
    arrange_used: Vec<ArrangeHandler>,
    /// the time arrangement need to be expired after a certain time in milliseconds
    expire_after: Option<Timestamp>,
    /// the last time each subgraph executed
    last_exec_time: Option<Timestamp>,
}

impl DataflowState {
    pub fn new_arrange(&mut self, name: Option<Vec<String>>) -> ArrangeHandler {
        let arrange = name.map(Arrangement::new_with_name).unwrap_or_default();

        let arr = ArrangeHandler::from(arrange);
        // mark this arrange as used in this dataflow
        self.arrange_used.push(
            arr.clone_future_only()
                .expect("No write happening at this point"),
        );
        arr
    }

    /// schedule all subgraph that need to run with time <= `as_of` and run_available()
    ///
    /// return true if any subgraph actually executed
    #[allow(clippy::swap_with_temporary)]
    pub fn run_available_with_schedule(&mut self, df: &mut Dfir) -> bool {
        // first split keys <= as_of into another map
        let mut before = self
            .schedule_subgraph
            .borrow_mut()
            .split_off(&(*self.as_of.borrow() + 1));
        std::mem::swap(&mut before, &mut self.schedule_subgraph.borrow_mut());
        for (_, v) in before {
            for subgraph in v {
                df.schedule_subgraph(subgraph);
            }
        }
        df.run_available()
    }
    pub fn get_scheduler(&self) -> Scheduler {
        Scheduler {
            schedule_subgraph: self.schedule_subgraph.clone(),
            cur_subgraph: Rc::new(RefCell::new(None)),
        }
    }

    /// return a handle to the current time, will update when `as_of` is updated
    ///
    /// so it can keep track of the current time even in a closure that is called later
    pub fn current_time_ref(&self) -> Rc<RefCell<Timestamp>> {
        self.as_of.clone()
    }

    pub fn current_ts(&self) -> Timestamp {
        *self.as_of.borrow()
    }

    pub fn set_current_ts(&mut self, ts: Timestamp) {
        self.as_of.replace(ts);
    }

    pub fn get_err_collector(&self) -> ErrCollector {
        self.err_collector.clone()
    }

    pub fn set_expire_after(&mut self, after: Option<repr::Duration>) {
        self.expire_after = after;
    }

    pub fn expire_after(&self) -> Option<Timestamp> {
        self.expire_after
    }

    pub fn get_state_size(&self) -> usize {
        self.arrange_used.iter().map(|x| x.read().get_size()).sum()
    }

    pub fn set_last_exec_time(&mut self, time: Timestamp) {
        self.last_exec_time = Some(time);
    }

    pub fn last_exec_time(&self) -> Option<Timestamp> {
        self.last_exec_time
    }
}

#[derive(Debug, Clone)]
pub struct Scheduler {
    // this scheduler is shared with `DataflowState`, so it can schedule subgraph
    schedule_subgraph: Rc<RefCell<BTreeMap<Timestamp, VecDeque<SubgraphId>>>>,
    cur_subgraph: Rc<RefCell<Option<SubgraphId>>>,
}

impl Scheduler {
    pub fn schedule_at(&self, next_run_time: Timestamp) {
        let mut schedule_subgraph = self.schedule_subgraph.borrow_mut();
        let subgraph = self.cur_subgraph.borrow();
        let subgraph = subgraph.as_ref().expect("Set SubgraphId before schedule");
        let subgraph_queue = schedule_subgraph.entry(next_run_time).or_default();
        subgraph_queue.push_back(*subgraph);
    }

    pub fn schedule_for_arrange(&self, arrange: &Arrangement, now: Timestamp) {
        if let Some(i) = arrange.get_next_update_time(&now) {
            self.schedule_at(i)
        }
    }

    pub fn set_cur_subgraph(&self, subgraph: SubgraphId) {
        self.cur_subgraph.replace(Some(subgraph));
    }
}
