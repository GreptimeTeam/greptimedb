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
use std::sync::Arc;

use hydroflow::scheduled::graph::Hydroflow;
use hydroflow::scheduled::handoff::TeeingHandoff;
use hydroflow::scheduled::port::RecvPort;
use hydroflow::scheduled::SubgraphId;
use itertools::Itertools;
use tokio::sync::Mutex;

use crate::expr::{Batch, EvalError, ScalarExpr};
use crate::repr::DiffRow;
use crate::utils::ArrangeHandler;

pub type Toff<T = DiffRow> = TeeingHandoff<T>;

/// A collection, represent a collections of data that is received from a handoff.
pub struct Collection<T: 'static> {
    /// represent a stream of updates recv from this port
    stream: RecvPort<TeeingHandoff<T>>,
}

impl<T: 'static + Clone> Collection<T> {
    pub fn from_port(port: RecvPort<TeeingHandoff<T>>) -> Self {
        Collection { stream: port }
    }

    /// clone a collection, require a mutable reference to the hydroflow instance
    ///
    /// Note: need to be the same hydroflow instance that this collection is created from
    pub fn clone(&self, df: &mut Hydroflow) -> Self {
        Collection {
            stream: self.stream.tee(df),
        }
    }

    pub fn into_inner(self) -> RecvPort<TeeingHandoff<T>> {
        self.stream
    }
}

/// Arranged is a wrapper around `ArrangeHandler` that maintain a list of readers and a writer
pub struct Arranged {
    pub arrangement: ArrangeHandler,
    pub writer: Rc<RefCell<Option<SubgraphId>>>,
    /// maintain a list of readers for the arrangement for the ease of scheduling
    pub readers: Rc<RefCell<Vec<SubgraphId>>>,
}

impl Arranged {
    pub fn new(arr: ArrangeHandler) -> Self {
        Self {
            arrangement: arr,
            writer: Default::default(),
            readers: Default::default(),
        }
    }

    /// Copy it's future only updates, internally `Rc-ed` so it's cheap to copy
    pub fn try_copy_future(&self) -> Option<Self> {
        self.arrangement
            .clone_future_only()
            .map(|arrangement| Arranged {
                arrangement,
                readers: self.readers.clone(),
                writer: self.writer.clone(),
            })
    }

    /// Copy the full arrangement, including the future and the current updates.
    ///
    /// Internally `Rc-ed` so it's cheap to copy
    pub fn try_copy_full(&self) -> Option<Self> {
        self.arrangement
            .clone_full_arrange()
            .map(|arrangement| Arranged {
                arrangement,
                readers: self.readers.clone(),
                writer: self.writer.clone(),
            })
    }
    pub fn add_reader(&self, id: SubgraphId) {
        self.readers.borrow_mut().push(id)
    }
}

/// A bundle of the various ways a collection can be represented.
///
/// This type maintains the invariant that it does contain at least one(or both) valid
/// source of data, either a collection or at least one arrangement. This is for convenience
/// of reading the data from the collection.
///
// TODO(discord9): make T default to Batch and obsolete the Row Mode
pub struct CollectionBundle<T: 'static = DiffRow> {
    /// This is useful for passively reading the new updates from the collection
    ///
    /// Invariant: the timestamp of the updates should always not greater than now, since future updates should be stored in the arrangement
    pub collection: Collection<T>,
    /// the key [`ScalarExpr`] indicate how the keys(also a [`Row`]) used in Arranged is extract from collection's [`Row`]
    /// So it is the "index" of the arrangement
    ///
    /// The `Arranged` is the actual data source, it can be used to read the data from the collection by
    /// using the key indicated by the `Vec<ScalarExpr>`
    /// There is a false positive in using `Vec<ScalarExpr>` as key due to `ScalarExpr::Literal`
    /// contain a `Value` which have `bytes` variant
    #[allow(clippy::mutable_key_type)]
    pub arranged: BTreeMap<Vec<ScalarExpr>, Arranged>,
}

pub trait GenericBundle {
    fn is_batch(&self) -> bool;

    fn try_as_batch(&self) -> Option<&CollectionBundle<Batch>> {
        None
    }

    fn try_as_row(&self) -> Option<&CollectionBundle<DiffRow>> {
        None
    }
}

impl GenericBundle for CollectionBundle<Batch> {
    fn is_batch(&self) -> bool {
        true
    }

    fn try_as_batch(&self) -> Option<&CollectionBundle<Batch>> {
        Some(self)
    }
}

impl GenericBundle for CollectionBundle<DiffRow> {
    fn is_batch(&self) -> bool {
        false
    }

    fn try_as_row(&self) -> Option<&CollectionBundle<DiffRow>> {
        Some(self)
    }
}

impl<T: 'static> CollectionBundle<T> {
    pub fn from_collection(collection: Collection<T>) -> Self {
        Self {
            collection,
            arranged: BTreeMap::default(),
        }
    }
}

impl<T: 'static + Clone> CollectionBundle<T> {
    pub fn clone(&self, df: &mut Hydroflow) -> Self {
        Self {
            collection: self.collection.clone(df),
            arranged: self
                .arranged
                .iter()
                .map(|(k, v)| (k.clone(), v.try_copy_future().unwrap()))
                .collect(),
        }
    }
}

/// A thread local error collector, used to collect errors during the evaluation of the plan
///
/// usually only the first error matters, but store all of them just in case
///
/// Using a `VecDeque` to preserve the order of errors
/// when running dataflow continuously and need errors in order
#[derive(Debug, Default, Clone)]
pub struct ErrCollector {
    pub inner: Arc<Mutex<VecDeque<EvalError>>>,
}

impl ErrCollector {
    pub fn get_all_blocking(&self) -> Vec<EvalError> {
        self.inner.blocking_lock().drain(..).collect_vec()
    }
    pub async fn get_all(&self) -> Vec<EvalError> {
        self.inner.lock().await.drain(..).collect_vec()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.blocking_lock().is_empty()
    }

    pub fn push_err(&self, err: EvalError) {
        self.inner.blocking_lock().push_back(err)
    }

    pub fn run<F, R>(&self, f: F) -> Option<R>
    where
        F: FnOnce() -> Result<R, EvalError>,
    {
        match f() {
            Ok(r) => Some(r),
            Err(e) => {
                self.push_err(e);
                None
            }
        }
    }
}
