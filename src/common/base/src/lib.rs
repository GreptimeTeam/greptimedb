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

pub mod bit_vec;
pub mod buffer;
pub mod bytes;
#[allow(clippy::all)]
pub mod readable_size;

use core::any::Any;
use std::sync::{Arc, Mutex, MutexGuard};

pub use bit_vec::BitVec;

/// [`Plugins`] is a wrapper of Arc contents.
/// Make it Cloneable and we can treat it like an Arc struct.
#[derive(Default, Clone)]
pub struct Plugins {
    inner: Arc<Mutex<anymap::Map<dyn Any + Send + Sync>>>,
}

impl Plugins {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(anymap::Map::new())),
        }
    }

    fn lock(&self) -> MutexGuard<anymap::Map<dyn Any + Send + Sync>> {
        self.inner.lock().unwrap()
    }

    pub fn insert<T: 'static + Send + Sync>(&self, value: T) {
        let _ = self.lock().insert(value);
    }

    pub fn get<T: 'static + Send + Sync + Clone>(&self) -> Option<T> {
        let binding = self.lock();
        binding.get::<T>().cloned()
    }

    pub fn map_mut<T: 'static + Send + Sync, F, R>(&self, mapper: F) -> R
    where
        F: FnOnce(Option<&mut T>) -> R,
    {
        let mut binding = self.lock();
        let opt = binding.get_mut::<T>();
        mapper(opt)
    }

    pub fn map<T: 'static + Send + Sync, F, R>(&self, mapper: F) -> Option<R>
    where
        F: FnOnce(&T) -> R,
    {
        let binding = self.lock();
        binding.get::<T>().map(mapper)
    }

    pub fn len(&self) -> usize {
        let binding = self.lock();
        binding.len()
    }

    pub fn is_empty(&self) -> bool {
        let binding = self.lock();
        binding.is_empty()
    }
}
