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

use std::any::Any;
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

/// [`Plugins`] is a wrapper of [AnyMap](https://github.com/chris-morgan/anymap) and provides a thread-safe way to store and retrieve plugins.
/// Make it Cloneable and we can treat it like an Arc struct.
#[derive(Default, Clone)]
pub struct Plugins {
    inner: Arc<RwLock<anymap::Map<dyn Any + Send + Sync>>>,
}

impl Plugins {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(anymap::Map::new())),
        }
    }

    pub fn insert<T: 'static + Send + Sync>(&self, value: T) {
        let _ = self.write().insert(value);
    }

    pub fn get<T: 'static + Send + Sync + Clone>(&self) -> Option<T> {
        self.read().get::<T>().cloned()
    }

    pub fn map_mut<T: 'static + Send + Sync, F, R>(&self, mapper: F) -> R
    where
        F: FnOnce(Option<&mut T>) -> R,
    {
        let mut binding = self.write();
        let opt = binding.get_mut::<T>();
        mapper(opt)
    }

    pub fn map<T: 'static + Send + Sync, F, R>(&self, mapper: F) -> Option<R>
    where
        F: FnOnce(&T) -> R,
    {
        self.read().get::<T>().map(mapper)
    }

    pub fn len(&self) -> usize {
        self.read().len()
    }

    pub fn is_empty(&self) -> bool {
        self.read().is_empty()
    }

    fn read(&self) -> RwLockReadGuard<anymap::Map<dyn Any + Send + Sync>> {
        self.inner.read().unwrap()
    }

    fn write(&self) -> RwLockWriteGuard<anymap::Map<dyn Any + Send + Sync>> {
        self.inner.write().unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_plugins() {
        #[derive(Debug, Clone)]
        struct FooPlugin {
            x: i32,
        }

        #[derive(Debug, Clone)]
        struct BarPlugin {
            y: String,
        }

        let plugins = Plugins::new();

        let m = plugins.clone();
        let thread1 = std::thread::spawn(move || {
            m.insert(FooPlugin { x: 42 });

            if let Some(foo) = m.get::<FooPlugin>() {
                assert_eq!(foo.x, 42);
            }

            assert_eq!(m.map::<FooPlugin, _, _>(|foo| foo.x * 2), Some(84));
        });

        let m = plugins.clone();
        let thread2 = std::thread::spawn(move || {
            m.clone().insert(BarPlugin {
                y: "hello".to_string(),
            });

            if let Some(bar) = m.get::<BarPlugin>() {
                assert_eq!(bar.y, "hello");
            }

            m.map_mut::<BarPlugin, _, _>(|bar| {
                if let Some(bar) = bar {
                    bar.y = "world".to_string();
                }
            });

            assert_eq!(m.get::<BarPlugin>().unwrap().y, "world");
        });

        thread1.join().unwrap();
        thread2.join().unwrap();

        assert_eq!(plugins.len(), 2);
        assert!(!plugins.is_empty());
    }
}
