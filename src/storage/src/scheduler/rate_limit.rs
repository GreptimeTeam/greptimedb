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

use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use crate::error::{RateLimitedSnafu, Result};

use super::Request;

pub trait RateLimitToken {
    /// Releases the token.
    /// ### Note
    /// Implementation should guarantee the idempotency.
    fn try_release(&self);
}

pub type BoxedRateLimitToken = Box<dyn RateLimitToken + Send + Sync>;

impl<T: RateLimitToken + ?Sized> RateLimitToken for Box<T> {
    fn try_release(&self) {
        (**self).try_release()
    }
}

/// Rate limiter
pub trait RateLimiter {

    /// Acquires a token from rate limiter. Returns `Err` on failure.  
    fn acquire_token(&self, req: &Box<dyn Request>) -> Result<BoxedRateLimitToken>;
}

pub type BoxedRateLimiter = Box<dyn RateLimiter + Send + Sync>;

/// Limits max inflight tasks number.
pub struct MaxInflightTaskLimiter {
    max_inflight_tasks: usize,
    inflight_tasks: Arc<AtomicUsize>,
    _phantom_data: PhantomData<Box<dyn Request>>,
}

impl MaxInflightTaskLimiter {
    pub fn new(max_inflight_tasks: usize) -> Self {
        Self {
            max_inflight_tasks,
            inflight_tasks: Arc::new(AtomicUsize::new(0)),
            _phantom_data: Default::default(),
        }
    }
}

impl RateLimiter for MaxInflightTaskLimiter {

    fn acquire_token(&self, _: &Box<dyn Request>) -> Result<BoxedRateLimitToken> {
        if self.inflight_tasks.fetch_add(1, Ordering::Relaxed) >= self.max_inflight_tasks {
            let _ = self.inflight_tasks.fetch_sub(1, Ordering::Relaxed);
            return RateLimitedSnafu {
                msg: format!(
                    "Max inflight task num exceeds, current: {}, max: {}",
                    self.inflight_tasks.load(Ordering::Relaxed),
                    self.max_inflight_tasks
                ),
            }
            .fail();
        }

        Ok(Box::new(MaxInflightLimiterToken::new(
            self.inflight_tasks.clone(),
        )))
    }
}

pub struct MaxInflightLimiterToken {
    counter: Arc<AtomicUsize>,
    released: AtomicBool,
}

impl MaxInflightLimiterToken {
    pub fn new(counter: Arc<AtomicUsize>) -> Self {
        Self {
            counter,
            released: AtomicBool::new(false),
        }
    }
}

impl RateLimitToken for MaxInflightLimiterToken {
    fn try_release(&self) {
        if self
            .released
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_ok()
        {
            let _ = self.counter.fetch_sub(1, Ordering::Relaxed);
        }
    }
}

/// A composite rate limiter that allows token acquisition only when all internal limiters allow.
pub struct CascadeRateLimiter {
    limits: Vec<BoxedRateLimiter>,
}

impl CascadeRateLimiter {
    pub fn new(limits: Vec<BoxedRateLimiter>) -> Self {
        Self { limits }
    }
}

impl RateLimiter for CascadeRateLimiter {

    fn acquire_token(&self, req: &Box<dyn Request>) -> Result<BoxedRateLimitToken> {
        let mut res = vec![];
        for limit in &self.limits {
            match limit.acquire_token(req) {
                Ok(token) => {
                    res.push(token);
                }
                Err(e) => {
                    res.iter().for_each(RateLimitToken::try_release);
                    return Err(e);
                }
            }
        }
        Ok(Box::new(CompositeToken { tokens: res }))
    }
}

/// Composite token that releases all acquired token when released.
pub struct CompositeToken {
    tokens: Vec<BoxedRateLimitToken>,
}

impl RateLimitToken for CompositeToken {
    fn try_release(&self) {
        for token in &self.tokens {
            token.try_release();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // #[test]
    // fn test_max_inflight_limiter() {
    //     let limiter = MaxInflightTaskLimiter::new(3);
    //     let t1 = limiter.acquire_token(&1).unwrap();
    //     assert_eq!(1, limiter.inflight_tasks.load(Ordering::Relaxed));
    //     let _t2 = limiter.acquire_token(&1).unwrap();
    //     assert_eq!(2, limiter.inflight_tasks.load(Ordering::Relaxed));
    //     let _t3 = limiter.acquire_token(&1).unwrap();
    //     assert_eq!(3, limiter.inflight_tasks.load(Ordering::Relaxed));
    //     assert!(limiter.acquire_token(&1).is_err());
    //     t1.try_release();
    //     assert_eq!(2, limiter.inflight_tasks.load(Ordering::Relaxed));
    //     let _t4 = limiter.acquire_token(&1).unwrap();
    // }

    // #[test]
    // fn test_cascade_limiter() {
    //     let limiter: CascadeRateLimiter<usize> =
    //         CascadeRateLimiter::new(vec![Box::new(MaxInflightTaskLimiter::new(3))]);
    //     let t1 = limiter.acquire_token(&1).unwrap();
    //     let _t2 = limiter.acquire_token(&1).unwrap();
    //     let _t3 = limiter.acquire_token(&1).unwrap();
    //     assert!(limiter.acquire_token(&1).is_err());
    //     t1.try_release();
    //     let _t4 = limiter.acquire_token(&1).unwrap();
    // }
}
