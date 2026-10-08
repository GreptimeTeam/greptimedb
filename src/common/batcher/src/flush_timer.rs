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

use std::pin::Pin;

use tokio::time::{Instant, Sleep, sleep_until};

/// Maintains one reusable timer for the current flush deadline.
///
/// Creating a timer does not require a Tokio runtime. Arming it requires a Tokio
/// runtime with time enabled. Disabling retains the allocation for the next arm.
#[derive(Default)]
pub struct FlushTimer {
    deadline: Option<Instant>,
    sleep: Option<Pin<Box<Sleep>>>,
}

impl FlushTimer {
    /// Creates a disabled timer without allocating a Tokio sleep.
    pub fn new() -> Self {
        Self::default()
    }

    /// Arms or disables the timer, resetting its sleep only when the deadline changes.
    ///
    /// # Panics
    ///
    /// Arming for the first time panics outside a Tokio runtime with time enabled.
    pub fn set_deadline(&mut self, deadline: Option<Instant>) {
        if self.deadline == deadline {
            return;
        }
        self.deadline = deadline;
        if let Some(deadline) = deadline {
            match &mut self.sleep {
                Some(sleep) => sleep.as_mut().reset(deadline),
                None => self.sleep = Some(Box::pin(sleep_until(deadline))),
            }
        }
    }

    /// Waits until the armed deadline, or indefinitely while disabled.
    ///
    /// Cancelling this wait does not change the deadline. An elapsed timer stays
    /// ready until the caller changes or disables its deadline.
    pub async fn wait(&mut self) {
        if self.deadline.is_some()
            && let Some(sleep) = &mut self.sleep
        {
            sleep.as_mut().await;
        } else {
            std::future::pending::<()>().await;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::future::{Future, poll_fn};
    use std::task::Poll;
    use std::time::Duration;

    use super::*;

    async fn is_ready(timer: &mut FlushTimer) -> bool {
        let wait = timer.wait();
        tokio::pin!(wait);
        poll_fn(|cx| Poll::Ready(wait.as_mut().poll(cx).is_ready())).await
    }

    #[test]
    fn test_create_without_runtime() {
        let mut timer = FlushTimer::new();
        timer.set_deadline(None);
        assert!(timer.sleep.is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn test_deadline_and_cancelled_wait() {
        let mut timer = FlushTimer::new();
        assert!(!is_ready(&mut timer).await);
        let deadline = Instant::now() + Duration::from_millis(10);
        timer.set_deadline(Some(deadline));
        assert!(!is_ready(&mut timer).await);
        tokio::time::advance(Duration::from_millis(9)).await;
        // Each poll drops its wait future. Repeating the same deadline must not
        // move the timer forward or replace its sleep allocation.
        let sleep = timer.sleep.as_ref().unwrap().as_ref().get_ref() as *const Sleep;
        timer.set_deadline(Some(deadline));
        assert_eq!(
            sleep,
            timer.sleep.as_ref().unwrap().as_ref().get_ref() as *const Sleep
        );
        assert!(!is_ready(&mut timer).await);
        tokio::time::advance(Duration::from_millis(1)).await;
        assert!(is_ready(&mut timer).await);
    }

    #[tokio::test(start_paused = true)]
    async fn test_disable_and_rearm_reuses_sleep() {
        let mut timer = FlushTimer::new();
        timer.set_deadline(Some(Instant::now()));
        assert!(is_ready(&mut timer).await);
        let sleep = timer.sleep.as_ref().unwrap().as_ref().get_ref() as *const Sleep;
        timer.set_deadline(None);
        assert!(!is_ready(&mut timer).await);
        assert!(!is_ready(&mut timer).await);
        timer.set_deadline(Some(Instant::now() + Duration::from_millis(5)));
        assert_eq!(
            sleep,
            timer.sleep.as_ref().unwrap().as_ref().get_ref() as *const Sleep
        );
        assert!(!is_ready(&mut timer).await);
        tokio::time::advance(Duration::from_millis(5)).await;
        assert!(is_ready(&mut timer).await);
    }

    #[tokio::test(start_paused = true)]
    async fn test_change_armed_deadline() {
        let mut timer = FlushTimer::new();
        let now = Instant::now();
        timer.set_deadline(Some(now + Duration::from_millis(5)));
        assert!(!is_ready(&mut timer).await);
        timer.set_deadline(Some(now + Duration::from_millis(10)));
        tokio::time::advance(Duration::from_millis(5)).await;
        assert!(!is_ready(&mut timer).await);
        timer.set_deadline(Some(Instant::now()));
        assert!(is_ready(&mut timer).await);
    }
}
