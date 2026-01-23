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

pub mod etcd;
pub mod insert_forwarder;
#[cfg(feature = "mysql_kvbackend")]
pub mod mysql;
#[cfg(feature = "pg_kvbackend")]
pub mod postgres;

#[macro_export]
macro_rules! define_ticker {
    (
        $(#[$meta:meta])*
        $name:ident,
        event_type = $event_ty:ty,
        event_value = $event_val:expr
    ) => {
        $(#[$meta])*
        pub struct $name {
            pub tick_handle: std::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
            pub tick_interval: std::time::Duration,
            pub sender: tokio::sync::mpsc::Sender<$event_ty>,
        }

        #[async_trait::async_trait]
        impl common_meta::leadership_notifier::LeadershipChangeListener for $name {
            fn name(&self) -> &'static str {
                stringify!($name)
            }

            async fn on_leader_start(&self) -> common_meta::error::Result<()> {
                self.start();
                Ok(())
            }

            async fn on_leader_stop(&self) -> common_meta::error::Result<()> {
                self.stop();
                Ok(())
            }
        }

        impl $name {
            pub(crate) fn new(
                tick_interval: std::time::Duration,
                sender: tokio::sync::mpsc::Sender<$event_ty>,
            ) -> Self {
                Self {
                    tick_handle: std::sync::Mutex::new(None),
                    tick_interval,
                    sender,
                }
            }

            pub fn start(&self) {
                let mut handle = self.tick_handle.lock().unwrap();
                if handle.is_none() {
                    let sender = self.sender.clone();
                    let tick_interval = self.tick_interval;
                    let ticker_loop = tokio::spawn(async move {
                        let mut interval = tokio::time::interval_at(
                            tokio::time::Instant::now() + tick_interval,
                            tick_interval,
                        );
                        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                        loop {
                            interval.tick().await;
                            if sender.send($event_val).await.is_err() {
                                common_telemetry::info!("EventReceiver is dropped, tick loop is stopped");
                                break;
                            }
                        }
                    });
                    *handle = Some(ticker_loop);
                }
                common_telemetry::info!("{} started.", stringify!($name));
            }

            pub fn stop(&self) {
                let mut handle = self.tick_handle.lock().unwrap();
                if let Some(handle) = handle.take() {
                    handle.abort();
                }
                common_telemetry::info!("{} stopped.", stringify!($name));
            }
        }

        impl Drop for $name {
            fn drop(&mut self) {
                self.stop();
            }
        }
    };
}
