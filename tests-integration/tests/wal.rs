// // Copyright 2023 Greptime Team
// //
// // Licensed under the Apache License, Version 2.0 (the "License");
// // you may not use this file except in compliance with the License.
// // You may obtain a copy of the License at
// //
// //     http://www.apache.org/licenses/LICENSE-2.0
// //
// // Unless required by applicable law or agreed to in writing, software
// // distributed under the License is distributed on an "AS IS" BASIS,
// // WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// // See the License for the specific language governing permissions and
// // limitations under the License.

// use std::collections::HashMap;
// use std::sync::Arc;

// use common_config::wal::KafkaConfig as DatanodeKafkaConfig;
// use common_config::{KafkaWalOptions, WalOptions};
// use common_meta::kv_backend::memory::MemoryKvBackend;
// use common_meta::kv_backend::KvBackendRef;
// use common_meta::wal::kafka::{
//     KafkaConfig as MetaSrvKafkaConfig, TopicManager as KafkaTopicManager,
// };
// use common_meta::wal::{allocate_region_wal_options, WalConfig, WalOptionsAllocator};
// use futures::StreamExt;
// use log_store::error::Result as LogStoreResult;
// use log_store::kafka::log_store::KafkaLogStore;
// use log_store::kafka::{EntryImpl, NamespaceImpl};
// use rskafka::client::controller::ControllerClient;
// use rskafka::client::ClientBuilder;
// use store_api::logstore::entry::Id as EntryId;
// use store_api::logstore::LogStore;
// use tests_integration::wal_util::{DockerCli, KafkaImage, DEFAULT_EXPOSED_PORT};

// // Notice: the following tests are literally unit tests. They are placed at here since
// // it seems too heavy to start a Kafka cluster for each unit test.

// // The key of an env variable that stores a series of Kafka broker endpoints.
// const BROKER_ENDPOINTS_KEY: &str = "GT_KAFKA_ENDPOINTS";

// // Tests that the TopicManager allocates topics in a round-robin mannar.
// #[tokio::test]
// async fn test_kafka_alloc_topics() {
//     let broker_endpoints = std::env::var(BROKER_ENDPOINTS_KEY)
//         .unwrap()
//         .split(',')
//         .map(ToString::to_string)
//         .collect::<Vec<_>>();
//     let config = MetaSrvKafkaConfig {
//         topic_name_prefix: "__test_kafka_alloc_topics".to_string(),
//         replication_factor: broker_endpoints.len() as i16,
//         broker_endpoints,
//         ..Default::default()
//     };
//     let kv_backend = Arc::new(MemoryKvBackend::new()) as KvBackendRef;
//     let manager = KafkaTopicManager::new(config.clone(), kv_backend);
//     manager.start().await.unwrap();

//     // Topics should be created.
//     let topics = (0..config.num_topics)
//         .map(|topic_id| format!("{}_{topic_id}", config.topic_name_prefix))
//         .collect::<Vec<_>>();

//     // Selects exactly the number of `num_topics` topics one by one.
//     for expected in topics.iter() {
//         let got = manager.select().unwrap();
//         assert_eq!(got, expected);
//     }

//     // Selects exactly the number of `num_topics` topics in a batching manner.
//     let got = manager
//         .select_batch(config.num_topics)
//         .unwrap()
//         .into_iter()
//         .map(ToString::to_string)
//         .collect::<Vec<_>>();
//     assert_eq!(got, topics);

//     // Selects none.
//     let got = manager.select_batch(config.num_topics).unwrap();
//     assert!(got.is_empty());

//     // Selects more than the number of `num_topics` topics.
//     let got = manager
//         .select_batch(2 * config.num_topics)
//         .unwrap()
//         .into_iter()
//         .map(ToString::to_string)
//         .collect::<Vec<_>>();
//     let expected = vec![topics.clone(); 2]
//         .into_iter()
//         .flatten()
//         .collect::<Vec<_>>();
//     assert_eq!(got, expected);
// }

// // Tests that the wal options allocator could successfully allocate Kafka wal options.
// #[tokio::test]
// async fn test_kafka_options_allocator() {
//     let broker_endpoints = std::env::var(BROKER_ENDPOINTS_KEY)
//         .unwrap()
//         .split(',')
//         .map(ToString::to_string)
//         .collect::<Vec<_>>();
//     let config = MetaSrvKafkaConfig {
//         topic_name_prefix: "__test_kafka_options_allocator".to_string(),
//         replication_factor: broker_endpoints.len() as i16,
//         broker_endpoints,
//         ..Default::default()
//     };
//     let wal_config = WalConfig::Kafka(config.clone());
//     let kv_backend = Arc::new(MemoryKvBackend::new()) as KvBackendRef;
//     let allocator = WalOptionsAllocator::new(wal_config, kv_backend);
//     allocator.start().await.unwrap();

//     let num_regions = 32;
//     let regions = (0..num_regions).collect::<Vec<_>>();
//     let got = allocate_region_wal_options(regions.clone(), &allocator).unwrap();

//     // Topics should be allocated.
//     let topics = (0..num_regions)
//         .map(|topic_id| format!("{}_{topic_id}", config.topic_name_prefix))
//         .collect::<Vec<_>>();
//     // Check the allocated wal options contain the expected topics.
//     let expected = (0..num_regions)
//         .map(|i| {
//             let options = WalOptions::Kafka(KafkaWalOptions {
//                 topic: topics[i as usize].clone(),
//             });
//             (i, serde_json::to_string(&options).unwrap())
//         })
//         .collect::<HashMap<_, _>>();
//     assert_eq!(got, expected);
// }

// async fn create_topic(topic: &str, replication_factor: i16, client: &ControllerClient) {
//     client
//         .create_topic(topic, 1, replication_factor, 500)
//         .await
//         .unwrap();
// }

// async fn check_entries(
//     ns: &NamespaceImpl,
//     start_offset: EntryId,
//     expected: Vec<EntryImpl>,
//     logstore: &KafkaLogStore,
// ) {
//     let stream = logstore.read(ns, start_offset).await.unwrap();
//     let got = stream
//         .collect::<Vec<_>>()
//         .await
//         .into_iter()
//         .flat_map(|x| x.unwrap())
//         .collect::<Vec<_>>();
//     assert_eq!(expected, got);
//     // for entry in expected {
//     //     let got = stream.next().await.unwrap().unwrap();
//     // }
// }

// // Tests that the Kafka log store is able to write and read log entries from Kafka.
// // #[tokio::test]
// // async fn test_kafka_log_store() {
// //     println!("Start running test");

// //     // Starts a Kafka container.
// //     let docker = DockerCli::default();
// //     let container = docker.run(KafkaImage::default());

// //     println!("Started the container");

// //     let broker_endpoints = std::env::var(BROKER_ENDPOINTS_KEY)
// //         .unwrap_or(format!(
// //             "localhost:{}",
// //             container.get_host_port_ipv4(DEFAULT_EXPOSED_PORT)
// //         ))
// //         .split(',')
// //         .map(ToString::to_string)
// //         .collect::<Vec<_>>();
// //     let config = DatanodeKafkaConfig {
// //         broker_endpoints,
// //         ..Default::default()
// //     };
// //     let logstore = KafkaLogStore::try_new(&config).await.unwrap();

// //     println!("Started the log store");

// //     let client = ClientBuilder::new(config.broker_endpoints.clone())
// //         .build()
// //         .await
// //         .unwrap()
// //         .controller_client()
// //         .unwrap();

// //     println!("Created a client");

// //     // Appends one entry.
// //     let topic = "__test_kafka_log_store_topic_append";
// //     create_topic(topic, config.broker_endpoints.len() as i16, &client).await;

// //     println!("Created a topic");

// //     let ns = NamespaceImpl {
// //         region_id: 0,
// //         topic: topic.to_string(),
// //     };
// //     let entry = new_test_entry(b"0", 0, ns.clone());
// //     let last_entry_id = logstore.append(entry.clone()).await.unwrap().last_entry_id;

// //     println!("Appended an entry");

// //     check_entries(&ns, last_entry_id, vec![entry], &logstore).await;

// //     // Appends a batch of entries.
// //     // Region 1, 2 are mapped to topic 1,
// //     let topic = "__test_kafka_log_store_topic_append_batch_1";
// //     create_topic(topic, config.broker_endpoints.len() as i16, &client).await;

// //     println!("Created a topic");

// //     let ns_1 = NamespaceImpl {
// //         region_id: 1,
// //         topic: topic.to_string(),
// //     };
// //     let ns_2 = NamespaceImpl {
// //         region_id: 2,
// //         topic: topic.to_string(),
// //     };

// //     // Region 3 is mapped to topic 2.
// //     let topic = "__test_kafka_log_store_topic_append_batch_2";
// //     create_topic(topic, config.broker_endpoints.len() as i16, &client).await;

// //     println!("Created a topic");

// //     let ns_3 = NamespaceImpl {
// //         region_id: 3,
// //         topic: topic.to_string(),
// //     };

// //     // Constructs a batch of entries.
// //     let entries_1 = vec![
// //         new_test_entry(b"1", 0, ns_1.clone()),
// //         new_test_entry(b"1", 1, ns_1.clone()),
// //     ];
// //     let entries_2 = vec![
// //         new_test_entry(b"2", 2, ns_2.clone()),
// //         new_test_entry(b"2", 3, ns_2.clone()),
// //     ];
// //     let entries_3 = vec![
// //         new_test_entry(b"3", 7, ns_3.clone()),
// //         new_test_entry(b"3", 8, ns_3.clone()),
// //     ];
// //     let entries = vec![entries_1.clone(), entries_2.clone(), entries_3.clone()]
// //         .into_iter()
// //         .flatten()
// //         .collect::<Vec<_>>();

// //     let last_entry_ids = logstore
// //         .append_batch(entries.clone())
// //         .await
// //         .unwrap()
// //         .last_entry_ids;

// //     // Reads entries for region 1.
// //     check_entries(&ns_1, last_entry_ids[&1], entries_1, &logstore).await;
// //     // Reads entries from region 2.
// //     check_entries(&ns_2, last_entry_ids[&2], entries_2, &logstore).await;
// //     // Reads entries from region 3.
// //     check_entries(&ns_3, last_entry_ids[&3], entries_3, &logstore).await;
// // }
