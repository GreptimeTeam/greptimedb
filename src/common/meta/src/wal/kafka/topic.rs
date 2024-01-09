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

/// Kafka wal topic.
/// Publishers publish log entries to the topic while subscribers pull log entries from the topic.
/// A topic is simply a string right now. But it may be more complex in the future.
// TODO(niebayes): remove the Topic alias.
pub type Topic = String;
