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

//! Common types.

/// Represents a sequence number of data in storage. The offset of logstore can be used
/// as a sequence number.
pub type SequenceNumber = u64;

// impl OpType {
//     /// Cast the [OpType] to u8.
//     #[inline]
//     pub fn as_u8(&self) -> u8 {
//         *self as u8
//     }
//
//     /// Minimal op type after casting to u8.
//     pub const fn min_type() -> OpType {
//         OpType::Delete
//     }
// }
//
// #[cfg(test)]
// mod tests {
//     use super::*;
//
//     #[test]
//     fn test_op_type() {
//         assert_eq!(0, OpType::Delete.as_u8());
//         assert_eq!(1, OpType::Put.as_u8());
//         assert_eq!(0, OpType::min_type().as_u8());
//     }
// }
