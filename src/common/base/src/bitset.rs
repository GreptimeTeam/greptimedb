// Copyright 2022 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use bitvec::prelude as bv;

// `Lsb0` provides the best codegen for bit manipulation,
// see https://github.com/bitvecto-rs/bitvec/blob/main/doc/order/Lsb0.md
pub type BitVec = bv::BitVec<u8, bv::Lsb0>;
