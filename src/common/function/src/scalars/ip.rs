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

mod cidr;
mod ipv4;
mod ipv6;
mod range;

use cidr::{Ipv4ToCidr, Ipv6ToCidr};
use ipv4::{Ipv4NumToString, Ipv4StringToNum};
use ipv6::{Ipv6NumToString, Ipv6StringToNum};
use range::{Ipv4InRange, Ipv6InRange};

use crate::function_registry::FunctionRegistry;

pub(crate) struct IpFunctions;

impl IpFunctions {
    pub fn register(registry: &FunctionRegistry) {
        // Register IPv4 functions
        registry.register_scalar(Ipv4NumToString::default());
        registry.register_scalar(Ipv4StringToNum::default());
        registry.register_scalar(Ipv4ToCidr::default());
        registry.register_scalar(Ipv4InRange::default());

        // Register IPv6 functions
        registry.register_scalar(Ipv6NumToString::default());
        registry.register_scalar(Ipv6StringToNum::default());
        registry.register_scalar(Ipv6ToCidr::default());
        registry.register_scalar(Ipv6InRange::default());
    }
}
