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

pub mod error;
pub mod information_extension;
pub mod metadata;
pub mod options;
pub mod procedure;

pub use information_extension::StandaloneInformationExtension;
pub use metadata::{build_metadata_kv_from_url, build_metadata_kvbackend};
pub use procedure::{StandaloneRepartitionProcedureFactory, build_procedure_manager};
