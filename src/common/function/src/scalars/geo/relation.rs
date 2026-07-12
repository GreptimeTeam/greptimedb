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

use std::fmt::Display;
use std::sync::Arc;

use datafusion_common::arrow::array::{Array, AsArray, BooleanBuilder};
use datafusion_common::arrow::compute;
use datafusion_common::arrow::datatypes::DataType;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, Signature, Volatility};
use derive_more::Display;
use geo::algorithm::contains::Contains;
use geo::algorithm::intersects::Intersects;
use geo::algorithm::within::Within;
use geo_types::Geometry;

use crate::function::{Function, extract_args};
use crate::scalars::geo::wkt::parse_wkt;

/// Test if spatial relationship: contains
#[derive(Clone, Debug, Display)]
#[display("{}", self.name())]
pub(crate) struct STContains {
    signature: Signature,
}

impl Default for STContains {
    fn default() -> Self {
        Self {
            signature: Signature::string(2, Volatility::Stable),
        }
    }
}

impl StFunction for STContains {
    const NAME: &'static str = "st_contains";

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn invoke(g1: Geometry, g2: Geometry) -> bool {
        g1.contains(&g2)
    }
}

/// Test if spatial relationship: within
#[derive(Clone, Debug, Display)]
#[display("{}", self.name())]
pub(crate) struct STWithin {
    signature: Signature,
}

impl Default for STWithin {
    fn default() -> Self {
        Self {
            signature: Signature::string(2, Volatility::Stable),
        }
    }
}

impl StFunction for STWithin {
    const NAME: &'static str = "st_within";

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn invoke(g1: Geometry, g2: Geometry) -> bool {
        g1.is_within(&g2)
    }
}

/// Test if spatial relationship: within
#[derive(Clone, Debug, Display)]
#[display("{}", self.name())]
pub(crate) struct STIntersects {
    signature: Signature,
}

impl Default for STIntersects {
    fn default() -> Self {
        Self {
            signature: Signature::string(2, Volatility::Stable),
        }
    }
}

impl StFunction for STIntersects {
    const NAME: &'static str = "st_intersects";

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn invoke(g1: Geometry, g2: Geometry) -> bool {
        g1.intersects(&g2)
    }
}

trait StFunction {
    const NAME: &'static str;

    fn signature(&self) -> &Signature;

    fn invoke(g1: Geometry, g2: Geometry) -> bool;
}

impl<T: StFunction + Display + Send + Sync> Function for T {
    fn name(&self) -> &str {
        T::NAME
    }

    fn return_type(&self, _: &[DataType]) -> datafusion_common::Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn signature(&self) -> &Signature {
        self.signature()
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion_common::Result<ColumnarValue> {
        let [arg0, arg1] = extract_args(self.name(), &args)?;

        let arg0 = compute::cast(&arg0, &DataType::Utf8View)?;
        let wkt_this_vec = arg0.as_string_view();
        let arg1 = compute::cast(&arg1, &DataType::Utf8View)?;
        let wkt_that_vec = arg1.as_string_view();

        let size = wkt_this_vec.len();
        let mut builder = BooleanBuilder::with_capacity(size);

        for i in 0..size {
            let wkt_this = wkt_this_vec.is_valid(i).then(|| wkt_this_vec.value(i));
            let wkt_that = wkt_that_vec.is_valid(i).then(|| wkt_that_vec.value(i));

            let result = match (wkt_this, wkt_that) {
                (Some(wkt_this), Some(wkt_that)) => {
                    Some(T::invoke(parse_wkt(wkt_this)?, parse_wkt(wkt_that)?))
                }
                _ => None,
            };

            builder.append_option(result);
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}
