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

use std::sync::Arc;
pub(crate) mod encoding;
mod geohash;
mod h3;
mod helpers;

use geohash::{GeohashFunction, GeohashNeighboursFunction};

use crate::function_registry::FunctionRegistry;

pub(crate) struct GeoFunctions;

impl GeoFunctions {
    pub fn register(registry: &FunctionRegistry) {
        // geohash
        registry.register(Arc::new(GeohashFunction));
        registry.register(Arc::new(GeohashNeighboursFunction));

        // h3 index
        registry.register(Arc::new(h3::H3LatLngToCell));
        registry.register(Arc::new(h3::H3LatLngToCellString));

        // h3 index inspection
        registry.register(Arc::new(h3::H3CellBase));
        registry.register(Arc::new(h3::H3CellIsPentagon));
        registry.register(Arc::new(h3::H3StringToCell));
        registry.register(Arc::new(h3::H3CellToString));
        registry.register(Arc::new(h3::H3CellCenterLat));
        registry.register(Arc::new(h3::H3CellCenterLng));
        registry.register(Arc::new(h3::H3CellResolution));

        // h3 hierarchical grid
        registry.register(Arc::new(h3::H3CellCenterChild));
        registry.register(Arc::new(h3::H3CellParent));
        registry.register(Arc::new(h3::H3CellToChildren));
        registry.register(Arc::new(h3::H3CellToChildrenSize));
        registry.register(Arc::new(h3::H3CellToChildPos));
        registry.register(Arc::new(h3::H3ChildPosToCell));

        // h3 grid traversal
    }
}
