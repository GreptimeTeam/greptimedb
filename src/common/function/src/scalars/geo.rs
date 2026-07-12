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

mod geohash;
mod h3;
pub(crate) mod helpers;
mod measure;
mod relation;
mod s2;
mod wkt;

use crate::function_registry::FunctionRegistry;

pub(crate) struct GeoFunctions;

impl GeoFunctions {
    pub fn register(registry: &FunctionRegistry) {
        // geohash
        registry.register_scalar(geohash::GeohashFunction::default());
        registry.register_scalar(geohash::GeohashNeighboursFunction::default());

        // h3 index
        registry.register_scalar(h3::H3LatLngToCell::default());
        registry.register_scalar(h3::H3LatLngToCellString::default());

        // h3 index inspection
        registry.register_scalar(h3::H3CellBase::default());
        registry.register_scalar(h3::H3CellIsPentagon::default());
        registry.register_scalar(h3::H3StringToCell::default());
        registry.register_scalar(h3::H3CellToString::default());
        registry.register_scalar(h3::H3CellCenterLatLng::default());
        registry.register_scalar(h3::H3CellResolution::default());

        // h3 hierarchical grid
        registry.register_scalar(h3::H3CellCenterChild::default());
        registry.register_scalar(h3::H3CellParent::default());
        registry.register_scalar(h3::H3CellToChildren::default());
        registry.register_scalar(h3::H3CellToChildrenSize::default());
        registry.register_scalar(h3::H3CellToChildPos::default());
        registry.register_scalar(h3::H3ChildPosToCell::default());
        registry.register_scalar(h3::H3CellContains::default());

        // h3 grid traversal
        registry.register_scalar(h3::H3GridDisk::default());
        registry.register_scalar(h3::H3GridDiskDistances::default());
        registry.register_scalar(h3::H3GridDistance::default());
        registry.register_scalar(h3::H3GridPathCells::default());

        // h3 measurement
        registry.register_scalar(h3::H3CellDistanceSphereKm::default());
        registry.register_scalar(h3::H3CellDistanceEuclideanDegree::default());

        // s2
        registry.register_scalar(s2::S2LatLngToCell::default());
        registry.register_scalar(s2::S2CellLevel::default());
        registry.register_scalar(s2::S2CellToToken::default());
        registry.register_scalar(s2::S2CellParent::default());

        // spatial data type
        registry.register_scalar(wkt::LatLngToPointWkt::default());

        // spatial relation
        registry.register_scalar(relation::STContains::default());
        registry.register_scalar(relation::STWithin::default());
        registry.register_scalar(relation::STIntersects::default());

        // spatial measure
        registry.register_scalar(measure::STDistance::default());
        registry.register_scalar(measure::STDistanceSphere::default());
        registry.register_scalar(measure::STArea::default());
    }
}
