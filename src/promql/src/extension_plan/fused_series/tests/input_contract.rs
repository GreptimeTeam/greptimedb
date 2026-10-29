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

use super::*;
use crate::extension_plan::SeriesRepartitionExec;

/// Producer ordering arguments cannot override the ordering declared by a frame schema.
#[test]
fn supplied_ordering_must_match_each_frame_declaration() {
    for source_count in [1, 2] {
        let tree = if source_count == 1 {
            rate_tree()
        } else {
            binary_tree_over_sources(BinaryOp::Add, raw_schema(), raw_schema(), 0, 1)
        };
        let node = FusedSeries::try_new(
            Arc::from("ordering contract"),
            tree.clone(),
            binding(&tree),
            (0..source_count)
                .map(|_| input_plan(frame_schema()))
                .collect(),
            if source_count == 1 {
                output_schema().to_dfschema_ref().unwrap()
            } else {
                binary_output_schema().to_dfschema_ref().unwrap()
            },
        )
        .unwrap();
        let inputs = (0..source_count)
            .map(|_| {
                (
                    memory_exec(Vec::new()),
                    RawLabelSource::Explicit,
                    SeriesOrdering::KeyThenTime,
                )
            })
            .collect();
        let error = node
            .to_execution_plan(inputs)
            .expect_err("the supplied ordering disagrees with the frame's declaration");
        assert!(error.to_string().contains("ordering"), "{error}");
    }
}

/// The physical adapter rejects a raw frame whose field layout differs from the tree input.
#[test]
fn raw_frame_fields_must_match_the_bound_source() {
    let tree = rate_tree();
    let node = FusedSeries::try_new(
        Arc::from("field contract"),
        tree.clone(),
        binding(&tree),
        vec![input_plan(frame_schema())],
        output_schema().to_dfschema_ref().unwrap(),
    )
    .unwrap();
    let error = node
        .to_execution_plan(vec![(
            memory_exec_with(frame_schema_of(&two_field_schema()), Vec::new()),
            RawLabelSource::Explicit,
            SeriesOrdering::Contiguous,
        )])
        .expect_err("a frame with extra fields cannot satisfy the source schema");
    assert!(
        error.to_string().contains("schema") || error.to_string().contains("field"),
        "{error}"
    );
}

fn evaluated_window_tree(map_above: bool, map_below: bool) -> NativeTree {
    let grid = grid();
    let evaluated = NativeType::Samples(Arc::new(SeriesSchema {
        fields: raw_schema().fields.clone(),
        eval_grid: Some(Arc::clone(&grid)),
    }));
    let mut leaf = NativeNode {
        expr: NativeExpr::Input(BoundInput {
            source_index: 0,
            output: evaluated.clone(),
            ordering: SeriesOrdering::Contiguous,
        }),
        output: evaluated.clone(),
    };
    if map_below {
        leaf = NativeNode {
            expr: NativeExpr::Map {
                input: Box::new(leaf),
                op: BoundMap {
                    ops: Arc::from([MapOp::Math(crate::map::MathOp::Abs)]),
                },
            },
            output: evaluated.clone(),
        };
    }
    let rate = rate_tree();
    let NativeExpr::Window { op, .. } = &rate.root().expr else {
        unreachable!()
    };
    let window = NativeNode {
        expr: NativeExpr::Window {
            input: Box::new(leaf),
            op: BoundWindow {
                input_grid: Some(Arc::clone(&grid)),
                output_grid: Arc::clone(&grid),
                ..op.clone()
            },
        },
        output: evaluated.clone(),
    };
    let mut window = window;
    if map_above {
        window = NativeNode {
            expr: NativeExpr::Map {
                input: Box::new(window.clone()),
                op: BoundMap {
                    ops: Arc::from([MapOp::Math(crate::map::MathOp::Abs)]),
                },
            },
            output: window.output.clone(),
        };
    }
    NativeTree::try_new(window).unwrap()
}

fn evaluated_input_schema() -> Arc<SeriesSchema> {
    Arc::new(SeriesSchema {
        fields: raw_schema().fields.clone(),
        eval_grid: Some(grid()),
    })
}

fn evaluated_frame_schema(schema: &SeriesSchema) -> SchemaRef {
    series_frame_schema(schema, FrameLabelMode::Explicit, None)
        .expect("evaluated samples have a valid frame schema")
}

fn physical_source(_tree: &NativeTree, partitions: usize) -> Arc<dyn ExecutionPlan> {
    let schema = evaluated_frame_schema(&evaluated_input_schema());
    memory_exec_partitioned_with(schema, (0..partitions).map(|_| Vec::new()).collect())
}

fn frame_node(tree: NativeTree) -> FusedSeries {
    let input = evaluated_input_schema();
    FusedSeries::try_new_with_format(
        Arc::from("window input contract"),
        tree.clone(),
        binding_of(&tree, input.clone(), vec![HOST.into(), NAME.into()]),
        vec![input_plan(evaluated_frame_schema(&input))],
        canonical_frame_schema(&tree),
        FusedOutputFormat::SeriesFrame,
    )
    .unwrap()
}

#[test]
fn per_series_map_keeps_multiple_input_partitions_without_routing() {
    let evaluated = evaluated_input_schema();
    let leaf = NativeNode {
        expr: NativeExpr::Input(BoundInput {
            source_index: 0,
            output: NativeType::Samples(Arc::clone(&evaluated)),
            ordering: SeriesOrdering::Contiguous,
        }),
        output: NativeType::Samples(Arc::clone(&evaluated)),
    };
    let tree = NativeTree::try_new(NativeNode {
        expr: NativeExpr::Map {
            input: Box::new(leaf),
            op: BoundMap {
                ops: Arc::from([MapOp::Math(crate::map::MathOp::Abs)]),
            },
        },
        output: NativeType::Samples(Arc::clone(&evaluated)),
    })
    .unwrap();
    let source = physical_source(&tree, 2);
    let exec = frame_node(tree)
        .to_execution_plan(vec![(
            Arc::clone(&source),
            RawLabelSource::Explicit,
            SeriesOrdering::Contiguous,
        )])
        .expect("per-series Map does not require series ownership routing");
    assert_eq!(2, exec.properties().output_partitioning().partition_count());
    assert!(Arc::ptr_eq(&source, exec.children()[0]));
}

#[test]
fn multi_partition_window_inputs_are_routed_but_map_below_window_is_rejected() {
    let source = physical_source(&evaluated_window_tree(false, false), 2);
    for tree in [
        evaluated_window_tree(false, false),
        evaluated_window_tree(true, false),
    ] {
        let exec = frame_node(tree)
            .to_execution_plan(vec![(
                Arc::clone(&source),
                RawLabelSource::Explicit,
                SeriesOrdering::Contiguous,
            )])
            .expect("normal physical construction routes a partitioned window input");
        assert_eq!(2, exec.properties().output_partitioning().partition_count());
        let router = exec.children()[0]
            .downcast_ref::<SeriesRepartitionExec>()
            .expect("window input has a direct owner router");
        assert_eq!(
            2,
            router.properties().output_partitioning().partition_count()
        );
        assert!(Arc::ptr_eq(&source, router.children()[0]));
    }

    let unsupported = frame_node(evaluated_window_tree(false, true));
    let router = Arc::new(SeriesRepartitionExec::try_new(Arc::clone(&source), 2).unwrap());
    let error = unsupported
        .to_execution_plan(vec![(
            router,
            RawLabelSource::Explicit,
            SeriesOrdering::Contiguous,
        )])
        .expect_err("Map below Window remains unsupported even with an owner router");
    assert!(error.to_string().contains("window"), "{error}");
}

#[test]
fn direct_series_router_is_accepted_but_router_below_map_is_not_direct() {
    let tree = evaluated_window_tree(false, false);
    let source = physical_source(&tree, 2);
    let router: Arc<dyn ExecutionPlan> =
        Arc::new(SeriesRepartitionExec::try_new(Arc::clone(&source), 2).unwrap());
    let accepted = frame_node(evaluated_window_tree(false, false))
        .to_execution_plan(vec![(
            Arc::clone(&router),
            RawLabelSource::Explicit,
            SeriesOrdering::Contiguous,
        )])
        .unwrap();
    assert_eq!(
        2,
        accepted
            .properties()
            .output_partitioning()
            .partition_count()
    );
    assert!(Arc::ptr_eq(&router, accepted.children()[0]));

    let single_source = physical_source(&tree, 1);
    let single = frame_node(evaluated_window_tree(false, false))
        .to_execution_plan(vec![(
            Arc::clone(&single_source),
            RawLabelSource::Explicit,
            SeriesOrdering::Contiguous,
        )])
        .unwrap();
    assert_eq!(
        1,
        single.properties().output_partitioning().partition_count()
    );
    assert!(Arc::ptr_eq(&single_source, single.children()[0]));

    let router_then_map_tree = evaluated_window_tree(false, true);
    let error = frame_node(router_then_map_tree)
        .to_execution_plan(vec![(
            Arc::clone(&router) as Arc<dyn ExecutionPlan>,
            RawLabelSource::Explicit,
            SeriesOrdering::Contiguous,
        )])
        .expect_err("a transform below the window is unsupported even after routing");
    assert!(
        error.to_string().contains("window") || error.to_string().contains("partition"),
        "{error}"
    );
}

#[test]
fn window_map_and_router_replacement_contracts() {
    let tree = evaluated_window_tree(true, false);
    let source = physical_source(&tree, 2);
    let map_above = frame_node(tree);
    let router = Arc::new(SeriesRepartitionExec::try_new(Arc::clone(&source), 2).unwrap());
    let automatic = map_above
        .to_execution_plan(vec![(
            Arc::clone(&source),
            RawLabelSource::Explicit,
            SeriesOrdering::Contiguous,
        )])
        .expect("Map above Window is routed by normal physical construction");
    assert_eq!(
        2,
        automatic
            .properties()
            .output_partitioning()
            .partition_count()
    );
    let automatic_router = automatic.children()[0]
        .downcast_ref::<SeriesRepartitionExec>()
        .expect("Map above Window has a direct owner router");
    assert!(Arc::ptr_eq(&source, automatic_router.children()[0]));
    let exec = map_above
        .to_execution_plan(vec![(
            Arc::clone(&router) as Arc<dyn ExecutionPlan>,
            RawLabelSource::Explicit,
            SeriesOrdering::Contiguous,
        )])
        .unwrap();
    assert_eq!(2, exec.properties().output_partitioning().partition_count());
    let rebuilt = exec.clone().replace_children(
        vec![source],
        ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
    );
    assert!(
        rebuilt.is_err(),
        "replacing the router with plain partitions is refused"
    );
}
