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

use std::path::PathBuf;

use rand::Rng;
use serde::Serialize;
use snafu::ResultExt;
use tests_fuzz::context::TableContextRef;
use tests_fuzz::fake::WordGenerator;
use tests_fuzz::generator::alter_expr::{
    AlterExprAddColumnGeneratorBuilder, AlterExprDropColumnGeneratorBuilder,
    AlterExprRenameGeneratorBuilder,
};
use tests_fuzz::generator::create_expr::CreateTableExprGeneratorBuilder;
use tests_fuzz::generator::Generator;
use tests_fuzz::ir::{droppable_columns, AlterTableExpr, CreateTableExpr};
use tinytemplate::TinyTemplate;
use tokio::fs::OpenOptions;

use crate::error::{self, Result};

/// Creates an file
pub(crate) async fn path_to_stdio(path: &str) -> Result<std::fs::File> {
    Ok(OpenOptions::new()
        .append(true)
        .create(true)
        .read(true)
        .write(true)
        .open(path)
        .await
        .context(error::CreateFileSnafu { path })?
        .into_std()
        .await)
}

/// Get the path of config dir `tests/conf`.
pub(crate) fn get_conf_path() -> PathBuf {
    let mut root_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    root_path.push("conf");
    root_path
}

/// Returns rendered config file.
pub(crate) fn render_config_file<C: Serialize>(template_path: &str, context: &C) -> String {
    let mut tt = TinyTemplate::new();
    let template = std::fs::read_to_string(template_path).unwrap();
    tt.add_template(template_path, &template).unwrap();
    tt.render(template_path, context).unwrap()
}

pub(crate) fn generate_create_table_expr<R: Rng + 'static>(rng: &mut R) -> CreateTableExpr {
    let columns = rng.gen_range(2..30);
    let create_table_generator = CreateTableExprGeneratorBuilder::default()
        .name_generator(Box::new(WordGenerator))
        .columns(columns)
        .engine("mito")
        .build()
        .unwrap();
    create_table_generator.generate(rng).unwrap()
}

#[allow(dead_code)]
pub fn generate_alter_table_expr<R: Rng + 'static>(
    table_ctx: TableContextRef,
    rng: &mut R,
) -> AlterTableExpr {
    let rename = rng.gen_bool(0.2);
    if rename {
        let expr_generator = AlterExprRenameGeneratorBuilder::default()
            .table_ctx(table_ctx)
            .name_generator(Box::new(WordGenerator))
            .build()
            .unwrap();
        expr_generator.generate(rng).unwrap()
    } else {
        let drop_column = rng.gen_bool(0.5) && !droppable_columns(&table_ctx.columns).is_empty();
        if drop_column {
            let expr_generator = AlterExprDropColumnGeneratorBuilder::default()
                .table_ctx(table_ctx)
                .build()
                .unwrap();
            expr_generator.generate(rng).unwrap()
        } else {
            let location = rng.gen_bool(0.5);
            let expr_generator = AlterExprAddColumnGeneratorBuilder::default()
                .table_ctx(table_ctx)
                .location(location)
                .build()
                .unwrap();
            expr_generator.generate(rng).unwrap()
        }
    }
}
