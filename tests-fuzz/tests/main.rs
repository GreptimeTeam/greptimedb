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

use std::env;

use common_telemetry::info;
use rand::{Rng, SeedableRng};
use sqlx::mysql::MySqlPoolOptions;
use sqlx::postgres::PgPoolOptions;
use tests_fuzz::fake::{
    merge_two_word_map_fn, random_capitalize_map, uppercase_and_keyword_backtick_map,
    uppercase_and_keyword_quote_map, MapWordGenerator,
};
use tests_fuzz::generator::create_expr::CreateTableExprGeneratorBuilder;
use tests_fuzz::generator::Generator;
use tests_fuzz::translator::greptime::create_expr::CreateTableExprTranslator;
use tests_fuzz::translator::DslTranslator;

const GT_STANDALONE_MYSQL_ADDR: &str = "GT_STANDALONE_MYSQL_ADDR";
const GT_STANDALONE_POSTGRES_ADDR: &str = "GT_STANDALONE_POSTGRES_ADDR";

#[tokio::test]
async fn test_greptime_create_table_expr() {
    common_telemetry::init_default_ut_logging();
    let _ = dotenv::dotenv();
    let addr = if let Ok(addr) = env::var(GT_STANDALONE_MYSQL_ADDR) {
        addr
    } else {
        info!("GT_STANDALONE_MYSQL_ADDR is empty, ignores test");
        return;
    };

    let pool = MySqlPoolOptions::new()
        .connect(&format!("mysql://{addr}/public"))
        .await
        .unwrap();

    let seed = rand::random();
    info!("Test seed: {seed:?}");
    let mut rng = rand_chacha::ChaCha8Rng::seed_from_u64(seed);

    let create_table_generator = CreateTableExprGeneratorBuilder::default()
        .name_generator(Box::new(MapWordGenerator::new(Box::new(
            merge_two_word_map_fn(random_capitalize_map, uppercase_and_keyword_backtick_map),
        ))))
        .columns(rng.gen_range(1..10))
        .engine("mito")
        .build()
        .unwrap();

    let create_table_expr = create_table_generator.generate(&mut rng).unwrap();
    let translator = CreateTableExprTranslator;
    let sql = translator.translate(&create_table_expr).unwrap();
    info!("Creating table: {sql}");

    let result = sqlx::query(&sql).execute(&pool).await;
    info!("{result:?}")
}

#[tokio::test]
async fn test_greptime_pg_create_table_expr() {
    common_telemetry::init_default_ut_logging();
    let _ = dotenv::dotenv();
    let addr = if let Ok(addr) = env::var(GT_STANDALONE_POSTGRES_ADDR) {
        addr
    } else {
        info!("GT_STANDALONE_POSTGRES_ADDR is empty, ignores test");
        return;
    };

    let pool = PgPoolOptions::new()
        .connect(&format!("postgres://{addr}/public"))
        .await
        .unwrap();

    let seed = rand::random();
    info!("Test seed: {seed:?}");
    let mut rng = rand_chacha::ChaCha8Rng::seed_from_u64(seed);

    let create_table_generator = CreateTableExprGeneratorBuilder::default()
        .name_generator(Box::new(MapWordGenerator::new(Box::new(
            merge_two_word_map_fn(random_capitalize_map, uppercase_and_keyword_quote_map),
        ))))
        .columns(rng.gen_range(1..10))
        .engine("mito")
        .build()
        .unwrap();

    let create_table_expr = create_table_generator.generate(&mut rng).unwrap();
    let translator = CreateTableExprTranslator;
    let sql = translator.translate(&create_table_expr).unwrap();
    info!("Creating table: {sql}");

    let result = sqlx::query(&sql).execute(&pool).await;
    info!("{result:?}")
}
