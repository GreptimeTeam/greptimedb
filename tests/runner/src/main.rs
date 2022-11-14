use env::Env;
use sqlness::{ConfigBuilder, Runner};

mod env;

#[tokio::main]
async fn main() {
    let config = ConfigBuilder::default()
        .case_dir("../cases".to_string())
        .build()
        .unwrap();
    let runner = Runner::new_with_config(config, Env {}).await.unwrap();
    runner.run().await.unwrap();
}
