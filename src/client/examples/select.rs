use client::{Client, Database, Select};
use tracing::{event, Level};

fn main() {
    tracing::subscriber::set_global_default(tracing_subscriber::FmtSubscriber::builder().finish())
        .unwrap();

    run();
}

#[tokio::main]
async fn run() {
    let client = Client::connect("http://127.0.0.1:3001").await.unwrap();
    let db = Database::new("greptime", client);

    let sql = Select::Sql("select * from demo".to_string());
    let result = db.select(sql).await.unwrap();

    event!(Level::INFO, "result: {:#?}", result);
}
