use std::env;
use async_graphql::{EmptyMutation, EmptySubscription, Schema};
use sqlx::postgres::PgPoolOptions;
use graphql::{QueryRoot, EvmSupport};

mod entities;
mod graphql;
mod server;
mod metrics;
mod error;
mod archive;


#[actix_web::main]
async fn main() -> std::io::Result<()> {
    env_logger::init();

    let database_url = env::var("DATABASE_URL")
        .expect("DATABASE_URL env variable is required");
    let max_connections = env::var("DATABASE_MAX_CONNECTIONS")
        .expect("DATABASE_MAX_CONNECTIONS env variable is required")
        .parse::<u32>()
        .unwrap();
    let evm_support = env::var("EVM_SUPPORT")
        .expect("EVM_SUPPORT env variable is required")
        .parse::<bool>()
        .unwrap();
    let pool = PgPoolOptions::new()
        .max_connections(max_connections)
        .connect(&database_url)
        .await
        .unwrap();

    let schema = Schema::build(QueryRoot, EmptyMutation, EmptySubscription)
        .data(pool)
        .data(EvmSupport(evm_support))
        .finish();
    server::run(schema).await
}
