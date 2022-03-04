use std::env;
use async_graphql::{EmptyMutation, EmptySubscription, Schema};
use sqlx::postgres::PgPoolOptions;
use graphql::QueryRoot;

mod entities;
mod graphql;
mod repository;
mod server;
mod metrics;


#[actix_web::main]
async fn main() -> std::io::Result<()> {
    env_logger::init();

    let database_url = env::var("DATABASE_URL")
        .expect("DATABASE_URL env variable is required");
    let max_connections = env::var("DATABASE_MAX_CONNECTIONS")
        .expect("DATABASE_MAX_CONNECTIONS env variable is required")
        .parse::<u32>()
        .unwrap();
    let pool = PgPoolOptions::new()
        .max_connections(max_connections)
        .connect(&database_url)
        .await
        .unwrap();

    let schema = Schema::build(QueryRoot, EmptyMutation, EmptySubscription)
        .data(pool)
        .finish();
    server::run(schema).await
}
