use std::env;
use std::time::Duration;
use sqlx::postgres::PgPoolOptions;
use archive_gateway::{ArchiveGateway, DatabaseType};
use tracing_subscriber::EnvFilter;

#[tracing::instrument]
#[actix_web::main]
async fn main() -> std::io::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_target(false)
        .json()
        .flatten_event(true)
        .with_span_list(false)
        .with_current_span(false)
        .init();

    let database_url = env::var("DATABASE_URL")
        .expect("DATABASE_URL env variable is required");
    let database_type = match env::var("DATABASE_TYPE")
        .expect("DATABASE_TYPE env variable is required")
        .as_str() {
            "postgres" => DatabaseType::Postgres,
            "cockroach" => DatabaseType::Cockroach,
            _ => panic!("DATABASE_TYPE env should be `postgres` or `cockroach`")
        };
    let max_connections = env::var("DATABASE_MAX_CONNECTIONS")
        .expect("DATABASE_MAX_CONNECTIONS env variable is required")
        .parse::<u32>()
        .unwrap();
    let evm_support = env::var("EVM_SUPPORT")
        .expect("EVM_SUPPORT env variable is required")
        .parse::<bool>()
        .unwrap();
    let contracts_support = match env::var("CONTRACTS_SUPPORT") {
        Ok(variable) => {
            variable.parse::<bool>().expect("CONTRACTS_SUPPORT parsing error")
        }
        Err(_) => false
    };
    let pool = PgPoolOptions::new()
        .max_connections(max_connections)
        .connect_timeout(Duration::from_secs(5))
        .connect_lazy(&database_url)
        .unwrap();
    ArchiveGateway::new(pool, database_type, evm_support, contracts_support)
        .run()
        .await
}
