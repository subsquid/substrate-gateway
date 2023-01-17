use clap::Parser;
use sqlx::postgres::PgPoolOptions;
use sqlx::Executor;
use std::time::Duration;
use substrate_gateway::{DatabaseType, SubstrateGateway};

mod logger;

#[derive(Parser, Debug)]
#[clap(about)]
struct Args {
    /// Database connection string
    #[clap(long)]
    database_url: String,

    /// Maximum number of connections supported by pool
    #[clap(long, default_value_t = 1)]
    database_max_connections: u32,

    /// Abort any statement that takes more than the specified amount of ms
    #[clap(long, default_value_t = 0)]
    database_statement_timeout: u32,

    /// Database type
    #[clap(long, value_enum, default_value_t = DatabaseType::Postgres)]
    database_type: DatabaseType,

    /// Number of blocks to start scanning a database
    #[clap(long, default_value_t = 100)]
    scan_start_value: u16,

    /// Query engine will be upper limited by this amount of blocks
    #[clap(long, default_value_t = 100_000)]
    scan_max_value: u32,

    /// EVM pallet support
    #[clap(long)]
    evm_support: bool,

    /// Сontracts pallet support
    #[clap(long)]
    contracts_support: bool,

    /// Gear pallet support
    #[clap(long)]
    gear_support: bool,

    /// Acala's EVM pallet support
    #[clap(long)]
    acala_support: bool,
}

#[tracing::instrument]
#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let args = Args::parse();
    logger::init();

    let pool = PgPoolOptions::new()
        .max_connections(args.database_max_connections)
        .idle_timeout(Duration::from_secs(10))
        .acquire_timeout(Duration::from_secs(5))
        .after_connect(move |connection, _meta| {
            Box::pin(async move {
                let query = format!(
                    "SET statement_timeout = {}",
                    args.database_statement_timeout
                );
                connection.execute(&*query).await?;
                Ok(())
            })
        })
        .connect_lazy(&args.database_url)
        .unwrap();
    SubstrateGateway::new(pool, args.database_type)
        .evm_support(args.evm_support)
        .contracts_support(args.contracts_support)
        .gear_support(args.gear_support)
        .acala_support(args.acala_support)
        .scan_start_value(args.scan_start_value)
        .scan_max_value(args.scan_max_value)
        .run()
        .await
}
