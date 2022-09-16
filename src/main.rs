use clap::Parser;
use sqlx::postgres::PgPoolOptions;
use std::time::Duration;
use substrate_gateway::SubstrateGateway;

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
        .connect_timeout(Duration::from_secs(5))
        .connect_lazy(&args.database_url)
        .unwrap();
    SubstrateGateway::new(pool)
        .evm_support(args.evm_support)
        .contracts_support(args.contracts_support)
        .gear_support(args.gear_support)
        .acala_support(args.acala_support)
        .run()
        .await
}
