use async_graphql::{EmptyMutation, EmptySubscription, Schema};
use graphql::{AcalaSupport, ContractsSupport, EvmSupport, GearSupport, QueryRoot};
use sqlx::{Pool, Postgres};
use std::boxed::Box;
pub use substrate_archive::postgres::DatabaseType;
use substrate_archive::postgres::PostgresArchive;

mod graphql;
mod metrics;
mod server;

pub struct SubstrateGateway {
    pool: Pool<Postgres>,
    database_type: DatabaseType,
    scan_start_value: u16,
    scan_max_value: u32,
    scan_time_limit: u16,
    evm_support: bool,
    acala_support: bool,
    contracts_support: bool,
    gear_support: bool,
}

impl SubstrateGateway {
    pub fn new(pool: Pool<Postgres>, database_type: DatabaseType) -> Self {
        SubstrateGateway {
            pool,
            database_type,
            scan_start_value: 50,
            scan_max_value: 100_000,
            scan_time_limit: 5000,
            evm_support: false,
            acala_support: false,
            contracts_support: false,
            gear_support: false,
        }
    }

    pub fn evm_support(mut self, value: bool) -> Self {
        self.evm_support = value;
        self
    }

    pub fn acala_support(mut self, value: bool) -> Self {
        self.acala_support = value;
        self
    }

    pub fn contracts_support(mut self, value: bool) -> Self {
        self.contracts_support = value;
        self
    }

    pub fn gear_support(mut self, value: bool) -> Self {
        self.gear_support = value;
        self
    }

    pub fn scan_start_value(mut self, value: u16) -> Self {
        self.scan_start_value = value;
        self
    }

    pub fn scan_max_value(mut self, value: u32) -> Self {
        self.scan_max_value = value;
        self
    }

    pub fn scan_time_limit(mut self, value: u16) -> Self {
        self.scan_time_limit = value;
        self
    }

    pub async fn run(&self) -> std::io::Result<()> {
        let archive = Box::new(PostgresArchive::new(
            self.pool.clone(),
            self.database_type.clone(),
            self.scan_start_value,
            self.scan_max_value,
            self.scan_time_limit,
        ));
        let query = QueryRoot { archive };
        let schema = Schema::build(query, EmptyMutation, EmptySubscription)
            .data(EvmSupport(self.evm_support))
            .data(AcalaSupport(self.acala_support))
            .data(ContractsSupport(self.contracts_support))
            .data(GearSupport(self.gear_support))
            .finish();
        server::run(schema).await
    }
}
