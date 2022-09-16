use async_graphql::{EmptyMutation, EmptySubscription, Schema};
use graphql::{AcalaSupport, ContractsSupport, EvmSupport, GearSupport, QueryRoot};
use sqlx::{Pool, Postgres};
use std::boxed::Box;
use substrate_archive::postgres::PostgresArchive;

mod graphql;
mod metrics;
mod server;

pub struct SubstrateGateway {
    pool: Pool<Postgres>,
    evm_support: bool,
    acala_support: bool,
    contracts_support: bool,
    gear_support: bool,
}

impl SubstrateGateway {
    pub fn new(pool: Pool<Postgres>) -> Self {
        SubstrateGateway {
            pool,
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

    pub async fn run(&self) -> std::io::Result<()> {
        let archive = Box::new(PostgresArchive::new(self.pool.clone()));
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
