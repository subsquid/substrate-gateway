use crate::archive::postgres::PostgresArchive;
use std::boxed::Box;
use async_graphql::{EmptyMutation, EmptySubscription, Schema};
use graphql::{QueryRoot, EvmSupport, ContractsSupport, GearSupport};
use sqlx::{Pool, Postgres};

mod entities;
mod graphql;
mod server;
mod metrics;
mod error;
mod archive;

pub struct ArchiveGateway {
    pool: Pool<Postgres>,
    evm_support: bool,
    contracts_support: bool,
    gear_support: bool,
}

impl ArchiveGateway {
    pub fn new(pool: Pool<Postgres>) -> Self {
        ArchiveGateway {
            pool,
            evm_support: false,
            contracts_support: false,
            gear_support: false,
        }
    }

    pub fn evm_support(mut self, value: bool) -> Self {
        self.evm_support = value;
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
            .data(ContractsSupport(self.contracts_support))
            .data(GearSupport(self.gear_support))
            .finish();
        server::run(schema).await
    }
}
