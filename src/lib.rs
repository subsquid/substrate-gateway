// use crate::archive::cockroach::CockroachArchive;
use crate::archive::postgres::PostgresArchive;
use crate::archive::ArchiveService;
use std::boxed::Box;
use async_graphql::{EmptyMutation, EmptySubscription, Schema};
use graphql::{QueryRoot, EvmSupport, ContractsSupport};
use sqlx::{Pool, Postgres};

mod entities;
mod graphql;
mod server;
mod metrics;
mod error;
mod archive;

pub enum DatabaseType {
    Postgres,
    // Cockroach,
}

pub struct ArchiveGateway {
    pool: Pool<Postgres>,
    database_type: DatabaseType,
    evm_support: bool,
    contracts_support: bool,
}

impl ArchiveGateway {
    pub fn new(
        pool: Pool<Postgres>,
        database_type: DatabaseType,
        evm_support: bool,
        contracts_support: bool,
    ) -> Self {
        ArchiveGateway {
            pool,
            database_type,
            evm_support,
            contracts_support,
        }
    }

    pub async fn run(&self) -> std::io::Result<()> {
        let archive: Box<dyn ArchiveService + Send + Sync> = match self.database_type {
            DatabaseType::Postgres => Box::new(PostgresArchive::new(self.pool.clone())),
            // DatabaseType::Cockroach => Box::new(CockroachArchive::new(self.pool.clone())),
        };
        let query = QueryRoot { archive };
        let schema = Schema::build(query, EmptyMutation, EmptySubscription)
            .data(EvmSupport(self.evm_support))
            .data(ContractsSupport(self.contracts_support))
            .finish();
        server::run(schema).await
    }
}
