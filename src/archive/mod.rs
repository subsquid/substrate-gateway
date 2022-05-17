use crate::entities::{Batch, Metadata, Status};
use crate::error::Error;
use selection::{EventSelection, CallSelection, EvmLogSelection, ContractsEventSelection};

pub mod selection;
pub mod cockroach;
pub mod postgres;

#[async_trait::async_trait]
pub trait ArchiveService {
    async fn batch(
        &self,
        limit: i32,
        from_block: i32,
        to_block: Option<i32>,
        evm_log_selections: &Vec<EvmLogSelection>,
        contracts_event_selections: &Vec<ContractsEventSelection>,
        event_selections: &Vec<EventSelection>,
        call_selections: &Vec<CallSelection>,
        include_all_blocks: bool
    ) -> Result<Vec<Batch>, Error>;
    async fn metadata(&self) -> Result<Vec<Metadata>, Error>;
    async fn metadata_by_id(&self, id: String) -> Result<Option<Metadata>, Error>;
    async fn status(&self) -> Result<Status, Error>;
}
