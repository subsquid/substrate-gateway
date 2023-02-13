use super::selection::{
    AcalaEvmEventSelection, CallSelection, ContractsEventSelection, EthTransactSelection,
    EventSelection, EvmLogSelection, GearMessageEnqueuedSelection, GearUserMessageSentSelection,
};
use crate::entities::{Batch, Metadata, Status};
use crate::error::Error;

pub struct BatchOptions {
    pub limit: Option<i32>,
    pub from_block: i32,
    pub to_block: Option<i32>,
    pub include_all_blocks: bool,
    pub selections: Selections,
}

pub struct BatchResponse {
    pub data: Vec<Batch>,
    pub next_block: Option<i32>,
}

#[derive(Clone)]
pub struct Selections {
    pub call: Vec<CallSelection>,
    pub event: Vec<EventSelection>,
    pub evm_log: Vec<EvmLogSelection>,
    pub eth_transact: Vec<EthTransactSelection>,
    pub contracts_event: Vec<ContractsEventSelection>,
    pub gear_message_enqueued: Vec<GearMessageEnqueuedSelection>,
    pub gear_user_message_sent: Vec<GearUserMessageSentSelection>,
    pub acala_evm_executed: Vec<AcalaEvmEventSelection>,
    pub acala_evm_executed_failed: Vec<AcalaEvmEventSelection>,
}

#[async_trait::async_trait]
pub trait ArchiveService {
    async fn batch(&self, options: &BatchOptions) -> Result<BatchResponse, Error>;
    async fn metadata(&self) -> Result<Vec<Metadata>, Error>;
    async fn metadata_by_id(&self, id: String) -> Result<Option<Metadata>, Error>;
    async fn status(&self) -> Result<Status, Error>;
}
