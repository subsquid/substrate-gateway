use super::selection::{
    AcalaEvmEventSelection, CallSelection, ContractsEventSelection, EthTransactSelection,
    EventSelection, EvmLogSelection, GearMessageEnqueuedSelection, GearUserMessageSentSelection,
};

pub struct BatchOptions {
    pub limit: Option<i32>,
    pub from_block: i32,
    pub to_block: Option<i32>,
    pub include_all_blocks: bool,
    pub selections: Selections,
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
