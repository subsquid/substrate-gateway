use super::selection::{
    CallSelection, EventSelection, EvmLogSelection, EthTransactSelection,
    ContractsEventSelection, GearMessageEnqueuedSelection, GearUserMessageSentSelection,
};

pub struct BatchOptions {
    limit: i32,
    from_block: i32,
    to_block: Option<i32>,
    include_all_blocks: bool,
    call_selections: Option<Vec<CallSelection>>,
    event_selections: Option<Vec<EventSelection>>,
    evm_log_selections: Option<Vec<EvmLogSelection>>,
    eth_transact_selections: Option<Vec<EthTransactSelection>>,
    contracts_event_selections: Option<Vec<ContractsEventSelection>>,
    gear_message_enqueued_selections: Option<Vec<GearMessageEnqueuedSelection>>,
    gear_user_message_sent_selections: Option<Vec<GearUserMessageSentSelection>>,
}

impl BatchOptions {
    pub fn new() -> Self {
        Self {
            limit: 0,
            from_block: 0,
            to_block: None,
            include_all_blocks: false,
            call_selections: None,
            event_selections: None,
            evm_log_selections: None,
            eth_transact_selections: None,
            contracts_event_selections: None,
            gear_message_enqueued_selections: None,
            gear_user_message_sent_selections: None,
        }
    }

    pub fn limit(mut self, limit: i32) -> Self {
        self.limit = limit;
        self
    }

    pub fn from_block(mut self, from_block: i32) -> Self {
        self.from_block = from_block;
        self
    }

    pub fn to_block(mut self, to_block: Option<i32>) -> Self {
        self.to_block = to_block;
        self
    }

    pub fn include_all_blocks(mut self, include_all_blocks: bool) -> Self {
        self.include_all_blocks = include_all_blocks;
        self
    }

    pub fn call_selections(mut self, call_selections: Option<Vec<CallSelection>>) -> Self {
        self.call_selections = call_selections;
        self
    }

    pub fn event_selections(mut self, event_selections: Option<Vec<EventSelection>>) -> Self {
        self.event_selections = event_selections;
        self
    }

    pub fn evm_log_selections(mut self, evm_log_selections: Option<Vec<EvmLogSelection>>) -> Self {
        self.evm_log_selections = evm_log_selections;
        self
    }

    pub fn eth_transact_selections(
        mut self,
        eth_transact_selections: Option<Vec<EthTransactSelection>>
    ) -> Self {
        self.eth_transact_selections = eth_transact_selections;
        self
    }

    pub fn contracts_event_selections(
        mut self,
        contracts_event_selections: Option<Vec<ContractsEventSelection>>
    ) -> Self {
        self.contracts_event_selections = contracts_event_selections;
        self
    }

    pub fn gear_message_enqueued_selections(
        mut self,
        gear_message_enqueued_selections: Option<Vec<GearMessageEnqueuedSelection>>
    ) -> Self {
        self.gear_message_enqueued_selections = gear_message_enqueued_selections;
        self
    }

    pub fn gear_user_message_sent_selections(
        mut self,
        gear_user_message_sent_selections: Option<Vec<GearUserMessageSentSelection>>
    ) -> Self {
        self.gear_user_message_sent_selections = gear_user_message_sent_selections;
        self
    }
}
