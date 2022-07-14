use crate::entities::{Call, Event, ContractsEvent};
use crate::archive::{CallSelection, EventSelection, ContractsEventSelection, EthTransactSelection};

const WILDCARD: &str = "*";

impl CallSelection {
    pub fn r#match(&self, call: &Call) -> bool {
        self.name == WILDCARD || self.name == call.name
    }
}

impl EventSelection {
    pub fn r#match(&self, event: &Event) -> bool {
        self.name == WILDCARD || self.name == event.name
    }
}

impl EthTransactSelection {
    pub fn r#match(&self, call: &Call) -> bool {
        if let Some(args) = &call.args {
            if let Some(transaction) = args.get("transaction") {
                let mut action = transaction.get("action");
                if action.is_none() {
                    if let Some(value) = transaction.get("value") {
                        action = value.get("action");
                    }
                }
                if let Some(action) = action {
                    if let Some(value) = action.get("value") {
                        if let Some(value) = value.as_str() {
                            return value.to_string() == self.contract
                        }
                    }
                }
            }
        }
        false
    }
}

impl ContractsEventSelection {
    pub fn r#match(&self, event: &ContractsEvent) -> bool {
        if let Some(value) = event.data.get("contract") {
            if let Some(contract) = value.as_str() {
                return contract.to_string() == self.contract
            }
        }
        false
    }
}
