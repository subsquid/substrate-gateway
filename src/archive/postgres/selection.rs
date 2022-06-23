use crate::entities::{FullCall, Event, ContractsEvent};
use crate::archive::{CallSelection, EventSelection, ContractsEventSelection};

const WILDCARD: &str = "*";

impl CallSelection {
    pub fn r#match(&self, call: &FullCall) -> bool {
        self.name == WILDCARD || self.name == call.name
    }
}

impl EventSelection {
    pub fn r#match(&self, event: &Event) -> bool {
        self.name == WILDCARD || self.name == event.name
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
