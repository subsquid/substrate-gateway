use crate::entities::{Call, Event, ContractsEvent};
use crate::archive::{CallSelection, EventSelection, ContractsEventSelection};

const WILDCARD: &str = "*";

impl CallSelection {
    pub fn condition(&self) -> String {
        if self.name == WILDCARD {
            "true".to_string()
        } else {
            format!("call.name = '{}'", self.name)
        }
    }

    pub fn r#match(&self, call: &Call) -> bool {
        self.name == WILDCARD || self.name == call.name
    }
}

impl EventSelection {
    pub fn condition(&self) -> String {
        if self.name == WILDCARD {
            "true".to_string()
        } else {
            format!("event.name = '{}'", self.name)
        }
    }

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
