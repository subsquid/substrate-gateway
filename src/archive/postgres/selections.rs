use crate::entities::{Call, Event};
use crate::archive::{CallSelection, EventSelection};

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
