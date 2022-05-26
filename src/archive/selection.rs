use super::fields::{EventFields, CallFields, ParentCallFields, ExtrinsicFields};

#[derive(Debug)]
pub struct EventDataSelection {
    pub event: EventFields,
}


impl EventDataSelection {
    pub fn new(value: bool) -> Self {
        EventDataSelection {
            event: EventFields::new(value),
        }
    }
}


#[derive(Debug)]
pub struct CallDataSelection {
    pub call: CallFields,
    pub extrinsic: ExtrinsicFields,
}


impl CallDataSelection {
    pub fn new(value: bool) -> Self {
        CallDataSelection {
            call: CallFields::new(value),
            extrinsic: ExtrinsicFields::new(value),
        }
    }

    pub fn selected_fields(&self) -> Vec<String> {
        let mut fields = vec![];
        if self.call._all {
            fields.push("success".to_string());
            fields.push("name".to_string());
            fields.push("args".to_string());
            fields.push("parent_id".to_string());
        } else {
            if self.call.success {
                fields.push("success".to_string());
            }
            if self.call.name {
                fields.push("name".to_string());
            }
            if self.call.args {
                fields.push("args".to_string());
            }
            if self.call.parent.any() {
                fields.push("parent_id".to_string());
            }
        }
        if self.extrinsic.any() {
            fields.push("extrinsic_id".to_string());
        }
        fields
    }
}


#[derive(Debug)]
pub struct EvmLogDataSelection {
    pub tx_hash: bool,
    pub substrate: EventDataSelection,
}


impl EvmLogDataSelection {
    pub fn new(value: bool) -> Self {
        EvmLogDataSelection {
            tx_hash: value,
            substrate: EventDataSelection::new(value),
        }
    }
}


#[derive(Debug)]
pub struct EventSelection {
    pub name: String,
    pub data: EventDataSelection,
}


#[derive(Debug)]
pub struct CallSelection {
    pub name: String,
    pub data: CallDataSelection,
}


#[derive(Debug)]
pub struct EvmLogSelection {
    pub contract: String,
    pub filter: Vec<Vec<String>>,
    pub data: EvmLogDataSelection,
}

#[derive(Debug)]
pub struct ContractsEventSelection {
    pub contract: String,
    pub data: EventDataSelection,
}
